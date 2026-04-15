import os
import pandas as pd
import requests
import msal
import time
import random
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ========= 參數 =========
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SHAREPOINT_HOST = "nanfung.sharepoint.com"
SITE_PATH       = "/sites/NFPM-App/POT"
LIST_TITLE      = "Energy Efficiency"

EXCEL_PATH  = "energy.xlsx"
SHEET_NAME  = "Sheet1"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE      = ["https://graph.microsoft.com/.default"]

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

MAX_RETRIES = 3
BASE_DELAY  = 2  # 秒

# ========= MSAL Auth =========
def get_access_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise Exception(f"Token 取得失敗: {result.get('error_description', result)}")
    return result["access_token"]

# ========= Retry 裝飾器 =========
def retry_graph(max_retries=MAX_RETRIES, base_delay=BASE_DELAY):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.HTTPError as e:
                    last_exception = e
                    status = e.response.status_code if e.response is not None else 0
                    if status in (429, 503):
                        retry_after = int(e.response.headers.get("Retry-After", base_delay * (2 ** attempt)))
                        wait_time = retry_after + random.uniform(0, 1)
                        print(f"  [RETRY {attempt+1}/{max_retries}] Graph API 限速 ({status})，等待 {wait_time:.1f}秒")
                        time.sleep(wait_time)
                    elif attempt < max_retries:
                        wait_time = base_delay * attempt + random.uniform(0, 0.5)
                        print(f"  [RETRY {attempt+1}/{max_retries}] HTTP 錯誤 {status}: {str(e)[:100]}，等待 {wait_time:.1f}秒")
                        time.sleep(wait_time)
                    else:
                        print(f"  [RETRY FAILED] 超過 {max_retries} 次重試: {str(e)}")
                        raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = base_delay * attempt + random.uniform(0, 0.5)
                        print(f"  [RETRY {attempt+1}/{max_retries}] 未知錯誤: {str(e)[:100]}，等待 {wait_time:.1f}秒")
                        time.sleep(wait_time)
                    else:
                        print(f"  [RETRY FAILED] 超過 {max_retries} 次重試: {str(e)}")
                        raise
            raise last_exception
        return wrapper
    return decorator

# ========= Graph API 封裝 =========
class GraphClient:
    def __init__(self):
        self._token = None
        self._token_expiry = 0

    def _get_headers(self) -> dict:
        now = time.time()
        if not self._token or now >= self._token_expiry - 60:
            self._token = get_access_token()
            self._token_expiry = now + 3600  # token 通常有效 1 小時
            print("[INFO] Token 已刷新")
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }

    def get(self, url: str, params: dict = None) -> dict:
        resp = requests.get(url, headers=self._get_headers(), params=params)
        resp.raise_for_status()
        return resp.json()

    def post(self, url: str, body: dict) -> dict:
        resp = requests.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.json()

    def patch(self, url: str, body: dict) -> dict:
        resp = requests.patch(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.json()

# ========= 初始化 (取得 site_id / list_id) =========
def init_graph(client: GraphClient):
    site_resp = client.get(f"{GRAPH_BASE}/sites/{SHAREPOINT_HOST}:{SITE_PATH}")
    site_id = site_resp["id"]
    print(f"[OK] SharePoint 站點 ID: {site_id}")

    lists_resp = client.get(
        f"{GRAPH_BASE}/sites/{site_id}/lists",
        params={"$filter": f"displayName eq '{LIST_TITLE}'"}
    )
    lists = lists_resp.get("value", [])
    if not lists:
        raise Exception(f"找不到 SharePoint List: {LIST_TITLE}")
    list_id = lists[0]["id"]
    print(f"[OK] List ID: {list_id}")

    return site_id, list_id

# ========= SharePoint CRUD (含 Retry) =========
@retry_graph()
def find_items(client: GraphClient, site_id: str, list_id: str, title_key: str) -> list:
    safe_key = title_key.replace("'", "''")
    resp = client.get(
        f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items",
        params={
            "$filter": f"fields/Title eq '{safe_key}'",
            "$expand": "fields"
        }
    )
    return resp.get("value", [])

@retry_graph()
def create_item(client: GraphClient, site_id: str, list_id: str, fields: dict) -> dict:
    return client.post(
        f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items",
        body={"fields": fields}
    )

@retry_graph()
def update_item(client: GraphClient, site_id: str, list_id: str, item_id: str, fields: dict) -> dict:
    return client.patch(
        f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields",
        body=fields
    )

# ========= 業務邏輯 =========
def parse_month3(month3: str) -> datetime:
    return datetime.strptime(month3, "%Y-%m")

def build_url(ou: str, month3: str) -> str:
    d = parse_month3(month3)
    year = d.year
    month_name = MONTH_NAMES[d.month]
    base = ("https://forms.office.com/Pages/ResponsePage.aspx"
            "?id=Uh2BCaZRS0qHehOgZKxxDlQKCx3qPv1DuiXeVBcnBVFURUlMVVRXMURCQ0RPNE9BRjJDTjFESjM5NS4u")
    return (f"{base}"
            f"&r5c9275b95d3649acb798f5c6ceda3d53={ou}"
            f"&r76782f76fb474cb881a13ea1b7b61309={month_name}"
            f"&rd583d3fc7b954f0c887330c18476947f={year}")

def normalize_ou(ou: str) -> str:
    ou = str(ou).strip()
    if ou == "TKO-MALL":
        return "TKO MALL"
    if ou == "MOS-MALL":
        return "MOS MALL"
    return ou

def get_pot_value(month3: str) -> str:
    dt = datetime.strptime(month3, "%Y-%m")
    boundaries = [
        (datetime(2017, 10, 1), datetime(2017, 10, 1), "5.4"),   # before 2017-10
        (datetime(2017, 11, 1), datetime(2018, 10, 1), "5.5"),
        (datetime(2018, 11, 1), datetime(2019, 10, 1), "5.7"),
        (datetime(2019, 11, 1), datetime(2020, 10, 1), "5.6"),
        (datetime(2020, 11, 1), datetime(2021, 10, 1), "6.4"),
        (datetime(2021, 11, 1), datetime(2022, 10, 1), "6.6"),
        (datetime(2022, 11, 1), datetime(2024,  3, 1), "6.6"),
    ]
    if dt < datetime(2017, 10, 1):
        return "5.4"
    for start, end, rate in boundaries[1:]:
        if start <= dt <= end:
            return rate
    return "6.7"

# ========= Main =========
if __name__ == "__main__":
    print("[INFO] 初始化 MSAL + Graph API...")
    client = GraphClient()
    site_id, list_id = init_graph(client)

    # 讀取 Excel
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    df = df[df["OU"].notna() & df["Sourcing month3"].notna()]
    print(f"[INFO] Excel 共 {len(df)} 行有效數據")

    success_count = 0
    create_count  = 0
    error_count   = 0

    for idx, row in df.iterrows():
        try:
            raw_ou  = row["OU"]
            ou      = normalize_ou(raw_ou)
            month3  = str(row["Sourcing month3"]).strip()
            title_key = f"{ou}-{month3}"
            print(f"處理第 {idx+1} 行: key = {title_key}")

            items = find_items(client, site_id, list_id, title_key)

            if len(items) == 0:
                print(f"  [CREATE] SharePoint 未有紀錄，建立新 item")
                pot_value = get_pot_value(month3)
                url_value = build_url(ou, month3)
                fields = {
                    "Title":       title_key,
                    "consumption": row.get("The electricity consumption (KWH) for the month", None),
                    "bill":        row.get("Bill", None),
                    "status":      "Approved",
                    "ou":          ou,
                    "pot":         pot_value,
                    "scoringMonth": month3,
                    "uri":         url_value
                }
                new_item = create_item(client, site_id, list_id, fields)
                print(f"  [OK] 已建立新 item，ID = {new_item.get('id')}")
                create_count += 1

            else:
                item    = items[0]
                item_id = item["id"]
                consumption = row.get("The electricity consumption (KWH) for the month", None)
                bill        = row.get("Bill", None)

                print(f"  Key: {title_key}")
                print(f"  Consumption: {consumption}  |  Bill: {bill}")

                fields = {
                    "consumption": consumption,
                    "bill":        bill,
                    "status":      "Approved"
                }
                update_item(client, site_id, list_id, item_id, fields)
                print(f"  [OK] 已更新 SharePoint item: {title_key}")
                success_count += 1

        except Exception as e:
            error_count += 1
            print(f"  [ERROR] 第 {idx+1} 行處理失敗: {str(e)}")
            continue

    print(f"\n=== 總結 ===")
    print(f"成功更新: {success_count} 行")
    print(f"新增項目: {create_count} 個")
    print(f"處理失敗: {error_count} 行")
    print(f"總處理:   {len(df)} 行")
