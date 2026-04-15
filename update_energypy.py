import os
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from datetime import datetime
import time
import random
from dotenv import load_dotenv

load_dotenv()

# ========= 參數 =========
SITE_URL = "https://nanfung.sharepoint.com/sites/NFPM-App/POT"
LIST_TITLE = "Energy Efficiency"  # List 名，與介面同名即可

USERNAME = os.getenv("SP_USERNAME") 
PASSWORD = os.getenv("SP_PASSWORD")

EXCEL_PATH = "energy.xlsx"
SHEET_NAME = "Sheet1"
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

MAX_RETRIES = 3
BASE_DELAY = 2  # 秒

# ========= Retry 裝飾器 =========
def retry_sharepoint(max_retries=MAX_RETRIES, base_delay=BASE_DELAY):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except ClientRequestException as e:
                    last_exception = e
                    if "throttled" in str(e).lower() or "429" in str(e) or "503" in str(e):
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        print(f"  [RETRY {attempt+1}/{max_retries}] SharePoint 暫時錯誤: {str(e)[:100]}... 等待 {wait_time:.1f}秒")
                        time.sleep(wait_time)
                        continue
                    elif attempt == max_retries:
                        print(f"  [RETRY FAILED] 超過 {max_retries} 次重試，錯誤: {str(e)}")
                        raise
                    else:
                        wait_time = base_delay * attempt + random.uniform(0, 0.5)
                        print(f"  [RETRY {attempt+1}/{max_retries}] 錯誤: {str(e)[:100]}... 等待 {wait_time:.1f}秒")
                        time.sleep(wait_time)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        print(f"  [RETRY FAILED] 未知錯誤超過 {max_retries} 次重試: {str(e)}")
                        raise
                    wait_time = base_delay * attempt + random.uniform(0, 0.5)
                    print(f"  [RETRY {attempt+1}/{max_retries}] 未知錯誤: {str(e)[:100]}... 等待 {wait_time:.1f}秒")
                    time.sleep(wait_time)
            raise last_exception
        return wrapper
    return decorator

# ========= 建立 SharePoint 連線 =========
@retry_sharepoint(max_retries=MAX_RETRIES)
def connect_sharepoint():
    ctx = ClientContext(SITE_URL).with_credentials(UserCredential(USERNAME, PASSWORD))
    sp_list = ctx.web.lists.get_by_title(LIST_TITLE)
    ctx.load(sp_list)
    ctx.execute_query()
    print(f"[OK] SharePoint 連線成功: {LIST_TITLE}")
    return ctx, sp_list

ctx, sp_list = connect_sharepoint()

# ========= 讀 Excel =========
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# 防呆：只攞有 OU + Sourcing month3 的 row
df = df[df["OU"].notna() & df["Sourcing month3"].notna()]
print(f"[INFO] Excel 共 {len(df)} 行有效數據")

def parse_month3(month3: str) -> datetime:
    # month3 形如 "2019-07"
    return datetime.strptime(month3, "%Y-%m")

def build_url(ou: str, month3: str) -> str:
    # month3 -> year, month name
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
    # month3 形如 "2019-07"
    dt = datetime.strptime(month3, "%Y-%m")

    # 定義邊界（全部用 yyyy-mm-01）
    d_2017_10 = datetime(2017, 10, 1)
    d_2017_11 = datetime(2017, 11, 1)
    d_2018_10 = datetime(2018, 10, 1)
    d_2018_11 = datetime(2018, 11, 1)
    d_2019_10 = datetime(2019, 10, 1)
    d_2019_11 = datetime(2019, 11, 1)
    d_2020_10 = datetime(2020, 10, 1)
    d_2020_11 = datetime(2020, 11, 1)
    d_2021_10 = datetime(2021, 10, 1)
    d_2021_11 = datetime(2021, 11, 1)
    d_2022_10 = datetime(2022, 10, 1)
    d_2022_11 = datetime(2022, 11, 1)
    d_2024_03 = datetime(2024, 3, 1)

    if dt < d_2017_10:
        return "5.4"
    elif d_2017_11 <= dt <= d_2018_10:
        return "5.5"
    elif d_2018_11 <= dt <= d_2019_10:
        return "5.7"
    elif d_2019_11 <= dt <= d_2020_10:
        return "5.6"
    elif d_2020_11 <= dt <= d_2021_10:
        return "6.4"
    elif d_2021_11 <= dt <= d_2022_10:
        return "6.6"
    elif d_2022_11 <= dt <= d_2024_03:
        return "6.6"
    else:
        return "6.7"

# ========= Retry 包裝的 SharePoint 操作 =========
@retry_sharepoint(max_retries=MAX_RETRIES)
def safe_execute_query(ctx):
    ctx.execute_query()

@retry_sharepoint(max_retries=MAX_RETRIES)
def find_sp_items(sp_list, title_key):
    items = sp_list.items.filter(f"Title eq '{title_key}'").get()
    safe_execute_query(ctx)
    return items

@retry_sharepoint(max_retries=MAX_RETRIES)
def create_sp_item(sp_list, create_info):
    new_item = sp_list.add_item(create_info)
    safe_execute_query(ctx)
    return new_item

@retry_sharepoint(max_retries=MAX_RETRIES)
def update_sp_item(item):
    item.update()
    safe_execute_query(ctx)

# ========= 逐行處理 =========
success_count = 0
create_count = 0
error_count = 0

for idx, row in df.iterrows():
    try:
        raw_ou = row["OU"]
        ou = normalize_ou(raw_ou)
        month3 = str(row["Sourcing month3"]).strip()  # e.g. "2019-07"

        title_key = f"{ou}-{month3}"  # e.g. "NFC-2019-07" / "TKO MALL-2019-07"
        print(f"處理第 {idx+1} 行: key = {title_key}")

        # 尋找對應 SharePoint item (Title = key)
        items = find_sp_items(sp_list, title_key)

        if len(items) == 0:
            print(f"  [CREATE] SharePoint 未有紀錄，建立新 item")
            pot_value = get_pot_value(month3)
            url_value = build_url(ou, month3)
            create_info = {
                "Title": title_key,
                "consumption": row.get("The electricity consumption (KWH) for the month", None),  # internal name 要確認
                "bill": row.get("Bill", None),
                "status": "Approved",
                "ou": ou,
                "pot": pot_value,
                "scoringMonth": month3,
                "uri": url_value
            }

            new_item = create_sp_item(sp_list, create_info)
            print(f"  [OK] 已建立新 item，ID = {new_item.properties.get('Id')}")
            create_count += 1
            
        else:
            item = items[0]

            # 取 Excel 欄位數值
            consumption = row.get("The electricity consumption (KWH) for the month", None)
            bill = row.get("Bill", None)
            created_by = row.get("Created By", None)

            print(f"  Key: {title_key}")
            print(f"  Consumption: {consumption}  |  Bill: {bill}")

            # 設定欄位
            item.set_property("consumption", consumption)
            item.set_property("bill", bill)
            item.set_property("status", "Approved")

            update_sp_item(item)
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
print(f"總處理: {len(df)} 行")
