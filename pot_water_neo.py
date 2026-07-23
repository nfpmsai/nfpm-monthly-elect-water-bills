import os
import json
from datetime import date
from typing import Dict, Any, List, Optional

import requests
import msal
from urllib.parse import urlparse
from dotenv import load_dotenv


load_dotenv()

# ========= 參數 =========
SITE_URL = "https://nanfung.sharepoint.com/sites/NFPM-App/Technical"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

LIST_WATER_MASTER = "Water Meter Master"

MONGO_URI        = os.getenv("MONGO_URI")
MONGO_DB         = os.getenv("MONGO_DB", "NFPM_CORE")
MONGO_SECRET     = os.getenv("MONGO_SECRET")
MONGO_COLLECTION = "water_bill_accounts"

SP_CLIENT_ID     = os.getenv("SHAREPOINT_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SP_TENANT_ID     = os.getenv("SHAREPOINT_TENANT_ID")


# ========= SharePoint Graph Client =========
class SharePointGraphClient:
    def __init__(self, site_url: str, client_id: str, client_secret: str, tenant_id: str):
        self.site_url      = site_url
        self.client_id     = client_id
        self.client_secret = client_secret
        self.tenant_id     = tenant_id
        self.headers: Optional[Dict[str, str]] = None
        self.site_id: Optional[str] = None

    def authenticate(self):
        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise RuntimeError("缺少 SHAREPOINT_CLIENT_ID / SHAREPOINT_CLIENT_SECRET / SHAREPOINT_TENANT_ID")

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
        token_result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" not in token_result:
            raise RuntimeError(f"Graph 認證失敗: {token_result}")

        self.headers = {
            "Authorization": f"Bearer {token_result['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.site_id = self._resolve_site_id()

    def _resolve_site_id(self) -> str:
        parsed   = urlparse(self.site_url)
        hostname = parsed.netloc
        path     = parsed.path
        url  = f"{GRAPH_BASE}/sites/{hostname}:{path}?$select=id,displayName,webUrl"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        print(f"連線成功: {data.get('displayName')}")
        return data["id"]

    def _get_list_id(self, list_name: str) -> str:
        url  = f"{GRAPH_BASE}/sites/{self.site_id}/lists?$select=id,displayName,name"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        for lst in resp.json().get("value", []):
            if lst.get("displayName") == list_name or lst.get("name") == list_name:
                return lst["id"]
        raise RuntimeError(f"找不到 SharePoint List: {list_name}")

    def get_all_list_items(self, list_name: str, top: int = 999) -> List[Dict[str, Any]]:
        list_id = self._get_list_id(list_name)
        url     = f"{GRAPH_BASE}/sites/{self.site_id}/lists/{list_id}/items?$expand=fields&$top={top}"

        results = []
        while url:
            resp = requests.get(url, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("value", []):
                fields       = item.get("fields", {})
                fields["ID"] = item.get("id")
                results.append(fields)
            url = data.get("@odata.nextLink")

        return results


def create_sharepoint_client() -> SharePointGraphClient:
    client = SharePointGraphClient(
        site_url=SITE_URL,
        client_id=SP_CLIENT_ID,
        client_secret=SP_CLIENT_SECRET,
        tenant_id=SP_TENANT_ID,
    )
    client.authenticate()
    return client


# ========= 核心業務邏輯 =========
def _safe_get(props, key, default=None):
    return props.get(key, default) if props else default


def build_water_meter_array(sp_client: SharePointGraphClient) -> List[Dict[str, Any]]:
    meters = sp_client.get_all_list_items(LIST_WATER_MASTER)
    today  = date.today()
    
    #today = date(2026, 4, 1)

    # Scoring month = previous calendar month
    if today.month == 1:
        scoring_year  = today.year - 1
        scoring_month = 12
    else:
        scoring_year  = today.year
        scoring_month = today.month - 1

    result = []
    for props in meters:
        account_no = _safe_get(props, "AccountNo")
        if not account_no:
            continue
        
        ou = _safe_get(props, "OU")
        ou = 'TKO MALL' if ou == 'TKO (Comm.)' else ou
        ou = 'MOS MALL' if ou == 'MOS (Comm.)' else ou
        result.append({
            "document_type":                "water_bills",
            "portfolio":                    _safe_get(props, "Profolio"),
            "district":                     _safe_get(props, "District"),
            "ou":                           ou,
            "account_no":                   account_no,
            "year":                 scoring_year,
            "month":                scoring_month,
            "not_all_data_received":            False,
            "month_of_wsd_billing":         f"{scoring_year}-{scoring_month:02d}",
            "cubic_meter_of_potable_water": None,
            "potable_water_reduction":      None,
            "is_active":                    True,
            "allow_deduction":              _safe_get(props, allow_deduction)
        })

    return result


# ========= MongoDB REST API =========
def api_request(method: str, endpoint: str, database: str = "NFPM_CORE",
                collection: str = None, filter: dict = None,
                data=None, fields: str = None) -> Dict[str, Any]:
    base_url = MONGO_URI
    headers  = {"x-api-key": MONGO_SECRET}

    params = {}
    if database:
        params["db"] = database
    if collection:
        params["collection"] = collection
    if filter:
        params["filter"] = json.dumps(filter)
    if fields:
        params["fields"] = fields

    try:
        if method == "GET":
            response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        elif method == "POST":
            response = requests.post(f"{base_url}{endpoint}", headers=headers, params=params, json=data)
        elif method == "PUT":
            response = requests.put(f"{base_url}{endpoint}", headers=headers, params=params, json=data)
        elif method == "DELETE":
            response = requests.delete(f"{base_url}{endpoint}", headers=headers, params=params)
        else:
            raise ValueError(f"Invalid HTTP method: {method}")

        response.raise_for_status()
        json_response = response.json()

        if isinstance(json_response, list):
            return {"status": "success", "message": "Request returned a list", "data": json_response}

        return {
            "status":  "success",
            "message": json_response.get("message", "Request successful"),
            "data":    json_response.get("data", json_response),
        }

    except requests.exceptions.RequestException as e:
        print(str(e))
        return {"status": "error", "message": str(e), "data": None}


# ========= Main =========
def main():
    print("[INFO] 連線 SharePoint...")
    sp_client  = create_sharepoint_client()

    print("[INFO] 讀取 Water Meter Master...")
    meter_docs = build_water_meter_array(sp_client)
    print(f"[INFO] 共 {len(meter_docs)} 筆水錶記錄")
    print(meter_docs)
    if not meter_docs:
        print("[WARN] 沒有有效記錄，跳過插入")
        return

    print(f"[INFO] 插入 MongoDB collection: {MONGO_COLLECTION}...")
    result = api_request("POST", "/", database=MONGO_DB, collection=MONGO_COLLECTION, data=meter_docs)
    print(result)


if __name__ == "__main__":
    main()
