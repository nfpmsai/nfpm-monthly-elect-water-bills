import os
import json
from datetime import date, datetime, timedelta
from calendar import monthrange
from typing import Dict, Any, List, Optional

import requests
import msal
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

SITE_URL = "https://nanfung.sharepoint.com/sites/NFPM-App/Technical"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

LIST_METER_MASTER = "Electricity Meter Master"
LIST_SUBMETER_MASTER = "Electricity Submeter Master"

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "NFPM_CORE")
MONGO_SECRET = os.getenv("MONGO_SECRET")
MONGO_COLLECTION = "electricity_bill_accounts"

SP_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SP_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")


class SharePointGraphClient:
    def __init__(self, site_url: str, client_id: str, client_secret: str, tenant_id: str):
        self.site_url = site_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.headers: Optional[Dict[str, str]] = None
        self.site_id: Optional[str] = None

    def authenticate(self):
        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise RuntimeError("缺少 SHAREPOINT_CLIENT_ID / SHAREPOINT_CLIENT_SECRET / SHAREPOINT_TENANT_ID")

        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
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
        parsed = urlparse(self.site_url)
        hostname = parsed.netloc
        path = parsed.path
        url = f"{GRAPH_BASE}/sites/{hostname}:{path}?$select=id,displayName,webUrl"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        print(f"連線成功: {data.get('displayName')}")
        return data["id"]

    def _get_list_id(self, list_name: str) -> str:
        url = f"{GRAPH_BASE}/sites/{self.site_id}/lists?$select=id,displayName,name"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()

        for lst in resp.json().get("value", []):
            if lst.get("displayName") == list_name or lst.get("name") == list_name:
                return lst["id"]

        raise RuntimeError(f"找不到 SharePoint List: {list_name}")

    def get_all_list_items(self, list_name: str, top: int = 999) -> List[Dict[str, Any]]:
        list_id = self._get_list_id(list_name)
        url = f"{GRAPH_BASE}/sites/{self.site_id}/lists/{list_id}/items?$expand=fields&$top={top}"

        results = []
        while url:
            resp = requests.get(url, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("value", []):
                fields = item.get("fields", {})
                fields["ID"] = item.get("id")
                results.append(fields)

            url = data.get("@odata.nextLink")

        return results


def create_sharepoint_client():
    client = SharePointGraphClient(
        site_url=SITE_URL,
        client_id=SP_CLIENT_ID,
        client_secret=SP_CLIENT_SECRET,
        tenant_id=SP_TENANT_ID,
    )
    client.authenticate()
    return client


def _safe_get(props, key, default=None):
    return props.get(key, default) if props else default


def get_all_list_items(sp_client, list_title: str):
    return sp_client.get_all_list_items(list_title)


def get_submeters_by_master_meter(sp_client, master_meter_no: str):
    all_submeters = get_all_list_items(sp_client, LIST_SUBMETER_MASTER)
    return [
        item for item in all_submeters
        if _safe_get(item, "MasterMeterNo") == master_meter_no
    ]


def build_electricity_meter_array(sp_client):
    meters = get_all_list_items(sp_client, LIST_METER_MASTER)
    result = []

    for props in meters:
        district = _safe_get(props, "District")
        ou = _safe_get(props, "OU")
        meter_no = _safe_get(props, "MeterNo")
        meter_calc = _safe_get(props, "CalType")
        sample_month = _safe_get(props, "MonthNo")
        period_start = _safe_get(props, "PeriodStart")
        period_end = _safe_get(props, "PeriodEnd")

        if not meter_no:
            continue

        new_year = date.today().year
        new_month = date.today().month - (int(sample_month) - get_month(period_start))
        if new_month == 0:
            new_year = date.today().year - 1
            new_month = 12
        new_period_start = cal_date(period_start, new_year, new_month)

        new_year = date.today().year
        new_month = date.today().month - (int(sample_month) - get_month(period_end))
        if new_month > 12:
            new_year = date.today().year + 1
            new_month = 1
        new_period_end = cal_date(period_end, new_year, new_month)

        if meter_calc == "Direct":
            result.append({
                "district": district,
                "ou": ou,
                "meter_no": meter_no,
                "meter_calculation": "Direct",
                "sub_meter_id": None,
                "sub_meter_no": None,
                "sub_meter_zone": None,
                "ct_meter_formula": None,
                "input_type": None,
                "bill_not_received": "N",
                "year": date.today().year,
                "month": date.today().month,
                "consumption": None,
                "bill_amount": None,
                "period_start": new_period_start,
                "period_end": new_period_end,
                "is_active": True
            })

        elif meter_calc == "Submetered":
            sub_items = get_submeters_by_master_meter(sp_client, meter_no)
            for sp in sub_items:
                result.append({
                    "district": district,
                    "ou": ou,
                    "account_no": meter_no,
                    "meter_calculation": "SubMeter",
                    "sub_meter_id": _safe_get(sp, "SubMeterID"),
                    "sub_meter_no": _safe_get(sp, "SubMeterNumber"),
                    "sub_meter_zone": _safe_get(sp, "SubMeterZone"),
                    "ct_meter_formula": _safe_get(sp, "CTMeterFormula"),
                    "input_type": _safe_get(sp, "InputType"),
                    "bill_not_received": "N",
                    "year": date.today().year,
                    "month": date.today().month,
                    "consumption": None,
                    "bill_amount": None,
                    "period_start": new_period_start,
                    "period_end": new_period_end,
                    "is_active": True
                })

    return result


def api_request(method, endpoint, database="NFPM_CORE", collection=None, filter=None, data=None, fields=None):
    base_url = MONGO_URI
    headers = {"x-api-key": MONGO_SECRET}

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
            raise ValueError("Invalid HTTP method")

        response.raise_for_status()
        json_response = response.json()

        if isinstance(json_response, list):
            return {
                "status": "success",
                "message": "Request returned a list",
                "data": json_response
            }

        return {
            "status": "success",
            "message": json_response.get("message", "Request successful"),
            "data": json_response.get("data", json_response)
        }
    except requests.exceptions.RequestException as e:
        print(str(e))
        return {
            "status": "error",
            "message": str(e),
            "data": None
        }


def last_day_of_month(year, month):
    _, last_day = monthrange(year, month)
    return last_day


def cal_date(ref_date, target_year, target_month):
    dt = datetime.fromisoformat(ref_date.replace("Z", "+00:00"))
    dt_plus_8h = dt + timedelta(hours=8)
    last_day = last_day_of_month(target_year, target_month)
    new_day = min(dt_plus_8h.day, last_day)
    new_dt = dt_plus_8h.replace(year=target_year, month=target_month, day=new_day)
    return new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_month(ref_date):
    dt = datetime.fromisoformat(ref_date.replace("Z", "+00:00"))
    dt_plus_8h = dt + timedelta(hours=8)
    return dt_plus_8h.month


def main():
    sp_client = create_sharepoint_client()
    meter_docs = build_electricity_meter_array(sp_client)
    print(meter_docs)
    inserted = api_request("POST", "/", collection=MONGO_COLLECTION, data=meter_docs)
    print(inserted)


if __name__ == "__main__":
    main()