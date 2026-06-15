import os
import time
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

import requests
import msal
from dotenv import load_dotenv

load_dotenv()

SITE_URL = "https://nanfung.sharepoint.com/sites/NFPM-App"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

DOCUMENT_LIBRARY = "Documents"
ROOT_FOLDER = "OCR Supporting Document"
TARGET_SUBFOLDERS = ["Electricity_Bills", "LnD_Attendance", "Water_Bills"]

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
        self.drive_id: Optional[str] = None

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
        self.drive_id = self._resolve_drive_id(DOCUMENT_LIBRARY)

    def _resolve_site_id(self) -> str:
        parsed = urlparse(self.site_url)
        hostname = parsed.netloc
        path = parsed.path
        url = f"{GRAPH_BASE}/sites/{hostname}:{path}?$select=id,displayName,webUrl"
        resp = requests.get(url, headers=self.headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        print(f"連線成功: {data.get('displayName')}")
        return data["id"]

    def _resolve_drive_id(self, drive_name: str) -> str:
        url = f"{GRAPH_BASE}/sites/{self.site_id}/drives"
        resp = requests.get(url, headers=self.headers, timeout=60)
        resp.raise_for_status()

        for drv in resp.json().get("value", []):
            if drv.get("name") == drive_name:
                print(f"文件庫: {drv.get('name')}")
                return drv["id"]

        raise RuntimeError(f"找不到文件庫: {drive_name}")

    def _is_retryable_status(self, status_code: int) -> bool:
        return status_code in {429, 500, 502, 503, 504}

    def _get_retry_delay(self, response, attempt: int, base_delay: int = 2, max_delay: int = 60) -> int:
        retry_after = response.headers.get("Retry-After")

        if retry_after:
            try:
                return max(1, min(int(float(retry_after)), max_delay))
            except ValueError:
                try:
                    retry_dt = parsedate_to_datetime(retry_after)
                    delay = int((retry_dt - datetime.now(retry_dt.tzinfo)).total_seconds())
                    return max(1, min(delay, max_delay))
                except Exception:
                    pass

        return min(base_delay * (2 ** (attempt - 1)), max_delay)

    def _get(self, url: str) -> requests.Response:
        resp = requests.get(url, headers=self.headers, timeout=60)
        resp.raise_for_status()
        return resp

    def _post_with_retry(self, url: str, payload: Dict[str, Any], max_retries: int = 5) -> Dict[str, Any]:
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(url, headers=self.headers, json=payload, timeout=60)

                if resp.status_code < 400:
                    return resp.json()

                if not self._is_retryable_status(resp.status_code):
                    resp.raise_for_status()

                delay = self._get_retry_delay(resp, attempt)
                print(f"[RETRY {attempt}/{max_retries}] HTTP {resp.status_code}, wait {delay}s")
                last_error = requests.HTTPError(
                    f"{resp.status_code} Error: {resp.text}",
                    response=resp
                )

                if attempt < max_retries:
                    time.sleep(delay)
                    continue

                resp.raise_for_status()

            except requests.RequestException as e:
                last_error = e
                if attempt < max_retries:
                    delay = min(2 ** (attempt - 1), 60)
                    print(f"[RETRY {attempt}/{max_retries}] RequestException, wait {delay}s -> {e}")
                    time.sleep(delay)
                    continue
                raise

        if last_error:
            raise last_error

        raise RuntimeError("Unknown error in _post_with_retry")

    def list_children_by_path(self, folder_path: str) -> List[Dict[str, Any]]:
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{folder_path}:/children?$top=999"
        results = []

        while url:
            resp = self._get(url)
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        return results

    def create_folder_if_not_exists(self, parent_path: str, folder_name: str) -> Dict[str, Any]:
        children = self.list_children_by_path(parent_path)

        for item in children:
            if item.get("name") == folder_name and "folder" in item:
                return {
                    "status": "exists",
                    "name": folder_name,
                    "webUrl": item.get("webUrl")
                }

        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{parent_path}:/children"
        payload = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "fail"
        }

        try:
            data = self._post_with_retry(url, payload, max_retries=5)
            return {
                "status": "created",
                "name": folder_name,
                "webUrl": data.get("webUrl")
            }
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            if resp is not None and resp.status_code == 409:
                return {
                    "status": "exists",
                    "name": folder_name,
                    "webUrl": None
                }
            raise


def create_sharepoint_client():
    client = SharePointGraphClient(
        site_url=SITE_URL,
        client_id=SP_CLIENT_ID,
        client_secret=SP_CLIENT_SECRET,
        tenant_id=SP_TENANT_ID,
    )
    client.authenticate()
    return client


def get_previous_month_folder_name(ref_date: Optional[date] = None) -> str:
    ref_date = ref_date or date.today()
    year = ref_date.year
    month = ref_date.month - 1

    if month == 0:
        year -= 1
        month = 12

    return f"{year:04d}-{month:02d}"


def get_ou_codes(sp_client: SharePointGraphClient) -> List[str]:
    items = sp_client.list_children_by_path(ROOT_FOLDER)
    ou_codes = [
        item["name"]
        for item in items
        if "folder" in item
    ]
    return sorted(ou_codes)


def create_month_folders_for_all_ou(sp_client: SharePointGraphClient) -> List[Dict[str, Any]]:
    target_month = get_previous_month_folder_name()
    ou_codes = get_ou_codes(sp_client)
    results = []

    print(f"目標月份資料夾: {target_month}")
    print(f"找到 OU 數量: {len(ou_codes)}")

    for ou in ou_codes:
        print(f"\n處理 OU: {ou}")

        for subfolder in TARGET_SUBFOLDERS:
            parent_path = f"{ROOT_FOLDER}/{ou}/{subfolder}"

            try:
                result = sp_client.create_folder_if_not_exists(parent_path, target_month)
                results.append({
                    "ou": ou,
                    "subfolder": subfolder,
                    "month_folder": target_month,
                    "status": result["status"],
                    "webUrl": result.get("webUrl")
                })
                print(f"[{result['status'].upper()}] {parent_path}/{target_month}")

            except Exception as e:
                results.append({
                    "ou": ou,
                    "subfolder": subfolder,
                    "month_folder": target_month,
                    "status": "error",
                    "error": str(e)
                })
                print(f"[ERROR] {parent_path}/{target_month} -> {e}")

    return results


def print_summary(results: List[Dict[str, Any]]):
    created = sum(1 for r in results if r["status"] == "created")
    exists = sum(1 for r in results if r["status"] == "exists")
    errors = sum(1 for r in results if r["status"] == "error")

    print("\n===== SUMMARY =====")
    print(f"Created: {created}")
    print(f"Exists : {exists}")
    print(f"Errors : {errors}")

    if errors > 0:
        print("\n===== ERROR DETAILS =====")
        for r in results:
            if r["status"] == "error":
                print(f"{r['ou']} / {r['subfolder']} / {r['month_folder']} -> {r['error']}")


def main():
    sp_client = create_sharepoint_client()
    results = create_month_folders_for_all_ou(sp_client)
    print_summary(results)


if __name__ == "__main__":
    main()