"""
リショップナビ スクレイパー (SaaS Worker版)
ID総当り方式でリフォーム会社情報を収集
※ Seleniumは不要、HTTPリクエストのみ
"""
import time
import random
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://rehome-navi.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# デフォルトID範囲
DEFAULT_START_ID = 1
DEFAULT_END_ID = 9500


class ReshopnaviScraper:
    """リショップナビからID総当りでリフォーム会社情報を収集"""

    def __init__(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        result_callback: Optional[Callable[[dict], None]] = None,
        is_running_check: Optional[Callable[[], bool]] = None,
    ):
        self.progress_callback = progress_callback
        self.result_callback = result_callback
        self.is_running_check = is_running_check or (lambda: True)
        self.result_count = 0

    def run(self, filters: dict = None) -> int:
        """スクレイピングを実行"""
        filters = filters or {}
        self.result_count = 0

        start_id = filters.get("start_id", DEFAULT_START_ID)
        end_id = filters.get("end_id", DEFAULT_END_ID)
        total = end_id - start_id + 1

        print(f"[Reshopnavi] ID範囲: {start_id} - {end_id} ({total}件)")

        retry_count = 0

        for shop_id in range(start_id, end_id + 1):
            if not self.is_running_check():
                print("[Reshopnavi] 停止リクエスト受信")
                break

            result = self._scrape_shop(shop_id)

            if result == "retry":
                retry_count += 1
                if retry_count >= 3:
                    print(f"[Reshopnavi] Rate limited at {shop_id}, waiting 30s...")
                    time.sleep(30)
                    retry_count = 0
                continue

            retry_count = 0

            if result:
                self.result_count += 1
                if self.result_callback:
                    self.result_callback(result)
                if self.progress_callback:
                    self.progress_callback(self.result_count, total)
                print(f"[Reshopnavi] [{shop_id}] ✓ {result.get('company_name', '')[:30]}")

            # リクエスト間隔
            time.sleep(random.uniform(0.3, 0.6))

            # 進捗報告（500件ごと）
            if shop_id % 500 == 0:
                print(f"[Reshopnavi] Progress: {shop_id}/{end_id} ({self.result_count}件取得)")

        print(f"[Reshopnavi] 完了: {self.result_count}件取得")
        return self.result_count

    def _scrape_shop(self, shop_id: int) -> Optional[dict]:
        """個別店舗ページをスクレイピング"""
        url = f"{BASE_URL}/shops/{shop_id}"

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)

            if r.status_code == 404:
                return None
            if r.status_code != 200:
                return "retry"

            if "会社名" not in r.text:
                return None

            soup = BeautifulSoup(r.text, "html.parser")
            data = {"shop_id": shop_id, "url": url}

            for dl in soup.find_all("dl"):
                for dt in dl.find_all("dt"):
                    dd = dt.find_next("dd")
                    if not dd:
                        continue

                    label = dt.get_text(strip=True)
                    value = dd.get_text(strip=True)

                    if "会社名" in label:
                        data["company_name"] = value
                    elif "電話番号" in label:
                        data["phone"] = value
                    elif "住所" in label:
                        data["address"] = value
                    elif "資本金" in label:
                        data["capital"] = value
                    elif "代表者名" in label:
                        data["representative"] = value
                    elif "会社HP" in label:
                        link = dd.find("a")
                        data["website"] = link.get("href", "") if link else value

            # 会社名がなければh2から取得
            if not data.get("company_name"):
                for h2 in soup.find_all("h2"):
                    text = h2.get_text(strip=True)
                    if "株式会社" in text or "有限会社" in text or "合同会社" in text:
                        data["company_name"] = text
                        break

            return data if data.get("company_name") else None

        except requests.exceptions.Timeout:
            return "retry"
        except Exception as e:
            print(f"[Reshopnavi] Error at {shop_id}: {e}")
            return None
