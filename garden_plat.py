"""
ガーデンプラット スクレイパー (SaaS Worker版)
ID総当り方式で外構業者情報を収集
※ Seleniumは不要、HTTPリクエストのみ
"""
import re
import time
import random
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.garden-plat.net/sp/shop{}/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# デフォルトID範囲
DEFAULT_START_ID = 1
DEFAULT_END_ID = 1200


class GardenplatScraper:
    """ガーデンプラットからID総当りで外構業者情報を収集"""

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

        print(f"[GardenPlat] ID範囲: {start_id} - {end_id} ({total}件)")

        retry_count = 0

        for shop_id in range(start_id, end_id + 1):
            if not self.is_running_check():
                print("[GardenPlat] 停止リクエスト受信")
                break

            result = self._scrape_shop(shop_id)

            if result == "retry":
                retry_count += 1
                if retry_count >= 3:
                    print(f"[GardenPlat] Rate limited at {shop_id}, waiting 30s...")
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
                print(f"[GardenPlat] [{shop_id}] ✓ {result.get('company_name', '')[:30]}")

            # リクエスト間隔
            time.sleep(random.uniform(0.3, 0.6))

            # 進捗報告（100件ごと）
            if shop_id % 100 == 0:
                print(f"[GardenPlat] Progress: {shop_id}/{end_id} ({self.result_count}件取得)")

        print(f"[GardenPlat] 完了: {self.result_count}件取得")
        return self.result_count

    def _scrape_shop(self, shop_id: int) -> Optional[dict]:
        """個別店舗ページをスクレイピング"""
        url = BASE_URL.format(shop_id)

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)

            if r.status_code == 404:
                return None
            if r.status_code != 200:
                return "retry"

            soup = BeautifulSoup(r.text, "html.parser")
            data = {"shop_id": shop_id, "url": url}

            # 会社名を取得
            h2 = soup.select_one("h2.c-heading.is-xlg.is-bottom")
            data["company_name"] = self._clean_text(h2.get_text()) if h2 else ""

            if not data.get("company_name"):
                return None

            # ブロック要素から情報を抽出
            for blk in soup.select("div.c-block-two-column__content"):
                h4 = blk.find("h4")
                if not h4:
                    continue
                label = self._clean_text(h4.get_text())
                p = blk.find("p")
                if not p:
                    continue

                if label == "所在地":
                    raw = self._clean_text(p.get_text())
                    raw = raw.replace("Google Mapで見る", "").strip()
                    m = re.match(r"(\d{3}-\d{4})\s*(.+)", raw.replace("〒", ""))
                    if m:
                        data["zip_code"] = m.group(1)
                        data["address"] = m.group(2).strip()
                    else:
                        data["zip_code"] = ""
                        data["address"] = raw
                elif label == "電話番号":
                    text = p.get_text()
                    m = re.search(r"0\d{1,4}-\d{1,4}-\d{3,4}", text)
                    data["phone"] = m.group() if m else ""
                elif label in ["FAX番号", "FAX", "ファックス"]:
                    text = p.get_text()
                    m = re.search(r"0\d{1,4}-\d{1,4}-\d{3,4}", text)
                    data["fax"] = m.group() if m else ""
                elif label == "ホームページ":
                    a = p.find("a")
                    data["website"] = a["href"] if a and a.has_attr("href") else ""
                elif label == "得意工事":
                    data["specialty"] = self._clean_text(p.get_text())

            # デフォルト値を設定
            data.setdefault("zip_code", "")
            data.setdefault("address", "")
            data.setdefault("phone", "")
            data.setdefault("fax", "")
            data.setdefault("website", "")
            data.setdefault("specialty", "")

            return data if data.get("company_name") else None

        except requests.exceptions.Timeout:
            return "retry"
        except Exception as e:
            print(f"[GardenPlat] Error at {shop_id}: {e}")
            return None

    def _clean_text(self, t: str) -> str:
        """テキストをクリーンアップ"""
        if not t:
            return ""
        return re.sub(r"\s+", " ", t).strip()
