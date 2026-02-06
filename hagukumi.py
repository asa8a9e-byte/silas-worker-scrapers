"""
ハグクミ スクレイパー (SaaS Worker版)
ID総当り方式でリフォーム会社情報を収集
※ Seleniumは不要、HTTPリクエストのみ
"""
import re
import time
import random
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://hugkumi-life.jp/detail/index.php?id={}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# デフォルトID範囲
DEFAULT_START_ID = 1
DEFAULT_END_ID = 7500


class HagukumiScraper:
    """ハグクミからID総当りでリフォーム会社情報を収集"""

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

        print(f"[Hagukumi] ID範囲: {start_id} - {end_id} ({total}件)")

        retry_count = 0

        for shop_id in range(start_id, end_id + 1):
            if not self.is_running_check():
                print("[Hagukumi] 停止リクエスト受信")
                break

            result = self._scrape_shop(shop_id)

            if result == "retry":
                retry_count += 1
                if retry_count >= 3:
                    print(f"[Hagukumi] Rate limited at {shop_id}, waiting 30s...")
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
                print(f"[Hagukumi] [{shop_id}] ✓ {result.get('company_name', '')[:30]}")

            # リクエスト間隔
            time.sleep(random.uniform(0.3, 0.6))

            # 進捗報告（500件ごと）
            if shop_id % 500 == 0:
                print(f"[Hagukumi] Progress: {shop_id}/{end_id} ({self.result_count}件取得)")

        print(f"[Hagukumi] 完了: {self.result_count}件取得")
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

            # ページが存在するか確認
            if "area_detail_about_info" not in r.text:
                return None

            soup = BeautifulSoup(r.text, "html.parser")
            data = {"shop_id": shop_id, "url": url}

            # 会社名を取得
            company_raw = self._extract_text(soup, "会社名")
            data["company_name"] = self._clean_company_name(company_raw)

            if not data.get("company_name"):
                return None

            # その他の情報を取得
            data["address"] = self._extract_text(soup, "所在地")

            contact = self._extract_text(soup, "連絡先")
            data["phone"] = self._extract_tel(contact)

            data["website"] = self._extract_hp(soup)
            data["capital"] = self._extract_text(soup, "資本金")
            data["representative"] = self._extract_text(soup, "代表者")

            return data if data.get("company_name") else None

        except requests.exceptions.Timeout:
            return "retry"
        except Exception as e:
            print(f"[Hagukumi] Error at {shop_id}: {e}")
            return None

    def _extract_text(self, soup: BeautifulSoup, title: str) -> str:
        """指定タイトルの次のテキストを取得"""
        unit = soup.find("p", string=title)
        if unit:
            text_tag = unit.find_next("p", class_="text")
            if text_tag:
                return text_tag.get_text(strip=True)
        return ""

    def _clean_company_name(self, name: str) -> str:
        """会社名をクリーンアップ"""
        name = name.strip()
        bracket = re.search(r"\[(.+?)\]", name)
        if bracket:
            return bracket.group(1).strip()
        return name

    def _extract_hp(self, soup: BeautifulSoup) -> str:
        """ホームページURLを取得"""
        unit = soup.find("p", string="ホームページ")
        if unit:
            link = unit.find_next("a")
            if link:
                return link.get("href", "")
        return ""

    def _extract_tel(self, text: str) -> str:
        """連絡先から電話番号を抽出"""
        match = re.search(r"TEL：?(\d[\d\-]+)", text)
        return match.group(1) if match else ""
