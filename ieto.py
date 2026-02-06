"""
イエト スクレイパー (SaaS Worker版)
中国・四国地方のビルダー情報を収集
※ Seleniumは不要、HTTPリクエストのみ
"""
import time
import random
from typing import Callable, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


IETO_DOMAIN = "https://ieto.stephouse.jp"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# 利用可能エリア
IETO_AREAS = {
    "ieto_okayama": "岡山",
    "ieto_fukuyama": "福山",
    "ieto_kagawa": "香川",
    "ieto_yamanashi": "山梨",
}


class IetoScraper:
    """イエトからビルダー情報を収集"""

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

        areas = filters.get("areas", list(IETO_AREAS.keys()))
        if not areas:
            areas = list(IETO_AREAS.keys())

        print(f"[Ieto] 対象エリア: {areas}")

        # 全エリアのリンクを収集
        all_links = []
        for area in areas:
            if not self.is_running_check():
                break
            links = self._get_builder_links(area)
            all_links.extend([(link, area) for link in links])

        total = len(all_links)
        print(f"[Ieto] 合計 {total}件のビルダーを発見")

        if self.progress_callback:
            self.progress_callback(0, total)

        # 各ビルダーページをスクレイピング
        for idx, (link, area) in enumerate(all_links):
            if not self.is_running_check():
                print("[Ieto] 停止リクエスト受信")
                break

            result = self._scrape_builder(link, area)

            if result:
                self.result_count += 1
                if self.result_callback:
                    self.result_callback(result)
                if self.progress_callback:
                    self.progress_callback(self.result_count, total)
                print(f"[Ieto] ✓ {result.get('name', '')[:30]}")

            # リクエスト間隔
            time.sleep(random.uniform(0.3, 0.6))

        print(f"[Ieto] 完了: {self.result_count}件取得")
        return self.result_count

    def _get_builder_links(self, area: str) -> list:
        """エリアからビルダーリンク一覧を取得"""
        url = f"{IETO_DOMAIN}/{area}/builder.html"
        links = set()

        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                print(f"[Ieto] [{area}] ページ取得失敗: {r.status_code}")
                return []

            soup = BeautifulSoup(r.text, "html.parser")

            for a in soup.select("a"):
                href = a.get("href", "")
                if href and "builder/" in href and href.endswith(".html"):
                    full = urljoin(IETO_DOMAIN, href)
                    links.add(full)

            print(f"[Ieto] [{area}] {len(links)}件発見")
            return sorted(links)

        except Exception as e:
            print(f"[Ieto] [{area}] エラー: {e}")
            return []

    def _scrape_builder(self, url: str, area: str) -> Optional[dict]:
        """個別ビルダーページをスクレイピング"""
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                return None

            soup = BeautifulSoup(r.text, "html.parser")
            data = {"url": url, "area": area, "area_name": IETO_AREAS.get(area, area)}

            # タイトル取得
            try:
                h1 = soup.select_one("h1")
                if h1:
                    data["name"] = h1.get_text(strip=True)
            except:
                pass

            # テーブル情報取得
            def get_text(th_text):
                th = soup.find("th", string=th_text)
                if th and th.find_next("td"):
                    return th.find_next("td").get_text(strip=True)
                return ""

            data["address"] = get_text("所在地")
            data["hours"] = get_text("営業時間")
            data["service_area"] = get_text("エリア")
            data["price_range"] = get_text("取扱坪単価")
            data["main_price"] = get_text("最多坪単価")
            data["phone"] = get_text("電話番号")
            data["holiday"] = get_text("定休日")
            data["founded"] = get_text("設立")
            data["employees"] = get_text("従業員数")

            # URL取得
            th = soup.find("th", string="URL")
            if th:
                a = th.find_next("a")
                if a:
                    data["website"] = a.get("href", "")

            # SNS取得
            sns = self._extract_sns(soup)
            data.update(sns)

            return data if data.get("name") else None

        except Exception as e:
            print(f"[Ieto] Error at {url}: {e}")
            return None

    def _extract_sns(self, soup) -> dict:
        """SNSリンクを抽出"""
        sns = {"instagram": "", "facebook": "", "x": "", "line": ""}
        sns_area = soup.select_one(".builder--info-sns")
        if sns_area:
            for a in sns_area.find_all("a"):
                link = a.get("href", "")
                if "instagram" in link:
                    sns["instagram"] = link
                elif "facebook" in link:
                    sns["facebook"] = link
                elif "x.com" in link:
                    sns["x"] = link
                elif "line.me" in link:
                    sns["line"] = link
        return sns
