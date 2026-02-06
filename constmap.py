"""
コンストマップ スクレイパー (SaaS Worker版)
関西・九州地方の建設業者情報を収集
"""
import time
import random
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


# エリア定義
CONSTMAP_REGIONS = {
    "kyushu": {
        "name": "九州",
        "base_url": "https://sumitec-9shu.com",
        "areas": None,  # 全件スキャン
    },
    "kansai": {
        "name": "関西・中国・四国",
        "base_url": "https://sumitec-kansai.com",
        "areas": [
            # 関西
            "osaka", "kyoto", "hyogo", "nara", "shiga", "wakayama",
            # 中部
            "mie", "aichi", "gifu", "fukui",
            # 中国
            "okayama", "hiroshima", "tottori",
            # 四国
            "tokusima", "kagawa", "ehime", "kochi",
        ],
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class ConstmapScraper:
    """コンストマップから建設業者情報を収集"""

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

        # 対象リージョン（デフォルトは両方）
        regions = filters.get("regions", list(CONSTMAP_REGIONS.keys()))

        for region_key in regions:
            if not self.is_running_check():
                print("[Constmap] 停止リクエスト受信")
                break

            if region_key not in CONSTMAP_REGIONS:
                continue

            region = CONSTMAP_REGIONS[region_key]
            print(f"[Constmap] {region['name']} スキャン開始...")

            if region["areas"]:
                # エリア別スキャン（関西版）
                self._scrape_by_areas(region)
            else:
                # 全ページスキャン（九州版）
                self._scrape_all_pages(region)

        return self.result_count

    def _scrape_all_pages(self, region: dict):
        """全ページをスキャン（九州版用）"""
        base_url = region["base_url"]
        seen_urls = set()

        for page in range(1, 100):  # 最大100ページ
            if not self.is_running_check():
                break

            if page == 1:
                url = f"{base_url}/?s"
            else:
                url = f"{base_url}/page/{page}?s"

            print(f"[Constmap] ページ {page} スキャン中...")

            try:
                r = requests.get(url, headers=HEADERS, timeout=15)
                if r.status_code != 200:
                    break

                soup = BeautifulSoup(r.text, "html.parser")
                links = soup.select('a[href*="/contractor/"]')

                # 詳細URLを抽出
                detail_urls = set()
                for link in links:
                    href = link.get("href", "")
                    if href and "/contractor/" in href and href.rstrip("/")[-1].isdigit():
                        if not href.startswith("http"):
                            href = base_url + href
                        detail_urls.add(href)

                if not detail_urls:
                    print(f"[Constmap] ページ {page}: リンクなし、終了")
                    break

                # 新規URLのみ処理
                new_urls = detail_urls - seen_urls
                if not new_urls:
                    print(f"[Constmap] ページ {page}: 新規なし、終了")
                    break

                seen_urls.update(new_urls)
                print(f"[Constmap] ページ {page}: {len(new_urls)}件")

                for detail_url in new_urls:
                    if not self.is_running_check():
                        break

                    self._scrape_detail(detail_url, region["name"])
                    time.sleep(random.uniform(0.3, 0.6))

                time.sleep(0.5)

            except Exception as e:
                print(f"[Constmap] ページエラー: {e}")
                break

    def _scrape_by_areas(self, region: dict):
        """エリア別にスキャン（関西版用）"""
        base_url = region["base_url"]

        for area in region["areas"]:
            if not self.is_running_check():
                break

            print(f"[Constmap] エリア: {area}")
            area_base_url = f"{base_url}/contractor/area_cat/{area}"
            seen_urls = set()
            page = 1

            while True:
                if not self.is_running_check():
                    break

                url = f"{area_base_url}/page/{page}" if page > 1 else area_base_url

                try:
                    r = requests.get(url, headers=HEADERS, timeout=15)
                    if r.status_code != 200:
                        break

                    soup = BeautifulSoup(r.text, "html.parser")

                    # 詳細リンクを抽出
                    detail_urls = []
                    for a in soup.select('a[href*="/contractor/"]'):
                        href = a.get("href", "")
                        if href and href.rstrip("/")[-1].isdigit():
                            if not href.startswith("http"):
                                href = base_url + href
                            if href not in seen_urls:
                                detail_urls.append(href)
                                seen_urls.add(href)

                    if not detail_urls:
                        break

                    print(f"[Constmap] {area} ページ{page}: {len(detail_urls)}件")

                    for detail_url in detail_urls:
                        if not self.is_running_check():
                            break

                        self._scrape_detail(detail_url, area)
                        time.sleep(random.uniform(0.3, 0.6))

                    page += 1
                    time.sleep(0.5)

                except Exception as e:
                    print(f"[Constmap] エリアエラー ({area}): {e}")
                    break

    def _scrape_detail(self, url: str, area: str):
        """詳細ページから情報を抽出"""
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                return

            soup = BeautifulSoup(r.text, "html.parser")

            # 店名
            name_tag = soup.select_one("h2.h.mainTxt")
            company_name = ""
            if name_tag:
                # ルビ（読み仮名）を除去
                ruby = name_tag.find("span", class_="sm")
                if ruby:
                    ruby.extract()
                company_name = name_tag.get_text(strip=True)

            if not company_name:
                return

            # 住所
            address = ""
            address_th = soup.find("th", string="住所")
            if address_th:
                address = address_th.find_next("td").get_text(strip=True)

            # 電話番号
            phone = ""
            phone_th = soup.find("th", string="電話番号")
            if phone_th:
                phone = phone_th.find_next("td").get_text(strip=True)

            # ホームページ
            website = ""
            hp_th = soup.find("th", string="ホームページ")
            if hp_th:
                a_tag = hp_th.find_next("td").find("a")
                if a_tag:
                    website = a_tag.get("href", "")

            data = {
                "company_name": company_name,
                "area": area,
                "address": address,
                "phone": phone,
                "website": website,
                "source_url": url,
            }

            self.result_count += 1
            if self.result_callback:
                self.result_callback(data)
            if self.progress_callback:
                self.progress_callback(self.result_count, 0)

            print(f"[Constmap] {company_name[:30]}")

        except Exception as e:
            print(f"[Constmap] 詳細取得エラー: {e}")
