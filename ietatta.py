"""
イエタッタ スクレイパー (SaaS Worker版)
全国12地域の住宅会社情報を収集
"""
import time
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


# 地域情報
IETATTA_REGIONS = {
    "ehime": {
        "name": "愛媛",
        "url": "https://ehime-ietatta.com/company/{}/",
        "default_start": 1,
        "default_end": 200,
    },
    "saitama": {
        "name": "埼玉",
        "url": "https://saitama.ie-tatta.com/company/{}/",
        "default_start": 1,
        "default_end": 350,
    },
    "ishikawa": {
        "name": "石川",
        "url": "https://www.xn----566as40brkc895c.com/company/{}",
        "default_start": 8500,
        "default_end": 12500,
    },
    "toyama": {
        "name": "富山",
        "url": "https://www.xn----566as40bbian2a.com/company/{}",
        "default_start": 9500,
        "default_end": 12500,
    },
    "ibaraki": {
        "name": "茨城",
        "url": "https://www.ie-tateru.jp/company/{}/",
        "default_start": 11200,
        "default_end": 12200,
    },
    "iwate": {
        "name": "岩手",
        "url": "https://ietatta-iwate.com/company/{}/",
        "default_start": 11600,
        "default_end": 12300,
    },
    "kansai": {
        "name": "関西",
        "url": "https://kansai-ietatta.com/company/{}/",
        "default_start": 1,
        "default_end": 550,
    },
    "kagoshima": {
        "name": "鹿児島",
        "url": "https://kagoshima-ie.com/company/{}/",
        "default_start": 11400,
        "default_end": 11900,
    },
    "fukushima": {
        "name": "福島",
        "url": "https://ietatta-fukushima.com/company/{}/",
        "default_start": 11900,
        "default_end": 12350,
    },
    "miyagi": {
        "name": "宮城",
        "url": "https://ietatta-miyagi.com/company/{}/",
        "default_start": 11900,
        "default_end": 12400,
    },
    "yamagata": {
        "name": "山形",
        "url": "https://ietatta-yamagata.com/company/{}/",
        "default_start": 11400,
        "default_end": 11900,
    },
    "fukuoka": {
        "name": "福岡",
        "url": "https://fukuoka-ietatta.com/company/{}/",
        "default_start": 11500,
        "default_end": 11900,
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class IetattaScraper:
    """イエタッタから住宅会社情報を収集"""

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

        regions = filters.get("regions", list(IETATTA_REGIONS.keys()))
        total_regions = len(regions)

        for idx, region in enumerate(regions):
            if not self.is_running_check():
                print("[Ietatta] 停止リクエスト受信")
                break

            region_info = IETATTA_REGIONS.get(region)
            if not region_info:
                print(f"[Ietatta] 不明な地域: {region}")
                continue

            region_name = region_info["name"]
            start_id = filters.get("start_id", region_info["default_start"])
            end_id = filters.get("end_id", region_info["default_end"])

            print(f"[Ietatta] [{idx+1}/{total_regions}] {region_name} (ID: {start_id}-{end_id})")
            self._scrape_region(region, region_info, start_id, end_id)

        return self.result_count

    def _scrape_region(self, region: str, region_info: dict, start_id: int, end_id: int):
        """特定の地域をスクレイピング"""
        base_url = region_info["url"]
        region_name = region_info["name"]
        total = end_id - start_id + 1

        for idx, i in enumerate(range(start_id, end_id + 1)):
            if not self.is_running_check():
                break

            if self.progress_callback:
                self.progress_callback(self.result_count, total)

            if i % 100 == 0:
                print(f"[Ietatta] {region_name} ID {i} / {end_id}")

            try:
                url = base_url.format(i)
                r = requests.get(url, headers=HEADERS, timeout=15)
                if r.status_code != 200:
                    continue

                soup = BeautifulSoup(r.text, "html.parser")

                # 複数のパース方法を試す
                data = self._parse_e_data(soup)
                if not data:
                    data = self._parse_datatable(soup)
                if not data:
                    data = self._parse_dt_dd(soup)

                company_name = data.get("社名", "")
                phone = data.get("電話番号", "") or data.get("電話", "")
                capital = data.get("資本金", "")
                representative = data.get("代表者", "")
                website = data.get("URL", "") or data.get("ホームページ", "")
                address = data.get("会社所在地", "") or data.get("住所", "")

                if not any([company_name, phone, website]):
                    continue

                result = {
                    "id": str(i),
                    "url": url,
                    "company_name": company_name,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "capital": capital,
                    "representative": representative,
                    "region": region_name,
                }

                self.result_count += 1
                if self.result_callback:
                    self.result_callback(result)

                print(f"[Ietatta] ✓ {company_name[:25] if company_name else 'ID:' + str(i)}")
                time.sleep(1)

            except Exception as e:
                continue

    def _parse_e_data(self, soup) -> dict:
        """div.e_data形式のパース"""
        data = {}
        for block in soup.select("div.e_data"):
            ps = block.find_all("p")
            if len(ps) >= 2:
                key = ps[0].get_text(strip=True)
                value = ps[1].get_text(strip=True)
                data[key] = value
        return data

    def _parse_datatable(self, soup) -> dict:
        """datatable_L形式のパース"""
        data = {}
        data_section = soup.find("div", class_="datatable_L")
        if data_section:
            for dl in data_section.find_all("dl"):
                dt = dl.find("dt")
                dd = dl.find("dd")
                if dt and dd:
                    key = dt.get_text(strip=True)
                    value = dd.get_text(" ", strip=True)
                    if key == "URL":
                        a_tag = dd.find("a")
                        if a_tag and a_tag.get("href"):
                            value = a_tag["href"]
                    data[key] = value
        return data

    def _parse_dt_dd(self, soup) -> dict:
        """dt/dd形式のパース"""
        data = {}

        def get_dd(label):
            dt = soup.find("dt", string=lambda t: t and t.strip() == label)
            if dt:
                dd = dt.find_next_sibling("dd")
                return dd.get_text(strip=True) if dd else ""
            return ""

        data["社名"] = get_dd("社名")
        data["電話番号"] = get_dd("電話")
        data["資本金"] = get_dd("資本金")
        data["代表者"] = get_dd("代表者")

        dt_site = soup.find("dt", string=lambda t: t and t.strip() == "公式サイト")
        if dt_site:
            dd = dt_site.find_next_sibling("dd")
            if dd:
                a = dd.find("a")
                if a and a.get("href"):
                    data["URL"] = a["href"].strip()

        return data
