"""
ガーデンクラブ スクレイパー (SaaS Worker版)
ガーデン・エクステリア業者情報を収集
"""
import re
import time
import urllib.parse
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup


# 都道府県リスト
GARDEN_CLUB_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

BASE_URL = "https://rgc.takasho.jp/db/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class GardenClubScraper:
    """ガーデンクラブからエクステリア業者情報を収集"""

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

    def run(self, prefectures: list, filters: dict = None) -> int:
        """スクレイピングを実行"""
        filters = filters or {}
        self.result_count = 0

        total_prefs = len(prefectures)
        current_pref = 0

        for prefecture in prefectures:
            if not self.is_running_check():
                print("[GardenClub] 停止リクエスト受信")
                break

            current_pref += 1
            print(f"[GardenClub] [{current_pref}/{total_prefs}] {prefecture} スキャン開始...")

            self._scrape_prefecture(prefecture)

        return self.result_count

    def _scrape_prefecture(self, prefecture: str):
        """特定の都道府県の業者を取得"""
        page_num = 1
        seen_detail_urls = set()

        encoded = urllib.parse.quote(prefecture)
        current_url = f"{BASE_URL}list.php?key={encoded}"

        while current_url:
            if not self.is_running_check():
                break

            try:
                r = requests.get(current_url, headers=HEADERS, timeout=30)
                r.encoding = 'utf-8'
                soup = BeautifulSoup(r.text, 'html.parser')

                shop_links = self._get_shop_links(soup)
                if not shop_links:
                    if page_num == 1:
                        print(f"[GardenClub] [{prefecture}] 店舗なし")
                    break

                # 重複ページチェック
                current_detail_urls = set(url for _, url in shop_links)
                if current_detail_urls == seen_detail_urls:
                    break
                seen_detail_urls = current_detail_urls

                print(f"[GardenClub] ページ{page_num}: {len(shop_links)}件")

                for company_name, detail_url in shop_links:
                    if not self.is_running_check():
                        break

                    try:
                        dr = requests.get(detail_url, headers=HEADERS, timeout=30)
                        dr.encoding = 'utf-8'
                        dsoup = BeautifulSoup(dr.text, 'html.parser')

                        data = self._parse_detail(dsoup, detail_url, prefecture)
                        if not data.get('company_name'):
                            data['company_name'] = company_name

                        self.result_count += 1
                        if self.result_callback:
                            self.result_callback(data)
                        if self.progress_callback:
                            self.progress_callback(self.result_count, 0)

                        print(f"[GardenClub] {data.get('company_name', 'N/A')[:30]}")
                        time.sleep(0.3)

                    except Exception as e:
                        print(f"[GardenClub] 詳細取得エラー: {e}")
                        continue

                # 次のページURL取得
                next_url = self._get_next_page_url(soup)
                if not next_url:
                    break

                current_url = next_url
                page_num += 1
                time.sleep(0.5)

            except Exception as e:
                print(f"[GardenClub] ページ取得エラー: {e}")
                break

        print(f"[GardenClub] [{prefecture}] 完了")

    def _get_next_page_url(self, soup) -> Optional[str]:
        """次のページURLを取得"""
        for a in soup.find_all('a', href=True):
            text = a.get_text(strip=True)
            if '次の' in text or text == '次へ':
                href = a['href']
                if href.startswith('?'):
                    return BASE_URL + 'list.php' + href
                elif href.startswith('./'):
                    return BASE_URL + href[2:]
                elif not href.startswith('http'):
                    return BASE_URL + href
                return href
        return None

    def _get_shop_links(self, soup) -> list:
        """店舗リンクを抽出"""
        shops = []
        seen = set()

        list_table = soup.find('table', id='list') or soup.find('table', class_='body')
        if not list_table:
            return shops

        for a in list_table.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True)
            if href.startswith('./') and href.endswith('.html') and text:
                if re.match(r'^\d+\.html$', href[2:]):
                    full_url = BASE_URL + href[2:]
                    if full_url not in seen:
                        seen.add(full_url)
                        shops.append((text, full_url))
        return shops

    def _parse_detail(self, soup, url: str, prefecture: str) -> dict:
        """詳細ページを解析"""
        data = {
            'prefecture': prefecture,
            'company_name': '',
            'type': '',
            'zip': '',
            'address': '',
            'free_dial': '',
            'phone': '',
            'fax': '',
            'hours': '',
            'holiday': '',
            'website': '',
            'founded': '',
            'ceo': '',
            'specialty': '',
            'url': url,
        }

        sidenav = soup.find('div', id='sidenav') or soup

        type_elem = sidenav.find('p', class_='type')
        if type_elem:
            data['type'] = type_elem.get_text(strip=True)

        for table in sidenav.find_all('table'):
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True)
                    value = td.get_text(strip=True)
                    if '会社名' in key:
                        data['company_name'] = value
                    elif '設立' in key:
                        data['founded'] = value
                    elif '代表者' in key:
                        data['ceo'] = value

        for box in sidenav.find_all('div', class_='box'):
            li_list = box.find_all('li')
            for i, li in enumerate(li_list):
                text = li.get_text(strip=True)

                # 郵便番号と住所
                if text.startswith('〒'):
                    match = re.search(r'〒([\d-]+)', text)
                    if match:
                        data['zip'] = match.group(1)
                    addr_in_line = re.search(r'〒[\d-]+\s*(.+?)(?:[\[［]|$)', text)
                    if addr_in_line and len(addr_in_line.group(1).strip()) > 3:
                        addr = re.sub(r'[\[［]MAP[\]］]$', '', addr_in_line.group(1)).strip()
                        data['address'] = addr
                    elif i + 1 < len(li_list):
                        next_text = li_list[i + 1].get_text(strip=True)
                        if not any(next_text.startswith(p) for p in ['TEL', 'FAX', '営業', '定休', '〒', 'ホーム']):
                            addr = re.sub(r'[\[［]MAP[\]］]$', '', next_text).strip()
                            if addr:
                                data['address'] = addr

                elif text.startswith('TEL') or 'TEL：' in text:
                    match = re.search(r'[\d-]{9,}', text)
                    if match:
                        data['phone'] = match.group()

                elif text.startswith('FAX') or 'FAX：' in text:
                    match = re.search(r'[\d-]{9,}', text)
                    if match:
                        data['fax'] = match.group()

                elif text.startswith('営業時間'):
                    data['hours'] = text.replace('営業時間：', '').replace('営業時間', '').strip()

                elif text.startswith('定休日'):
                    data['holiday'] = text.replace('定休日：', '').replace('定休日', '').strip()

                a_tag = li.find('a', href=True)
                if a_tag and 'ホームページ' in text:
                    data['website'] = a_tag['href']

        return data
