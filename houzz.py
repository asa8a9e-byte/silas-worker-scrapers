"""
Houzz スクレイパー (SaaS Worker版)
建築・リフォーム業者情報を収集
"""
import time
from typing import Callable, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# 職種リスト（日本語名 -> URL用スラッグ）
HOUZZ_PROFESSIONS = {
    "総合建設": "general-contractor",
    "造園・外構": "landscape-contractors",
    "外装工事": "exterior-construction",
    "リフォーム全般": "home-remodeling",
    "リフォーム専門": "remodeling-specialists",
}

# 都道府県リスト
HOUZZ_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


class HouzzScraper:
    """Houzzから建築・リフォーム業者情報を収集"""

    def __init__(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        result_callback: Optional[Callable[[dict], None]] = None,
        is_running_check: Optional[Callable[[], bool]] = None,
    ):
        self.progress_callback = progress_callback
        self.result_callback = result_callback
        self.is_running_check = is_running_check or (lambda: True)

        self.driver = None
        self.result_count = 0

    def run(self, professions: list, prefectures: list, filters: dict = None) -> int:
        """スクレイピングを実行"""
        filters = filters or {}
        self.result_count = 0

        try:
            self._init_browser()

            total_combinations = len(professions) * len(prefectures)
            current_combo = 0

            for profession_slug in professions:
                if not self.is_running_check():
                    print("[Houzz] 停止リクエスト受信")
                    break

                # 職種の日本語名を取得
                profession_name = next(
                    (k for k, v in HOUZZ_PROFESSIONS.items() if v == profession_slug),
                    profession_slug
                )

                for location in prefectures:
                    if not self.is_running_check():
                        break

                    current_combo += 1
                    print(f"[Houzz] [{current_combo}/{total_combinations}] 職種: {profession_name} / 地域: {location}")

                    self._scrape_location(profession_slug, profession_name, location, filters)

            return self.result_count

        finally:
            self._close_browser()

    def _init_browser(self, headless: bool = True):
        """ブラウザを初期化"""
        print("[Houzz] ブラウザを起動中...")

        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        # メモリ節約
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        # クラッシュ防止
        options.add_argument("--disable-features=VizDisplayCompositor")

        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        print("[Houzz] ブラウザ起動完了" + (" (ヘッドレス)" if headless else ""))

    def _close_browser(self):
        """ブラウザを終了"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            print("[Houzz] ブラウザを終了しました")

    def _scrape_location(self, profession_slug: str, profession_name: str, location: str, filters: dict):
        """特定の職種・地域の業者を取得"""
        base_url = f"https://www.houzz.jp/professionals/{profession_slug}/c/{location}"
        max_pages = filters.get("max_pages", 25)

        for page in range(1, max_pages + 1):
            if not self.is_running_check():
                break

            offset = (page - 1) * 15
            url = base_url if page == 1 else f"{base_url}/p/{offset}"
            print(f"[Houzz] ページ {page} を取得中...")

            try:
                self.driver.get(url)
                time.sleep(3)
            except Exception as e:
                print(f"[Houzz] ページ取得エラー: {e}")
                break

            # 100kmの距離を選択（最初の1ページ目のみ）
            if page == 1:
                self._set_distance_100km()

            # 店舗リンクの取得
            store_links = self._get_store_links()
            if not store_links:
                print("[Houzz] 店舗データが見つかりませんでした")
                break

            print(f"[Houzz] {len(store_links)} 件の店舗を検出")

            # 各店舗の詳細データを取得
            for store_url in store_links:
                if not self.is_running_check():
                    break

                try:
                    data = self._extract_store_detail(store_url, profession_name, location)
                    if data:
                        self.result_count += 1
                        if self.result_callback:
                            self.result_callback(data)
                        if self.progress_callback:
                            self.progress_callback(self.result_count, 0)
                        print(f"[Houzz] ✓ {data.get('company_name', 'N/A')}")
                except Exception as e:
                    print(f"[Houzz] 店舗詳細取得エラー: {e}")
                    continue

    def _set_distance_100km(self):
        """距離を100kmに変更"""
        try:
            print("[Houzz] 距離を100kmに変更中...")
            dropdown = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "hui-select-menu-2"))
            )
            dropdown.click()
            time.sleep(1)

            option_100km = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "hui-menu-1-item-4"))
            )
            option_100km.click()
            time.sleep(3)
            print("[Houzz] 距離を100kmに変更しました")
        except Exception as e:
            print(f"[Houzz] 範囲の選択に失敗: {e}")

    def _get_store_links(self) -> list:
        """店舗リンクを取得"""
        try:
            store_elements = self.driver.find_elements(By.XPATH, "//span[@itemprop='name']/ancestor::a")
            return [elem.get_attribute("href") for elem in store_elements if elem.get_attribute("href")]
        except:
            return []

    def _extract_store_detail(self, store_url: str, profession_name: str, location: str) -> Optional[dict]:
        """店舗詳細ページから情報を抽出"""
        try:
            self.driver.get(store_url)
            time.sleep(2)
        except:
            return None

        # 会社名
        try:
            company_name = self.driver.find_element(By.TAG_NAME, "h1").text
        except:
            company_name = ""

        if not company_name:
            return None

        # 電話番号
        try:
            phone = self.driver.find_element(By.XPATH, "//h3[contains(text(), '電話番号')]/following-sibling::p").text
        except:
            phone = ""

        # 住所
        try:
            address = self.driver.execute_script("""
                let el = document.querySelector("div[class*='dbBdzY'] p");
                return el ? el.innerText.replace(/\\n/g, " ") : "";
            """)
        except:
            address = ""

        # ホームページ
        try:
            website = self.driver.find_element(By.XPATH, "//span[contains(@class, 'Website__EllipsisText')]").text
        except:
            website = ""

        return {
            "profession": profession_name,
            "company_name": company_name,
            "phone": phone,
            "website": website,
            "address": address,
            "url": store_url,
            "prefecture": location,
        }
