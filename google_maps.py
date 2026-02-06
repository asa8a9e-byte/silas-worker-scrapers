"""
Google Maps スクレイパー (SaaS Worker版)
店舗情報を収集する独立版スクレイパー - 詳細版（写真・口コミ・最新投稿対応）
"""
import re
import time
from datetime import datetime
from typing import Callable, Optional
from urllib.parse import urlparse, parse_qs, urlsplit

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


# 設定
MAX_REVIEWS = 5
MAX_UPDATES = 5
MAX_PHOTO_THUMBNAILS = 20
MAX_PHOTOS = 10
MAX_STORES = 100

# ドメイン抽出用正規表現
DOMAIN_RE = re.compile(r'([a-zA-Z0-9][-a-zA-Z0-9\.]*\.[a-zA-Z]{2,24})(?:[\/\?#][^\s]*)?')


class GoogleMapsScraper:
    """Google Mapsから店舗情報を収集（詳細版）"""

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
        self.actions = None
        self.result_count = 0

    def run(self, keywords: list, filters: dict = None) -> int:
        """スクレイピングを実行"""
        filters = filters or {}
        self.result_count = 0

        try:
            self._init_browser()

            total_keywords = len(keywords)
            for idx, keyword in enumerate(keywords):
                if not self.is_running_check():
                    print("[Scraper] 停止リクエスト受信")
                    break

                print(f"[Scraper] 検索中: {keyword} ({idx + 1}/{total_keywords})")
                self._search_keyword(keyword, filters)

            return self.result_count

        finally:
            self._close_browser()

    def _init_browser(self):
        """ブラウザを初期化"""
        print("[Scraper] ブラウザを起動中...")

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-features=RendererCodeIntegrity,AutoExpandDetailsElement")
        # ヘッドレスモード（サーバー環境向け）
        # options.add_argument("--headless=new")

        self.driver = webdriver.Chrome(options=options)
        self.actions = ActionChains(self.driver)
        # ウィンドウを画面外に移動（見えなくする）
        self.driver.set_window_position(-2000, 0)
        print("[Scraper] ブラウザ起動完了")

    def _close_browser(self):
        """ブラウザを終了"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            self.actions = None

    def _search_keyword(self, keyword: str, filters: dict):
        """キーワードで検索して結果を収集"""
        search_url = f"https://www.google.com/maps/search/{keyword.replace(' ', '+')}/"

        try:
            self.driver.get(search_url)
            time.sleep(3)
        except Exception as e:
            print(f"[Scraper] 検索エラー: {e}")
            return

        # 結果一覧を収集
        results = self._collect_results()
        if not results:
            print(f"[Scraper] 結果なし: {keyword}")
            return

        print(f"[Scraper] {len(results)}件の店舗を発見")

        # 各店舗の詳細を取得
        seen = set()
        total = len(results)
        print(f"[Scraper] 詳細取得開始: {total}件")
        for i, item in enumerate(results):
            if not self.is_running_check():
                break

            href = item["href"]
            name = item["name"]

            print(f"[Scraper] 詳細取得中: {i + 1}/{total} - {name[:30]}")

            if self.progress_callback:
                self.progress_callback(i + 1, total)

            try:
                self.driver.get(href)
                time.sleep(1.5)
                self._close_extra_tabs()
            except:
                continue

            data = self._extract_detail(keyword)
            if not data:
                continue

            # 重複チェック
            key = f"{data['title']}__{data['address']}"
            if key in seen:
                continue
            seen.add(key)

            # フィルター適用
            if not self._apply_filters(data, filters):
                print(f"[Scraper] フィルター除外: {data['title']}")
                continue

            # 結果を送信
            self.result_count += 1
            if self.result_callback:
                self.result_callback(data)

    def _collect_results(self) -> list:
        """検索結果一覧を収集"""
        try:
            scroll_area = self._wait_for_scroll_area()
        except TimeoutException:
            return []

        # スクロールして全件取得
        prev_count = -1
        stable = 0

        while stable < 5:
            if not self.is_running_check():
                break

            self.driver.execute_script("arguments[0].scrollBy(0, 1000);", scroll_area)
            time.sleep(2)

            links = self._get_result_links()
            count = len(links)

            if count == prev_count:
                stable += 1
            else:
                stable = 0
                print(f"[Scraper] リスト取得中... {count}件")
            prev_count = count

        # ユニークな結果を収集
        seen_href = set()
        results = []

        for a in self._get_result_links():
            href = a.get_attribute("href") or ""
            name = (a.get_attribute("aria-label") or "").strip()

            if not href or href in seen_href:
                continue
            seen_href.add(href)
            results.append({"href": href, "name": name})

        return results[:MAX_STORES]

    def _wait_for_scroll_area(self, timeout: int = 20):
        """スクロール領域を待機"""
        for _ in range(timeout * 4):
            try:
                return self.driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
            except:
                time.sleep(0.25)
        raise TimeoutException("スクロール領域が見つかりません")

    def _get_result_links(self) -> list:
        """結果リンクを取得（スポンサー除外）"""
        links = self.driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
        filtered = []

        for a in links:
            try:
                badge = a.find_element(By.XPATH, './/span[contains(@class,"jHLihd")]')
                if badge and "スポンサー" in badge.text:
                    continue
            except:
                pass
            filtered.append(a)

        return filtered

    def _close_extra_tabs(self):
        """余分なタブを閉じる"""
        try:
            if len(self.driver.window_handles) > 1:
                main = self.driver.window_handles[0]
                for h in self.driver.window_handles[1:]:
                    self.driver.switch_to.window(h)
                    self.driver.close()
                self.driver.switch_to.window(main)
        except:
            pass

    def _extract_detail(self, keyword: str) -> Optional[dict]:
        """詳細ページから情報を抽出（写真・口コミ・最新投稿含む）"""
        # 店名
        title = ""
        for _ in range(60):
            if self._is_stopped():
                return None
            try:
                t = self.driver.find_element(By.CSS_SELECTOR, "h1.DUwDvf").text.strip()
                if t:
                    title = t
                    break
            except:
                pass
            time.sleep(0.3)

        if not title or self._is_stopped():
            return None

        print(f"[Scraper] 店舗: {title}")

        # 口コミ数
        review_count = self._extract_review_count()

        # 評価
        rating = self._extract_rating(review_count)

        # 業種
        category = ""
        try:
            category = self.driver.find_element(By.CSS_SELECTOR, "button.DkEaL").text.strip()
        except:
            pass

        # 住所・電話番号・HP
        address, phone, website = self._extract_contact_info()

        if self._is_stopped():
            return None

        # 写真情報を取得
        print(f"[Scraper] 写真情報取得中...")
        photos = self._get_photos(title)

        if self._is_stopped():
            return None

        # 口コミを取得
        print(f"[Scraper] 口コミ取得中...")
        reviews, owner_reply_count, total_review_checked, reply_ratio = self._get_reviews()

        if self._is_stopped():
            return None

        # 最新投稿を取得
        print(f"[Scraper] 最新投稿取得中...")
        latest_date, latest_content = self._get_latest_updates()

        return {
            "keyword": keyword,
            "title": title,
            "rating": rating,
            "review_count": review_count,
            "phone": phone,
            "address": address,
            "website": website,
            "category": category,
            "latest_date": latest_date,
            "latest_content": latest_content,
            "reviews": reviews,
            "photos": photos,
            "owner_reply_count": owner_reply_count,
            "total_review_checked": total_review_checked,
            "reply_ratio": reply_ratio,
        }

    def _is_stopped(self) -> bool:
        """停止フラグをチェック"""
        return not self.is_running_check()

    def _get_reviews(self) -> tuple:
        """クチコミを取得"""
        reviews = []
        owner_reply_count = 0
        total_review_count = 0

        if self._is_stopped():
            return reviews, 0, 0, "0%"

        try:
            tabs = self.driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]')
            review_tab_found = False
            for tab in tabs:
                if "クチコミ" in tab.text:
                    tab.click()
                    time.sleep(2)
                    self._close_extra_tabs()
                    review_tab_found = True
                    break

            if not review_tab_found or self._is_stopped():
                return reviews, 0, 0, "0%"

            scroll_area = None
            try:
                scroll_area = self.driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde')
            except NoSuchElementException:
                try:
                    scroll_area = self.driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb')
                except NoSuchElementException:
                    pass

            if scroll_area:
                for _ in range(3):
                    self.driver.execute_script("arguments[0].scrollBy(0, 500);", scroll_area)
                    time.sleep(0.5)

            # 「もっと見る」ボタンをクリック
            more_btns = self.driver.find_elements(By.CSS_SELECTOR, 'button.w8nwRe.kyuRq')
            for btn in more_btns[:5]:
                try:
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.3)
                except WebDriverException:
                    pass

            review_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf.fontBodyMedium')
            total_review_count = min(len(review_elements), MAX_REVIEWS)

            for review_el in review_elements[:MAX_REVIEWS]:
                try:
                    author = ""
                    author_el = review_el.find_elements(By.CSS_SELECTOR, 'div.d4r55')
                    if author_el:
                        author = author_el[0].text.strip()

                    stars = ""
                    star_el = review_el.find_elements(By.CSS_SELECTOR, 'span.kvMYJc')
                    if star_el:
                        aria = star_el[0].get_attribute('aria-label') or ""
                        m = re.search(r'(\d+)', aria)
                        if m:
                            stars = m.group(1)

                    date = ""
                    date_el = review_el.find_elements(By.CSS_SELECTOR, 'span.rsqaWe')
                    if date_el:
                        date = date_el[0].text.strip()

                    text = ""
                    text_el = review_el.find_elements(By.CSS_SELECTOR, 'span.wiI7pd')
                    if text_el:
                        text = text_el[0].text.strip()[:200]

                    has_owner_reply = False
                    try:
                        owner_reply_el = review_el.find_elements(By.CSS_SELECTOR, 'span.fontTitleSmall')
                        for el in owner_reply_el:
                            if "オーナーからの返信" in el.text:
                                has_owner_reply = True
                                break
                    except:
                        pass

                    if not has_owner_reply:
                        try:
                            owner_reply_div = review_el.find_elements(By.CSS_SELECTOR, 'div.CDe7pd')
                            if owner_reply_div:
                                has_owner_reply = True
                        except:
                            pass

                    if has_owner_reply:
                        owner_reply_count += 1

                    if text:
                        review_str = f"[{author}] ★{stars} ({date}): {text}"
                        reviews.append(review_str)
                except Exception as e:
                    continue

            # 概要タブに戻る
            tabs = self.driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]')
            for tab in tabs:
                if "概要" in tab.text:
                    tab.click()
                    time.sleep(1)
                    break

        except Exception as e:
            print(f"[Scraper] 口コミ取得エラー: {e}")

        if total_review_count > 0:
            reply_ratio = f"{owner_reply_count}/{total_review_count}"
        else:
            reply_ratio = "0/0"

        return reviews, owner_reply_count, total_review_count, reply_ratio

    def _get_newest_photo_date(self, dates: list) -> str:
        """日付リストから最新の日付を取得"""
        if not dates:
            return ""

        def date_to_num(date_str):
            m = re.search(r'(\d{4})年(\d{1,2})月', date_str)
            if m:
                year, month = int(m.group(1)), int(m.group(2))
                return year * 12 + month
            return 0

        sorted_dates = sorted(dates, key=date_to_num, reverse=True)
        return sorted_dates[0] if sorted_dates else ""

    def _get_photos(self, store_name: str) -> dict:
        """写真情報を取得（オーナー/ユーザー判定、最新投稿日）"""
        result = {
            "owner_count": 0,
            "user_count": 0,
            "owner_ratio": "0%",
            "latest_date": ""
        }

        if self._is_stopped():
            return result

        try:
            # 写真ビューを開く方法を複数試す
            photo_opened = False

            # 方法1: 「写真を表示」div
            try:
                divs = self.driver.find_elements(By.CSS_SELECTOR, 'div.YkuOqf')
                for div in divs:
                    if "写真" in div.text:
                        self.driver.execute_script("arguments[0].click();", div)
                        time.sleep(2)
                        photo_opened = True
                        break
            except:
                pass

            # 方法2: XPATHで「写真を表示」
            if not photo_opened:
                try:
                    photo_btn = self.driver.find_element(By.XPATH, '//div[contains(text(),"写真を表示")]')
                    self.driver.execute_script("arguments[0].click();", photo_btn)
                    time.sleep(2)
                    photo_opened = True
                except:
                    pass

            # 方法3: 写真サムネイル画像をクリック
            if not photo_opened:
                try:
                    photo_thumb = self.driver.find_element(By.CSS_SELECTOR, 'button[aria-label*="写真"]')
                    self.driver.execute_script("arguments[0].click();", photo_thumb)
                    time.sleep(2)
                    photo_opened = True
                except:
                    pass

            # 方法4: 写真タブをクリック
            if not photo_opened:
                try:
                    tabs = self.driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]')
                    for tab in tabs:
                        if "写真" in tab.text:
                            tab.click()
                            time.sleep(2)
                            photo_opened = True
                            break
                except:
                    pass

            # 方法5: 写真のサムネイル領域をクリック
            if not photo_opened:
                try:
                    photo_area = self.driver.find_element(By.CSS_SELECTOR, 'div.RZ66Rb.FgCUCc')
                    self.driver.execute_script("arguments[0].click();", photo_area)
                    time.sleep(2)
                    photo_opened = True
                except:
                    pass

            if not photo_opened:
                return result

            self._close_extra_tabs()
            time.sleep(1)

            # スクロールして全写真を読み込む
            prev_count = 0
            stable_count = 0

            for scroll_attempt in range(15):
                if self._is_stopped():
                    return result

                photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.Uf0tqf.ch8jbf')
                if not photo_items:
                    photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'button.U39Pmb')
                if not photo_items:
                    photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.U39Pmb')

                current_count = len(photo_items)

                if current_count == prev_count:
                    stable_count += 1
                    if stable_count >= 3:
                        break
                else:
                    stable_count = 0

                prev_count = current_count

                # スクロール
                scroll_success = False
                try:
                    grid = self.driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 600;", grid)
                    scroll_success = True
                except:
                    pass

                if not scroll_success:
                    try:
                        grid = self.driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb')
                        self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 600;", grid)
                        scroll_success = True
                    except:
                        pass

                if not scroll_success and photo_items:
                    try:
                        last_item = photo_items[-1]
                        self.driver.execute_script("arguments[0].scrollIntoView(false);", last_item)
                    except:
                        pass

                time.sleep(0.6)

            # 最終的なサムネイル数を取得
            photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.Uf0tqf.ch8jbf')
            if not photo_items:
                photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'button.U39Pmb')
            if not photo_items:
                photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.U39Pmb')
            if not photo_items:
                photo_items = self.driver.find_elements(By.CSS_SELECTOR, 'a[data-photo-index]')

            actual_photo_count = min(len(photo_items), MAX_PHOTO_THUMBNAILS)

            if actual_photo_count == 0:
                return result

            photos_to_check = min(actual_photo_count, MAX_PHOTOS)

            # 最初の写真をクリック
            try:
                self.driver.execute_script("arguments[0].click();", photo_items[0])
                time.sleep(1.5)
                self._close_extra_tabs()
            except:
                return result

            # オーナー名の候補を作成
            owner_names = [
                store_name.lower(),
                store_name.replace(' ', '').lower(),
                store_name.replace('　', '').lower(),
            ]
            clean_name = re.sub(r'(株式会社|有限会社|㈱|㈲|合同会社|LLC)', '', store_name).strip()
            if clean_name:
                owner_names.append(clean_name.lower())

            all_dates = []
            owner_count = 0
            user_count = 0

            for i in range(photos_to_check):
                if self._is_stopped():
                    break

                time.sleep(0.5)

                # 投稿者を取得
                author = ""
                try:
                    author_el = self.driver.find_element(By.CSS_SELECTOR, 'span.OVC7id')
                    author = author_el.text.strip()
                except:
                    pass

                # 投稿日を取得
                date = ""
                try:
                    date_el = self.driver.find_element(By.CSS_SELECTOR, 'div.W0fu2b')
                    date_text = date_el.text.strip()
                    m = re.search(r'(\d{4}年\d{1,2}月)', date_text)
                    if m:
                        date = m.group(1)
                except:
                    pass

                if not author and not date:
                    break

                if date:
                    all_dates.append(date)

                # オーナー判定
                is_owner = False
                if author:
                    author_lower = author.lower().replace(' ', '').replace('　', '')
                    if "ストリートビュー" in author or "street view" in author_lower or author_lower == "google":
                        is_owner = False
                    else:
                        for owner_name in owner_names:
                            if owner_name in author_lower or author_lower in owner_name:
                                is_owner = True
                                break

                if is_owner:
                    owner_count += 1
                else:
                    user_count += 1

                # 次の写真へ移動
                if i < photos_to_check - 1:
                    moved = False

                    try:
                        next_btn = self.driver.find_element(By.CSS_SELECTOR, 'button[aria-label="次の写真を表示"]')
                        self.driver.execute_script("arguments[0].click();", next_btn)
                        moved = True
                        time.sleep(0.8)
                        self._close_extra_tabs()
                    except:
                        pass

                    if not moved:
                        try:
                            next_btn = self.driver.find_element(By.CSS_SELECTOR, 'button.Yv1slc.goTRNd')
                            self.driver.execute_script("arguments[0].click();", next_btn)
                            moved = True
                            time.sleep(0.8)
                            self._close_extra_tabs()
                        except:
                            pass

                    if not moved:
                        try:
                            next_btn = self.driver.find_element(By.XPATH, '//button[contains(@aria-label,"次")]')
                            self.driver.execute_script("arguments[0].click();", next_btn)
                            moved = True
                            time.sleep(0.8)
                            self._close_extra_tabs()
                        except:
                            pass

                    if not moved:
                        try:
                            img = self.driver.find_element(By.CSS_SELECTOR, 'img.bDPjIe, img.U4UiSd')
                            img.click()
                            time.sleep(0.2)
                            self.actions.send_keys('\ue014').perform()
                            moved = True
                            time.sleep(0.8)
                            self._close_extra_tabs()
                        except:
                            pass

                    if not moved:
                        break

            result["owner_count"] = owner_count
            result["user_count"] = user_count

            total_checked = owner_count + user_count
            if total_checked > 0:
                ratio = (owner_count / total_checked) * 100
                result["owner_ratio"] = f"{ratio:.0f}%"

            if all_dates:
                result["latest_date"] = self._get_newest_photo_date(all_dates)

            # 写真ビューを閉じる
            for _ in range(3):
                try:
                    self.actions.send_keys('\ue00c').perform()
                    time.sleep(0.3)
                except:
                    pass
            time.sleep(0.5)

            # 概要タブに戻る
            try:
                tabs = self.driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]')
                for tab in tabs:
                    if "概要" in tab.text:
                        tab.click()
                        time.sleep(1)
                        break
            except:
                pass

        except Exception as e:
            print(f"[Scraper] 写真取得エラー: {e}")

        return result

    def _get_latest_updates(self) -> tuple:
        """最新投稿を取得"""
        updates = []
        latest_date = ""

        if self._is_stopped():
            return latest_date, ""

        try:
            post_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.SBD2Rc.waIsr')

            for idx in range(min(len(post_buttons), MAX_UPDATES)):
                if self._is_stopped():
                    break

                try:
                    current_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.SBD2Rc.waIsr')
                    if idx >= len(current_buttons):
                        break

                    btn = current_buttons[idx]
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(1)
                    self._close_extra_tabs()

                    content = ""
                    try:
                        content_el = self.driver.find_element(By.CSS_SELECTOR, 'div.hfJtQe.fontBodyMedium')
                        content = content_el.text.strip()[:200]
                    except:
                        try:
                            content_el = self.driver.find_element(By.CSS_SELECTOR, 'div.hfJtQe')
                            content = content_el.text.strip()[:200]
                        except:
                            pass

                    date = ""
                    try:
                        date_el = self.driver.find_element(By.CSS_SELECTOR, 'div.mgX1W.fontBodySmall div')
                        date = date_el.text.strip()
                    except:
                        try:
                            date_el = self.driver.find_element(By.CSS_SELECTOR, 'div.mgX1W div')
                            date = date_el.text.strip()
                        except:
                            pass

                    if idx == 0 and date:
                        latest_date = date

                    if content:
                        updates.append(f"[{date}] {content}")

                    try:
                        self.actions.send_keys('\ue00c').perform()
                        time.sleep(0.5)
                    except:
                        pass

                except:
                    try:
                        self.actions.send_keys('\ue00c').perform()
                        time.sleep(0.3)
                    except:
                        pass
                    continue

        except Exception as e:
            print(f"[Scraper] 最新投稿取得エラー: {e}")

        return latest_date, "\n---\n".join(updates[:MAX_UPDATES])

    def _extract_review_count(self) -> str:
        """口コミ数を抽出"""
        try:
            el = self.driver.find_element(
                By.XPATH, '//div[contains(@class,"F7nice")]//span[@aria-label and contains(@aria-label,"件のクチコミ")]'
            )
            label = (el.get_attribute("aria-label") or el.text or "").strip()
            m = re.search(r"(\d+)\s*件のクチコミ", label)
            if m:
                return m.group(1)
        except:
            pass
        return ""

    def _extract_rating(self, review_count: str) -> str:
        """評価を抽出"""
        if not review_count or not review_count.isdigit() or int(review_count) == 0:
            return ""

        try:
            el = self.driver.find_element(By.CSS_SELECTOR, 'div.F7nice span[aria-hidden="true"]')
            txt = el.text.strip()
            if re.match(r"^\d+(\.\d+)?$", txt):
                return txt
        except:
            pass
        return ""

    def _extract_contact_info(self) -> tuple:
        """連絡先情報を抽出"""
        address = ""
        phone = ""
        website_candidates = []

        # ウェブサイトボタン
        try:
            a = self.driver.find_element(By.CSS_SELECTOR, 'a[aria-label*="ウェブサイト"]')
            href = a.get_attribute("href") or ""
            if href:
                website_candidates.append(href)
        except:
            pass

        # 基本情報テキスト
        info_elements = self.driver.find_elements(
            By.CSS_SELECTOR, 'div.Io6YTe.fontBodyMedium.kR99db.fdkmkc'
        )

        for el in info_elements:
            txt = (el.text or "").strip()

            # 住所
            if not address and re.match(r"^〒?\d{3}-\d{4}", txt):
                address = txt

            # 電話番号
            if not phone and re.match(r"^0\d{1,4}-\d{1,4}-\d{3,4}$", txt):
                phone = txt

            # ドメイン
            dom = self._extract_domain(txt)
            if dom:
                website_candidates.append(dom)

        website = self._pick_best_url(website_candidates)

        return address, phone, website

    def _extract_domain(self, text: str) -> str:
        """テキストからドメインを抽出"""
        if not text or "@" in text:
            return ""
        m = DOMAIN_RE.search(text.strip())
        return m.group(1) if m else ""

    def _pick_best_url(self, urls: list) -> str:
        """最適なURLを選択"""
        if not urls:
            return ""

        cleaned = []
        for u in urls:
            nu = self._normalize_url(u)
            if not nu or self._is_claim_link(nu):
                continue
            cleaned.append(nu)

        if not cleaned:
            return ""

        # 最短ドメイン優先
        cleaned.sort(key=lambda u: len(urlparse(u).netloc or u))
        return cleaned[0]

    def _normalize_url(self, s: str) -> str:
        """URLを正規化"""
        if not s:
            return ""
        s = s.strip()

        try:
            parts = urlsplit(s)
            if parts.netloc.endswith("google.com") and parts.path.startswith("/url"):
                q = parse_qs(parts.query).get("q", [""])[0]
                if q:
                    s = q
        except:
            pass

        if s.startswith("http://") or s.startswith("https://"):
            return s
        return "http://" + s

    def _is_claim_link(self, url: str) -> bool:
        """claim誘導リンクかどうか"""
        if not url:
            return False

        try:
            parsed = urlparse(url)
            if "business.google.com" in parsed.netloc and "/create" in parsed.path:
                return True
            if "gmbsrc=" in parsed.query or "ppsrc=GMBMI" in parsed.query:
                return True
        except:
            pass

        return False

    def _apply_filters(self, data: dict, filters: dict) -> bool:
        """フィルターを適用（reply, photo対応）"""
        # 口コミ返信フィルター
        reply_filter = filters.get("reply", 0)
        if reply_filter == 1:  # 返信あり
            if data.get("owner_reply_count", 0) == 0:
                return False
        elif reply_filter == 2:  # 返信なし
            if data.get("owner_reply_count", 0) > 0:
                return False

        # 写真更新フィルター
        photo_filter = filters.get("photo", 0)
        if photo_filter in [1, 2]:
            latest_date = data.get("photos", {}).get("latest_date", "")
            if latest_date:
                # "1年前"、"2か月前"、"3週間前" などのパターンをパース
                if "年前" in latest_date:
                    match = re.search(r'(\d+)\s*年前', latest_date)
                    if match:
                        years = int(match.group(1))
                        if photo_filter == 1 and years >= 1:  # 1年以内
                            return False
                        if photo_filter == 2 and years >= 2:  # 2年以内
                            return False
                # YYYY年MM月形式
                elif "年" in latest_date and "月" in latest_date:
                    m = re.search(r'(\d{4})年(\d{1,2})月', latest_date)
                    if m:
                        year, month = int(m.group(1)), int(m.group(2))
                        now = datetime.now()
                        photo_date = datetime(year, month, 1)
                        years_diff = (now - photo_date).days / 365
                        if photo_filter == 1 and years_diff >= 1:  # 1年以内
                            return False
                        if photo_filter == 2 and years_diff >= 2:  # 2年以内
                            return False

        # 旧フィルター互換（評価、口コミ数）
        min_rating = filters.get("min_rating")
        if min_rating:
            try:
                rating = float(data.get("rating") or 0)
                if rating < min_rating:
                    return False
            except:
                pass

        max_reviews = filters.get("max_reviews")
        if max_reviews:
            try:
                reviews = int(data.get("review_count") or 0)
                if reviews > max_reviews:
                    return False
            except:
                pass

        return True
