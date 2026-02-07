"""
MEO診断チェッカー (SaaS Worker版)
meo-tools.comから診断データを取得
ローカル版と同等の機能を提供
"""
import time
import json
from typing import Callable, Optional, List, Dict, Any

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class MeoCheckerScraper:
    """MEO診断チェッカー"""

    # 危険ワード（削除等の操作を避ける）
    DANGER_WORDS = ["削除", "delete", "remove", "取消"]

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
        self.wait = None
        self.result_count = 0

    def run(self, accounts: List[Dict[str, str]], run_diagnosis: bool = True) -> int:
        """
        複数アカウントの診断データを取得

        Args:
            accounts: [{"email": "...", "password": "..."}, ...]
            run_diagnosis: 診断を実行するかどうか（デフォルト: True）

        Returns:
            取得した店舗数
        """
        self.result_count = 0
        self.run_diagnosis_flag = run_diagnosis

        try:
            self._init_browser()

            total_accounts = len(accounts)
            for idx, account in enumerate(accounts):
                if not self.is_running_check():
                    print("[MEO] 停止リクエスト受信")
                    break

                print(f"[MEO] アカウント処理中: {idx + 1}/{total_accounts}")

                try:
                    self._process_account(account, idx, total_accounts)
                except Exception as e:
                    print(f"[MEO] アカウント処理エラー: {e}")

            return self.result_count

        finally:
            self._close_browser()

    def _init_browser(self):
        """ブラウザを初期化"""
        print("[MEO] ブラウザを起動中...")

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)
        self.driver.set_window_position(-2000, 0)
        print("[MEO] ブラウザ起動完了")

    def _close_browser(self):
        """ブラウザを終了"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            self.wait = None

    def _process_account(self, account: Dict[str, str], idx: int, total: int):
        """1アカウントを処理"""
        email = account.get("email", "")
        password = account.get("password", "")
        target_company = account.get("targetCompany", "")  # フィルタ用

        if not email or not password:
            print("[MEO] メールまたはパスワードが空です")
            return

        # ログイン
        print(f"[MEO] ログイン中: {email}")
        self.driver.get("https://meo-tools.com/agencies/sign_in")
        time.sleep(1)

        try:
            email_input = self.driver.find_element(By.CSS_SELECTOR, "input[type='email']")
            email_input.clear()
            email_input.send_keys(email)

            password_input = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
            password_input.clear()
            password_input.send_keys(password)

            submit_btn = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
            submit_btn.click()
            time.sleep(2)

            # ログイン成功確認
            if "/agencies/sign_in" in self.driver.current_url:
                print(f"[MEO] ログイン失敗: {email}")
                return

            print(f"[MEO] ログイン成功: {email}")

        except Exception as e:
            print(f"[MEO] ログインエラー: {e}")
            return

        # 店舗一覧を取得して処理
        page = 1
        while self.is_running_check():
            print(f"[MEO] ページ {page} を処理中...")
            self.driver.get(f"https://meo-tools.com/agencies/accounts?page={page}")
            time.sleep(1)

            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                print("[MEO] 店舗がありません")
                break

            shops = []
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        shop_id = cells[0].text.strip()
                        shop_name = cells[3].text.strip()
                        company = cells[4].text.strip()

                        # ターゲット会社でフィルタ（指定がある場合）
                        if target_company and company != target_company:
                            continue

                        shops.append({"id": shop_id, "name": shop_name, "company": company})
                except:
                    pass

            print(f"[MEO] このページの対象店舗数: {len(shops)}")

            for shop in shops:
                if not self.is_running_check():
                    break

                try:
                    self._process_shop(shop, page)
                except Exception as e:
                    print(f"[MEO] 店舗処理エラー: {e}")

            # 次ページチェック
            next_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.paginator-page")
            has_next = False
            for btn in next_buttons:
                if btn.text.isdigit() and int(btn.text) == page + 1:
                    has_next = True
                    break

            if has_next:
                page += 1
            else:
                break

        # ログアウト
        try:
            self.driver.get("https://meo-tools.com/agencies/sign_out")
            time.sleep(1)
        except:
            pass

        if self.progress_callback:
            self.progress_callback(idx + 1, total)

    def _process_shop(self, shop: Dict[str, str], page: int):
        """1店舗のデータを取得"""
        print(f"[MEO] 店舗処理: {shop['name'][:30]}")

        # 店舗一覧ページに戻る
        self.driver.get(f"https://meo-tools.com/agencies/accounts?page={page}")
        time.sleep(1)

        # 対象店舗の行を再取得
        rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        target_row = None
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 5 and cells[0].text.strip() == shop["id"]:
                    target_row = row
                    break
            except:
                pass

        if not target_row:
            print(f"[MEO] 店舗が見つかりません: {shop['name']}")
            return

        # アクションボタンをクリック
        try:
            action_btn = target_row.find_element(By.CSS_SELECTOR, "button[id^='radix-']")
            action_btn.click()
            time.sleep(0.3)
        except:
            print("[MEO] アクションボタンが見つかりません")
            return

        # ログインメニューを探す
        menu_items = self.driver.find_elements(By.CSS_SELECTOR, "[role='menuitem']")
        login_item = None

        for item in menu_items:
            item_text = item.text.strip().lower()
            if any(d in item_text for d in self.DANGER_WORDS):
                continue
            if item.text.strip() == "ログイン":
                login_item = item
                break

        if not login_item:
            print("[MEO] ログインメニューが見つかりません")
            return

        # 新しいタブでダッシュボードを開く
        original_window = self.driver.current_window_handle
        original_windows = self.driver.window_handles
        login_item.click()
        time.sleep(1.5)

        new_windows = self.driver.window_handles
        if len(new_windows) > len(original_windows):
            for handle in new_windows:
                if handle != original_window:
                    self.driver.switch_to.window(handle)
                    break

        time.sleep(1.5)

        if "/users" not in self.driver.current_url:
            print("[MEO] ダッシュボードではありません")
            if len(self.driver.window_handles) > 1:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            return

        # ダッシュボードURL保存
        dashboard_url = self.driver.current_url.split("?")[0]
        if not dashboard_url.endswith("/users"):
            dashboard_url = dashboard_url.split("/users")[0] + "/users"

        # 診断実行（フラグがオンの場合）
        if self.run_diagnosis_flag:
            print("[MEO] --- 診断レポート生成 ---")
            self._run_diagnosis(dashboard_url)

        # データ取得
        print("[MEO] --- データ取得 ---")

        # ダッシュボードに戻る
        if "/users" not in self.driver.current_url or "/reports" in self.driver.current_url or "/keywords" in self.driver.current_url:
            self.driver.get(dashboard_url)
            time.sleep(3)

        dashboard_data = self._get_dashboard_data()
        insights = self._get_insight_data()
        review_stats = self._get_review_stats()
        keywords = self._get_keyword_rankings()

        # 結果を整形
        result = self._format_result(shop, dashboard_data, insights, review_stats, keywords)

        if self.result_callback:
            self.result_callback(result)

        self.result_count += 1
        print(f"[MEO] 完了: スコア={result.get('totalScore', '-')}")

        # タブを閉じて戻る
        if len(self.driver.window_handles) > 1:
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])

    def _run_diagnosis(self, dashboard_url: str) -> bool:
        """診断レポートを生成して完了を待つ"""
        try:
            print("[MEO] 診断結果ページへ移動...")
            reports_link = self.driver.find_element(By.CSS_SELECTOR, "a[href='/users/reports']")
            reports_link.click()
            time.sleep(1.5)

            print("[MEO] 新しい診断を実行...")
            run_btn = self.driver.find_element(By.XPATH, "//button[contains(., '新しい診断を実行')]")
            run_btn.click()

            print("[MEO] 診断完了を待機中...（最大90秒）")
            popup_wait = WebDriverWait(self.driver, 90)

            try:
                alert = popup_wait.until(EC.alert_is_present())
                alert_text = alert.text
                print(f"[MEO] ✓ 診断完了: {alert_text}")
                alert.accept()
            except:
                try:
                    ok_btn = popup_wait.until(EC.element_to_be_clickable((
                        By.XPATH, "//button[text()='OK' or text()='ok' or text()='Ok']"
                    )))
                    print("[MEO] ✓ 診断完了!")
                    ok_btn.click()
                except:
                    print("[MEO] 診断完了待ちタイムアウト")

            time.sleep(1)

            print("[MEO] ダッシュボードに戻る...")
            self.driver.get(dashboard_url)
            time.sleep(3)

            return True

        except Exception as e:
            print(f"[MEO] ❌ 診断実行失敗: {e}")
            self.driver.get(dashboard_url)
            time.sleep(3)
            return False

    def _get_dashboard_data(self) -> Dict[str, Any]:
        """ダッシュボードからJSONデータを取得"""
        try:
            dashboard_wait = WebDriverWait(self.driver, 10)
            dashboard_el = dashboard_wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-react-component='Dashboard']"))
            )
            props_json = dashboard_el.get_attribute("data-react-props")
            return json.loads(props_json)
        except Exception as e:
            print(f"[MEO] ダッシュボードデータ取得失敗: {e}")
            return {}

    def _get_insight_data(self) -> Dict[str, str]:
        """HTMLからインサイト数値を取得"""
        insights = {}
        try:
            insight_cards = self.driver.find_elements(By.CSS_SELECTOR, ".grid .bg-white.border.rounded-lg.p-4")
            for card in insight_cards:
                try:
                    label = card.find_element(By.CSS_SELECTOR, "h3").text.strip()
                    value = card.find_element(By.CSS_SELECTOR, "p.text-2xl").text.strip()
                    insights[label] = value
                except:
                    pass
        except:
            pass

        print(f"[MEO] インサイト: {insights}")
        return insights

    def _get_review_stats(self) -> Dict[str, str]:
        """口コミ統計を取得"""
        stats = {}
        time.sleep(1)

        # h3タイトルから親カードを辿る
        try:
            all_h3 = self.driver.find_elements(By.CSS_SELECTOR, "h3.tracking-tight")
            for h3 in all_h3:
                title = h3.text.strip()
                if title in ["口コミ合計", "返信済", "未返信"]:
                    try:
                        card = h3.find_element(By.XPATH, "./ancestor::div[contains(@class, 'rounded-lg')]")
                        value_el = card.find_element(By.CSS_SELECTOR, ".text-2xl.font-bold")
                        value = value_el.text.strip().replace("件", "")
                        stats[title] = value
                    except:
                        pass
        except:
            pass

        # 口コミ増加数の推移
        try:
            increase_cards = self.driver.find_elements(By.CSS_SELECTOR, ".grid.grid-cols-3.gap-4 > div.bg-white.border.rounded-lg.p-4")
            for card in increase_cards:
                try:
                    label_el = card.find_element(By.CSS_SELECTOR, "h3.text-sm.font-medium.text-gray-600")
                    label = label_el.text.strip()
                    value_el = card.find_element(By.CSS_SELECTOR, "p.text-2xl.font-bold")
                    value = value_el.text.strip()

                    if "1ヶ月" in label:
                        stats["1ヶ月増加"] = value
                    elif "6ヶ月" in label:
                        stats["6ヶ月増加"] = value
                    elif "12ヶ月" in label:
                        stats["12ヶ月増加"] = value
                except:
                    pass
        except:
            pass

        print(f"[MEO] 口コミ: {stats}")
        return stats

    def _get_keyword_rankings(self) -> List[Dict[str, str]]:
        """キーワードランキングを取得（ONのもののみ、最大5つ）"""
        keywords = []
        seen_keywords = set()

        try:
            keyword_link = self.driver.find_element(
                By.XPATH, "//span[contains(text(), 'キーワードランキング')]/ancestor::a"
            )
            keyword_link.click()
            time.sleep(1.5)

            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

            for row in rows:
                if len(keywords) >= 5:
                    break

                try:
                    switches = row.find_elements(
                        By.CSS_SELECTOR, "button[role='switch'][data-state='checked']"
                    )
                    if not switches:
                        continue

                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 2:
                        continue

                    keyword_divs = cells[0].find_elements(By.CSS_SELECTOR, "div > div:first-child")
                    if not keyword_divs:
                        continue
                    keyword = keyword_divs[0].text.strip()

                    if keyword in seen_keywords:
                        continue
                    seen_keywords.add(keyword)

                    rank_spans = cells[1].find_elements(By.CSS_SELECTOR, "div > span:first-child")
                    if rank_spans:
                        rank = rank_spans[0].text.strip()
                    else:
                        rank = cells[1].text.strip().split()[0] if cells[1].text.strip() else ""

                    keywords.append({"keyword": keyword, "rank": rank})

                except:
                    continue

            print(f"[MEO] キーワード: {len(keywords)}件")

        except Exception as e:
            print(f"[MEO] キーワード取得失敗: {e}")

        return keywords

    def _format_result(
        self,
        shop: Dict[str, str],
        dashboard_data: Dict[str, Any],
        insights: Dict[str, str],
        review_stats: Dict[str, str],
        keywords: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """結果を整形（ローカル版と同等のデータ）"""
        account = dashboard_data.get("account", {})
        report = dashboard_data.get("latestReport", {})
        category_scores = report.get("categoryScores", {})
        report_items = {item["name"]: item["result"] for item in report.get("reportItems", [])}

        return {
            # 店舗情報
            "storeName": shop.get("name", account.get("storeName", "")),
            "companyName": shop.get("company", account.get("agencyName", "")),
            "userName": account.get("userName", ""),
            "userEmail": account.get("userEmail", ""),
            "searchOriginAddress": account.get("searchOriginAddress", ""),
            "keywordSearchRadius": account.get("keywordSearchRadius", ""),

            # 診断情報
            "reportDate": report.get("reportedDate", ""),
            "status": report.get("status", ""),
            "totalScore": report.get("totalScore", 0),
            "basicInfo": category_scores.get("basicInfo", 0),
            "posts": category_scores.get("posts", 0),
            "photos": category_scores.get("photos", 0),
            "reviews": category_scores.get("reviews", 0),

            # 診断項目詳細
            "reportItems": {
                "ビジネス名": "○" if report_items.get("ビジネス名") else "×",
                "メインカテゴリ": "○" if report_items.get("メインカテゴリ") else "×",
                "ビジネスの説明": "○" if report_items.get("ビジネスの説明") else "×",
                "開業日": "○" if report_items.get("開業日") else "×",
                "住所": "○" if report_items.get("住所") else "×",
                "営業時間設定": "○" if report_items.get("営業時間設定") else "×",
                "営業時間正確性": "○" if report_items.get("営業時間正確性") else "×",
                "メニュー、サービス": "○" if report_items.get("メニュー、サービス") else "×",
                "店舗HP URL": "○" if report_items.get("店舗HP URL") else "×",
                "電話番号": "○" if report_items.get("電話番号") else "×",
                "投稿頻度": "○" if report_items.get("投稿頻度") else "×",
                "写真投稿数": "○" if report_items.get("写真投稿数") else "×",
                "写真の投稿頻度": "○" if report_items.get("写真の投稿頻度") else "×",
                "ロゴ&カバー写真": "○" if report_items.get("ロゴ&カバー写真") else "×",
                "平均評価": "○" if report_items.get("平均評価") else "×",
                "クチコミ投稿件数": "○" if report_items.get("クチコミ投稿件数") else "×",
                "クチコミ返信率": "○" if report_items.get("クチコミ返信率") else "×",
            },

            # インサイト
            "insights": {
                "表示回数": insights.get("表示回数", ""),
                "モバイル": insights.get("モバイル", ""),
                "PC": insights.get("PC", ""),
                "平均クリック率": insights.get("平均クリック率", ""),
                "電話クリック数": insights.get("電話クリック数", ""),
                "ルート検索回数": insights.get("ルート検索回数", ""),
                "ウェブサイトクリック数": insights.get("ウェブサイトクリック数", ""),
                "メニュークリック数": insights.get("メニュークリック数", ""),
            },

            # 口コミ統計
            "reviewTotal": review_stats.get("口コミ合計", ""),
            "reviewReplied": review_stats.get("返信済", ""),
            "reviewUnreplied": review_stats.get("未返信", ""),
            "review1MonthIncrease": review_stats.get("1ヶ月増加", ""),
            "review6MonthIncrease": review_stats.get("6ヶ月増加", ""),
            "review12MonthIncrease": review_stats.get("12ヶ月増加", ""),

            # キーワード
            "keywords": keywords,

            # その他
            "aiProvider": account.get("aiProvider", ""),
            "aiTemperature": account.get("aiTemperature", ""),
            "googlePermissionDenied": "○" if account.get("googlePermissionDenied") else "×",
            "instagramAutoPostEnabled": "○" if account.get("instagramAutoPostEnabled") else "×",
            "createdAt": account.get("createdAt", ""),
            "updatedAt": account.get("updatedAt", ""),
        }
