"""
SILAS SaaS スクレイピング実行エンジン
サーバーからの指示を受けてスクレイピングを実行
"""
import threading
import subprocess
import signal
import os
import sys
import importlib
from typing import Optional, Any
from pathlib import Path

from client import SaaSClient


def _get_update_dir() -> Path:
    """更新ファイルの保存ディレクトリを取得"""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "SILAS Worker" / "scrapers"
    elif sys.platform == "win32":
        return Path(os.environ.get("APPDATA", "")) / "SILAS Worker" / "scrapers"
    else:
        return Path.home() / ".silas-worker" / "scrapers"


def _import_scraper(module_name: str, class_name: str):
    """
    スクレイパーをインポート（更新版を優先）
    """
    update_dir = _get_update_dir()
    updated_file = update_dir / f"{module_name}.py"

    # 更新版が存在する場合はそちらを使用
    if updated_file.exists():
        # 既にインポートされている場合はリロード
        if module_name in sys.modules:
            del sys.modules[module_name]

        # 更新ディレクトリを一時的にパスの先頭に追加
        update_dir_str = str(update_dir)
        if update_dir_str not in sys.path:
            sys.path.insert(0, update_dir_str)

        try:
            module = importlib.import_module(module_name)
            print(f"[Executor] 更新版スクレイパーを使用: {updated_file}")
            return getattr(module, class_name)
        except Exception as e:
            print(f"[Executor] 更新版インポートエラー: {e}、バンドル版を使用")

    # バンドル版を使用
    module = importlib.import_module(f"scraper.{module_name}")
    print(f"[Executor] バンドル版スクレイパーを使用: scraper.{module_name}")
    return getattr(module, class_name)


class ScrapingExecutor:
    """SaaSからのスクレイピング指示を実行"""

    def __init__(self, client: SaaSClient):
        self.client = client
        self.client.on_request = self.handle_request
        self.current_task_id: Optional[str] = None
        self._is_running = False
        self._stop_flag = False
        self._current_scraper: Optional[Any] = None  # 現在実行中のスクレイパーインスタンス
        print(f"[Executor] 初期化完了: on_request設定済み")

    def handle_request(self, request: dict):
        """スクレイピングリクエストを処理"""
        task_id = request.get("task_id")
        scraper_type = request.get("scraper_type", "google_maps")
        keywords = request.get("keywords", [])
        filters = request.get("filters", {})

        print(f"[Executor] タスク受信: {task_id}")
        print(f"[Executor] タイプ: {scraper_type}")
        print(f"[Executor] キーワード: {keywords}")

        self.current_task_id = task_id
        self._is_running = True
        self._stop_flag = False

        # 別スレッドでスクレイピング実行
        thread = threading.Thread(
            target=self._execute_scraping,
            args=(task_id, scraper_type, keywords, filters),
            daemon=True,
        )
        thread.start()

    def stop(self):
        """実行中のスクレイピングを停止（ブラウザも強制終了）"""
        print(f"[Executor] 停止リクエスト受信")
        self._stop_flag = True
        self._is_running = False

        # 現在のスクレイパーのブラウザを強制終了
        if self._current_scraper:
            try:
                # Seleniumドライバーの場合
                if hasattr(self._current_scraper, 'driver') and self._current_scraper.driver:
                    print(f"[Executor] Seleniumブラウザを強制終了中...")
                    try:
                        self._current_scraper.driver.quit()
                    except:
                        pass
                    self._current_scraper.driver = None

                # Playwrightブラウザの場合
                if hasattr(self._current_scraper, 'browser') and self._current_scraper.browser:
                    print(f"[Executor] Playwrightブラウザを強制終了中...")
                    try:
                        self._current_scraper.browser.close()
                    except:
                        pass
                    self._current_scraper.browser = None

                # Playwrightコンテキストの場合
                if hasattr(self._current_scraper, 'context') and self._current_scraper.context:
                    try:
                        self._current_scraper.context.close()
                    except:
                        pass
                    self._current_scraper.context = None

                print(f"[Executor] ブラウザを終了しました")
            except Exception as e:
                print(f"[Executor] ブラウザ終了エラー: {e}")

        # 残っているChromeプロセスも強制終了（念のため）
        self._kill_browser_processes()

    def is_running(self) -> bool:
        """実行中かどうか"""
        return self._is_running and not self._stop_flag

    def _kill_browser_processes(self):
        """残っているブラウザプロセスを強制終了"""
        try:
            import platform
            if platform.system() == "Darwin":  # macOS
                # このスクリプトが起動したChrome/Chromiumプロセスのみ終了
                subprocess.run(["pkill", "-f", "chromium.*--headless"], capture_output=True)
                subprocess.run(["pkill", "-f", "chrome.*--headless"], capture_output=True)
            elif platform.system() == "Linux":
                subprocess.run(["pkill", "-f", "chromium.*--headless"], capture_output=True)
                subprocess.run(["pkill", "-f", "chrome.*--headless"], capture_output=True)
            print(f"[Executor] ヘッドレスブラウザプロセスをクリーンアップしました")
        except Exception as e:
            print(f"[Executor] プロセスクリーンアップエラー: {e}")

    def _execute_scraping(self, task_id: str, scraper_type: str, keywords: list, filters: dict):
        """スクレイピングを実行"""
        try:
            if scraper_type in ["google_maps", "gmaps", "gmaps_fast"]:
                self._run_google_maps(task_id, keywords, filters)
            elif scraper_type == "houzz":
                self._run_houzz(task_id, filters)
            elif scraper_type == "reshopnavi":
                self._run_reshopnavi(task_id, filters)
            elif scraper_type == "garden_club":
                self._run_garden_club(task_id, filters)
            elif scraper_type == "ieto":
                self._run_ieto(task_id, filters)
            elif scraper_type == "hagukumi":
                self._run_hagukumi(task_id, filters)
            elif scraper_type == "ietatta":
                self._run_ietatta(task_id, filters)
            elif scraper_type == "garden_plat":
                self._run_garden_plat(task_id, filters)
            elif scraper_type == "constmap":
                self._run_constmap(task_id, filters)
            elif scraper_type == "meo_checker":
                self._run_meo_checker(task_id, filters)
            else:
                self.client.send_error(task_id, f"未対応のスクレイパー: {scraper_type}")
        except Exception as e:
            print(f"[Executor] エラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._is_running = False

    def _run_google_maps(self, task_id: str, keywords: list, filters: dict):
        """Google Mapsスクレイパーを実行"""
        GoogleMapsScraper = _import_scraper("google_maps", "GoogleMapsScraper")

        if not keywords:
            self.client.send_error(task_id, "キーワードが指定されていません")
            return

        print(f"[Executor] Google Maps スクレイピング開始")
        print(f"[Executor] キーワード数: {len(keywords)}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = GoogleMapsScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper  # スクレイパーインスタンスを保持
            count = scraper.run(keywords, filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] スクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_houzz(self, task_id: str, filters: dict):
        """Houzzスクレイパーを実行"""
        HouzzScraper = _import_scraper("houzz", "HouzzScraper")
        try:
            HOUZZ_PROFESSIONS = _import_scraper("houzz", "HOUZZ_PROFESSIONS")
        except:
            from scraper.houzz import HOUZZ_PROFESSIONS

        professions = filters.get("professions", [])
        prefectures = filters.get("prefectures", [])

        if not professions:
            self.client.send_error(task_id, "職種が指定されていません")
            return

        if not prefectures:
            self.client.send_error(task_id, "都道府県が指定されていません")
            return

        print(f"[Executor] Houzz スクレイピング開始")
        print(f"[Executor] 職種数: {len(professions)}")
        print(f"[Executor] 都道府県数: {len(prefectures)}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = HouzzScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(professions, prefectures, filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] Houzzスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_reshopnavi(self, task_id: str, filters: dict):
        """リショップナビスクレイパーを実行（ID総当り方式）"""
        ReshopnaviScraper = _import_scraper("reshopnavi", "ReshopnaviScraper")

        start_id = filters.get("start_id", 1)
        end_id = filters.get("end_id", 9500)

        print(f"[Executor] リショップナビ スクレイピング開始")
        print(f"[Executor] ID範囲: {start_id} - {end_id}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = ReshopnaviScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] リショップナビスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_garden_club(self, task_id: str, filters: dict):
        """ガーデンクラブスクレイパーを実行"""
        GardenClubScraper = _import_scraper("garden_club", "GardenClubScraper")

        prefectures = filters.get("prefectures", [])

        if not prefectures:
            self.client.send_error(task_id, "都道府県が指定されていません")
            return

        print(f"[Executor] ガーデンクラブ スクレイピング開始")
        print(f"[Executor] 都道府県数: {len(prefectures)}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = GardenClubScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(prefectures, filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] ガーデンクラブスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_ieto(self, task_id: str, filters: dict):
        """イエトスクレイパーを実行（中国・四国地方のビルダー）"""
        IetoScraper = _import_scraper("ieto", "IetoScraper")
        try:
            IETO_AREAS = _import_scraper("ieto", "IETO_AREAS")
        except:
            from scraper.ieto import IETO_AREAS

        areas = filters.get("areas", list(IETO_AREAS.keys()))

        print(f"[Executor] イエト スクレイピング開始")
        print(f"[Executor] 対象エリア: {areas}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = IetoScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] イエトスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_hagukumi(self, task_id: str, filters: dict):
        """ハグクミスクレイパーを実行（ID総当り方式）"""
        HagukumiScraper = _import_scraper("hagukumi", "HagukumiScraper")

        start_id = filters.get("start_id", 1)
        end_id = filters.get("end_id", 7500)

        print(f"[Executor] ハグクミ スクレイピング開始")
        print(f"[Executor] ID範囲: {start_id} - {end_id}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = HagukumiScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] ハグクミスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_ietatta(self, task_id: str, filters: dict):
        """イエタッタスクレイパーを実行（全国12地域の住宅会社）"""
        IetattaScraper = _import_scraper("ietatta", "IetattaScraper")
        try:
            IETATTA_REGIONS = _import_scraper("ietatta", "IETATTA_REGIONS")
        except:
            from scraper.ietatta import IETATTA_REGIONS

        regions = filters.get("regions", list(IETATTA_REGIONS.keys()))

        print(f"[Executor] イエタッタ スクレイピング開始")
        print(f"[Executor] 対象地域: {regions}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = IetattaScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run({"regions": regions})
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] イエタッタスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_garden_plat(self, task_id: str, filters: dict):
        """ガーデンプラットスクレイパーを実行（ID総当り方式）"""
        GardenplatScraper = _import_scraper("garden_plat", "GardenplatScraper")

        start_id = filters.get("start_id", 1)
        end_id = filters.get("end_id", 1200)

        print(f"[Executor] ガーデンプラット スクレイピング開始")
        print(f"[Executor] ID範囲: {start_id} - {end_id}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = GardenplatScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(filters)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] ガーデンプラットスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_constmap(self, task_id: str, filters: dict):
        """コンストマップスクレイパーを実行（関西・九州の建設業者）"""
        ConstmapScraper = _import_scraper("constmap", "ConstmapScraper")
        try:
            CONSTMAP_REGIONS = _import_scraper("constmap", "CONSTMAP_REGIONS")
        except:
            from scraper.constmap import CONSTMAP_REGIONS

        regions = filters.get("regions", list(CONSTMAP_REGIONS.keys()))

        print(f"[Executor] コンストマップ スクレイピング開始")
        print(f"[Executor] 対象リージョン: {regions}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = ConstmapScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run({"regions": regions})
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] コンストマップスクレイピングエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None

    def _run_meo_checker(self, task_id: str, filters: dict):
        """MEO診断チェッカーを実行"""
        MeoCheckerScraper = _import_scraper("meo_checker", "MeoCheckerScraper")

        accounts = filters.get("accounts", [])

        if not accounts:
            self.client.send_error(task_id, "アカウントが指定されていません")
            return

        print(f"[Executor] MEO診断チェック開始")
        print(f"[Executor] アカウント数: {len(accounts)}")

        def on_progress(current, total):
            self.client.send_progress(task_id, current, total)

        def on_result(data):
            self.client.send_result(task_id, data)

        def is_running():
            return not self._stop_flag

        try:
            scraper = MeoCheckerScraper(
                progress_callback=on_progress,
                result_callback=on_result,
                is_running_check=is_running,
            )
            self._current_scraper = scraper
            count = scraper.run(accounts)
            if not self._stop_flag:
                self.client.send_completed(task_id)
                print(f"[Executor] 完了: {count}件取得")
            else:
                self.client.send_stopped(task_id, count)
                print(f"[Executor] 停止: {count}件取得済み")
        except Exception as e:
            print(f"[Executor] MEO診断チェックエラー: {e}")
            import traceback
            traceback.print_exc()
            self.client.send_error(task_id, str(e))
        finally:
            self._current_scraper = None
