#!/usr/bin/env python3
"""
SSO 认证脚本 — 通用 SSO 鉴权

功能:
  1. 自动创建应用 (appId)
  2. 通过 API 获取 SSO access_token
  3. 捕获认证过程中服务端返回的 **所有 Cookie**，完整缓存
  4. 供 data_map_api.py AuthProvider 读取，作为最高优先级鉴权方式

使用方式:
  python3 sso_auth.py                    # 自动认证
  python3 sso_auth.py --check            # 检查缓存
  python3 sso_auth.py --clear-cache      # 清除缓存

缓存位置:
  ~/.sso/.redInfo    — 认证信息（appId、token、userInfo、cookies）
  ~/.sso/.env        — 环境变量文件（source 后可用）
"""

import os
import sys
import json
import argparse
import platform
import time
import webbrowser
from pathlib import Path
from typing import Optional, Dict, Any

try:
    import requests
except ImportError:
    print("❌ requests 未安装")
    print("请运行: pip3 install requests 或 pip install requests")
    sys.exit(1)


# ────────────────────────────────────────────────────
# 配置
# ────────────────────────────────────────────────────

class Config:
    """SSO 认证配置"""
    CACHE_DIR = Path.home() / ".sso"
    AUTH_FILE = CACHE_DIR / ".redInfo"
    ENV_FILE = CACHE_DIR / ".env"

    # API 配置
    API_BASE_URL = "https://fe-data.devops.xiaohongshu.com"
    CREATE_APP_URL = f"{API_BASE_URL}/api/open/app"
    GET_TOKEN_URL = f"{API_BASE_URL}/api/open/app/token"
    LOGIN_PAGE_URL = f"{API_BASE_URL}/login"

    APP_DESC = "crux2-data-map"

    # Token 本地缓存不设过期，让服务端自然过期
    # 若接口返回 401，重新运行 python3 sso_auth.py 即可刷新
    TOKEN_EXPIRY_OFFSET =  365 * 24 * 60 * 60 * 1000  # 1年，由SSO决定过期时间

    # 轮询等待配置
    POLL_INTERVAL = 3       # 轮询间隔（秒）
    POLL_TIMEOUT = 300      # 轮询超时（秒），5 分钟


# ────────────────────────────────────────────────────
# API 客户端（使用 Session 捕获所有 Cookie）
# ────────────────────────────────────────────────────

class APIClient:
    """SSO 平台 API 客户端，使用 requests.Session 收集所有 Cookie"""

    def __init__(self):
        self.session = requests.Session()

    def create_app(self, app_desc: str = Config.APP_DESC) -> Optional[str]:
        """创建应用，返回 appId"""
        try:
            print("📝 正在创建应用...")
            response = self.session.post(
                Config.CREATE_APP_URL,
                json={"appDesc": app_desc, "callback_urls": []},
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data", {}).get("appId"):
                    app_id = data["data"]["appId"]
                    print(f"✅ 应用创建成功: {app_id}")
                    return app_id

            print(f"❌ 创建应用失败: {response.text}")
            return None
        except Exception as e:
            print(f"❌ 创建应用出错: {e}")
            return None

    def get_token(self, app_id: str, quiet: bool = False) -> Optional[Dict[str, Any]]:
        """通过 appId 获取 token

        Args:
            app_id: 应用 ID
            quiet: 静默模式，不打印中间日志（用于轮询场景）
        """
        try:
            if not quiet:
                print(f"🔐 正在获取 token (appId: {app_id})...")
            response = self.session.get(
                f"{Config.GET_TOKEN_URL}?appId={app_id}",
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data", {}).get("accessToken"):
                    if not quiet:
                        print("✅ 成功获取 token")
                    return data
                elif data.get("code") == 404:
                    if not quiet:
                        print("⚠️  应用不存在或未登录 (404)")
                    return None

            if not quiet:
                print(f"❌ 获取 token 失败: {response.text}")
            return None
        except Exception as e:
            if not quiet:
                print(f"❌ 获取 token 出错: {e}")
            return None

    def get_all_cookies(self) -> Dict[str, str]:
        """获取 Session 中累积的所有 Cookie（name → value）"""
        return {c.name: c.value for c in self.session.cookies}


# ────────────────────────────────────────────────────
# 认证管理
# ────────────────────────────────────────────────────

class AuthManager:
    """Token 缓存与生命周期管理"""

    @staticmethod
    def ensure_cache_dir():
        Config.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def read_auth_file() -> Optional[Dict[str, Any]]:
        if not Config.AUTH_FILE.exists():
            return None
        try:
            with open(Config.AUTH_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return None

    @staticmethod
    def save_auth_state(state: Dict[str, Any]):
        AuthManager.ensure_cache_dir()
        with open(Config.AUTH_FILE, "w") as f:
            json.dump(state, f, indent=2)

    @staticmethod
    def is_token_valid(auth_data: Dict[str, Any]) -> bool:
        token = auth_data.get("token")
        exp = auth_data.get("exp", 0)
        if not token or exp <= 0:
            return False
        now = int(time.time() * 1000)
        return now < exp

    @staticmethod
    def get_app_id() -> Optional[str]:
        auth_data = AuthManager.read_auth_file()
        if auth_data:
            return auth_data.get("appId")
        return None

    @staticmethod
    def save_token_info(app_id: str, token_response: Dict[str, Any],
                        session_cookies: Optional[Dict[str, str]] = None):
        """保存 token、userInfo、以及 Session 中收集到的所有 Cookie"""
        token = token_response.get("data", {}).get("accessToken")
        user_info = token_response.get("data", {}).get("userInfo")
        exp = int(time.time() * 1000) + Config.TOKEN_EXPIRY_OFFSET

        state = {
            "appId": app_id,
            "token": token,
            "userInfo": user_info,
            "exp": exp,
            "cookies": session_cookies or {},
        }
        AuthManager.save_auth_state(state)
        return token, user_info

    @staticmethod
    def generate_env_file(token: str, user_info: Optional[Dict[str, Any]] = None):
        AuthManager.ensure_cache_dir()
        is_windows = platform.system() == "Windows"

        # 构建环境变量键值对
        env_vars = {"SSO_ACCESS_TOKEN": token}
        if user_info and isinstance(user_info, dict):
            if "email" in user_info:
                env_vars["SSO_USER_EMAIL"] = user_info["email"]
            if "userId" in user_info:
                env_vars["SSO_USER_ID"] = user_info["userId"]
            if "name" in user_info:
                env_vars["SSO_USER_NAME"] = user_info["name"]

        if is_windows:
            # Windows: 生成 PowerShell 脚本 (.ps1) 和 CMD 批处理 (.bat)
            ps1_file = Config.ENV_FILE.with_suffix(".ps1")
            bat_file = Config.ENV_FILE.with_suffix(".bat")

            ps1_lines = [f'$env:{k} = "{v}"' for k, v in env_vars.items()]
            with open(ps1_file, "w", encoding="utf-8") as f:
                f.write("\n".join(ps1_lines) + "\n")

            bat_lines = [f'set "{k}={v}"' for k, v in env_vars.items()]
            with open(bat_file, "w", encoding="utf-8") as f:
                f.write("\n".join(bat_lines) + "\n")

            print(f"\n✅ 环境变量文件已保存:")
            print(f"   PowerShell: {ps1_file}")
            print(f"   CMD:        {bat_file}")
            print(f"📝 设置环境变量:")
            print(f"   PowerShell:  . {ps1_file}")
            print(f"   CMD:         {bat_file}")
        else:
            # Unix/macOS: 生成 shell export 文件
            env_lines = [f"export {k}='{v}'" for k, v in env_vars.items()]
            with open(Config.ENV_FILE, "w") as f:
                f.write("\n".join(env_lines) + "\n")
            print(f"\n✅ 环境变量文件已保存到: {Config.ENV_FILE}")
            print(f"📝 设置环境变量:")
            print(f"   source {Config.ENV_FILE}")


# ────────────────────────────────────────────────────
# 供外部模块调用的便捷函数
# ────────────────────────────────────────────────────

def get_cached_token() -> Optional[str]:
    """读取缓存中有效的 SSO Token，无效则返回 None。"""
    env_token = os.environ.get("SSO_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token

    auth_data = AuthManager.read_auth_file()
    if auth_data and AuthManager.is_token_valid(auth_data):
        return auth_data.get("token")

    return None


def get_cached_auth_info() -> Optional[Dict[str, Any]]:
    """读取缓存中完整的认证信息。

    返回:
      {
        "token": str,
        "userId": str,
        "email": str,
        "name": str,
        "cookies": {"cookie_name": "cookie_value", ...}  # SSO 认证过程中收集到的所有 Cookie
      }
    """
    token = None
    user_id = None
    email = None
    name = None
    cookies = {}

    # 优先读环境变量
    env_token = os.environ.get("SSO_ACCESS_TOKEN", "").strip()
    if env_token:
        token = env_token
        user_id = os.environ.get("SSO_USER_ID", "").strip() or None
        email = os.environ.get("SSO_USER_EMAIL", "").strip() or None
        name = os.environ.get("SSO_USER_NAME", "").strip() or None
    else:
        # 其次读缓存文件
        auth_data = AuthManager.read_auth_file()
        if auth_data and AuthManager.is_token_valid(auth_data):
            token = auth_data.get("token")
            user_info = auth_data.get("userInfo") or {}
            cookies = auth_data.get("cookies") or {}
            if isinstance(user_info, dict):
                user_id = user_info.get("userId")
                email = user_info.get("email")
                name = user_info.get("name")

    if not token:
        return None

    return {
        "token": token,
        "userId": user_id,
        "email": email,
        "name": name,
        "cookies": cookies,
    }


# ────────────────────────────────────────────────────
# 交互式认证流程
# ────────────────────────────────────────────────────

class SSO:
    """SSO 认证入口"""

    @staticmethod
    def authenticate() -> bool:
        print("\n🔐 开始 SSO 认证流程...\n")

        # 路径 1: 检查缓存的 token 是否有效
        auth_data = AuthManager.read_auth_file()
        if auth_data and AuthManager.is_token_valid(auth_data):
            print("✅ 使用缓存的有效 token")
            token = auth_data.get("token")
            user_info = auth_data.get("userInfo")
            cookies = auth_data.get("cookies") or {}

            print(f"\n📋 认证信息:")
            print(f"   Access Token: {token[:30]}...")
            if user_info and isinstance(user_info, dict):
                if "email" in user_info:
                    print(f"   Email: {user_info['email']}")
                if "name" in user_info:
                    print(f"   Name: {user_info['name']}")
            print(f"   已缓存 Cookie: {len(cookies)} 项")

            AuthManager.generate_env_file(token, user_info)
            return True

        # 路径 2: 获取或创建 appId
        app_id = AuthManager.get_app_id()
        client = APIClient()

        if not app_id:
            print("📝 首次认证，需要创建应用...")
            app_id = client.create_app()
            if not app_id:
                print("❌ 创建应用失败")
                return False
            AuthManager.save_auth_state({"appId": app_id})

        # 路径 3: 通过 API 获取 token
        print(f"\n🔄 尝试通过 API 获取 token...")
        token_response = client.get_token(app_id)

        if token_response:
            # 收集 Session 中的所有 Cookie
            session_cookies = client.get_all_cookies()
            token, user_info = AuthManager.save_token_info(
                app_id, token_response, session_cookies
            )

            print(f"\n✅ SSO 认证成功！")
            print(f"\n📋 认证信息:")
            print(f"   Access Token: {token[:30]}...")
            if user_info and isinstance(user_info, dict):
                if "email" in user_info:
                    print(f"   Email: {user_info['email']}")
                if "name" in user_info:
                    print(f"   Name: {user_info['name']}")
            print(f"   收集到 Cookie: {len(session_cookies)} 项")
            if session_cookies:
                for name in sorted(session_cookies.keys()):
                    val = session_cookies[name]
                    val_display = f"{val[:40]}..." if len(val) > 40 else val
                    print(f"     - {name}={val_display}")

            AuthManager.generate_env_file(token, user_info)
            return True

        # 路径 4: API 获取失败，打开浏览器并轮询等待登录完成
        login_url = f"{Config.LOGIN_PAGE_URL}?appId={app_id}"
        print(f"\n⚠️  需要浏览器登录")
        print(f"\n🌐 请在浏览器中完成 SSO 登录:")
        print(f"   {login_url}")

        try:
            print(f"\n⏳ 正在打开浏览器...")
            webbrowser.open(login_url)
        except Exception:
            print(f"   ⚠️  无法自动打开浏览器，请手动复制上方 URL 访问")

        # 轮询等待用户在浏览器中完成登录
        print(f"\n⏳ 等待登录完成（每 {Config.POLL_INTERVAL} 秒检测一次，"
              f"超时 {Config.POLL_TIMEOUT} 秒）...")
        print(f"   按 Ctrl+C 可中断等待\n")

        start_time = time.time()
        attempt = 0
        while True:
            elapsed = time.time() - start_time
            if elapsed >= Config.POLL_TIMEOUT:
                print(f"\n❌ 等待超时（{Config.POLL_TIMEOUT} 秒），请重新运行脚本")
                return False

            time.sleep(Config.POLL_INTERVAL)
            attempt += 1

            # 每次轮询使用新的 session，避免旧 session 缓存影响
            poll_client = APIClient()
            token_response = poll_client.get_token(app_id, quiet=True)

            if token_response:
                session_cookies = poll_client.get_all_cookies()
                token, user_info = AuthManager.save_token_info(
                    app_id, token_response, session_cookies
                )

                print(f"\n✅ SSO 认证成功！（第 {attempt} 次轮询，"
                      f"耗时 {int(elapsed)} 秒）")
                print(f"\n📋 认证信息:")
                print(f"   Access Token: {token[:30]}...")
                if user_info and isinstance(user_info, dict):
                    if "email" in user_info:
                        print(f"   Email: {user_info['email']}")
                    if "name" in user_info:
                        print(f"   Name: {user_info['name']}")
                print(f"   收集到 Cookie: {len(session_cookies)} 项")
                if session_cookies:
                    for name in sorted(session_cookies.keys()):
                        val = session_cookies[name]
                        val_display = f"{val[:40]}..." if len(val) > 40 else val
                        print(f"     - {name}={val_display}")

                AuthManager.generate_env_file(token, user_info)
                return True

            # 打印等待进度（静默模式，不打印 get_token 的失败信息）
            remaining = int(Config.POLL_TIMEOUT - elapsed)
            print(f"   ⏳ 第 {attempt} 次检测...未完成登录（剩余 {remaining} 秒）")

    @staticmethod
    def check() -> bool:
        print("\n📋 检查认证状态")
        print("-" * 50)

        auth_data = AuthManager.read_auth_file()
        if not auth_data:
            print("❌ 未找到缓存的认证信息")
            print("   请运行: python3 sso_auth.py")
            return False

        if AuthManager.is_token_valid(auth_data):
            print("✅ Token 有效")
            token = auth_data.get("token", "")
            print(f"\n📋 认证信息:")
            print(f"   App ID: {auth_data.get('appId', 'N/A')}")
            print(f"   Access Token: {token[:30]}...")

            user_info = auth_data.get("userInfo", {})
            if isinstance(user_info, dict):
                if "email" in user_info:
                    print(f"   Email: {user_info['email']}")
                if "name" in user_info:
                    print(f"   Name: {user_info['name']}")

            cookies = auth_data.get("cookies") or {}
            print(f"   已缓存 Cookie: {len(cookies)} 项")
            if cookies:
                for name in sorted(cookies.keys()):
                    val = cookies[name]
                    val_display = f"{val[:40]}..." if len(val) > 40 else val
                    print(f"     - {name}={val_display}")

            return True
        else:
            print("❌ Token 已过期或无效")
            print("   请运行: python3 sso_auth.py")
            return False

    @staticmethod
    def clear_cache():
        if Config.AUTH_FILE.exists():
            Config.AUTH_FILE.unlink()
            print(f"✅ 已删除: {Config.AUTH_FILE}")
        # 清理所有平台的环境变量文件
        for ext in ("", ".ps1", ".bat"):
            f = Config.ENV_FILE.with_suffix(ext) if ext else Config.ENV_FILE
            if f.exists():
                f.unlink()
                print(f"✅ 已删除: {f}")


# ────────────────────────────────────────────────────
# CLI 入口
# ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SSO 认证脚本 — 通用 SSO 鉴权",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用方式:
  python3 sso_auth.py                    # 自动认证
  python3 sso_auth.py --check            # 检查缓存
  python3 sso_auth.py --clear-cache      # 清除缓存

工作流程:
  1. python3 sso_auth.py                 # 自动认证
  2. 如果需要，在浏览器中完成登录
  3. source ~/.sso/.env                  # 加载环境变量（可选）

接口返回 401 时:
  重新运行 python3 sso_auth.py 即可刷新 Token

缓存位置:
  认证信息: ~/.sso/.redInfo
  环境变量: ~/.sso/.env
        """,
    )
    parser.add_argument("--check", action="store_true", help="检查缓存的认证信息")
    parser.add_argument("--clear-cache", action="store_true", help="清除本地缓存")

    args = parser.parse_args()

    try:
        if args.clear_cache:
            SSO.clear_cache()
            return 0
        if args.check:
            return 0 if SSO.check() else 1
        return 0 if SSO.authenticate() else 1
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())