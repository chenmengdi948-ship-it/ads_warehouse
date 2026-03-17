"""数据地图 API 层：HTTP 封装 + 鉴权 AuthProvider

鉴权优先级（从高到低）：
  1. SSO Token（推荐）：~/.sso/.redInfo 缓存或环境变量 SSO_ACCESS_TOKEN → 自动构造 crux2 全套 Cookie
  2. 环境变量 CRUX2_DATA_MAP_COOKIE
  3. assets/.cookie 文件 → Cookie: {content}
  4. 无鉴权（crux2 内网接口可直接访问）

遇到 401 且 Cookie 为空时抛出 NoCookieError，由调用方引导用户补充。
"""
import os
import requests
from urllib.parse import urlparse

BASE_URL = os.environ.get("DATA_MAP_BASE_URL", "https://crux2.devops.xiaohongshu.com/api")
_SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# assets/.cookie 鉴权文件
COOKIE_FILE = os.path.join(_SKILL_DIR, "assets", ".cookie")
# assets/ 目录（用于落盘预览数据等）
RESOURCES_DIR = os.path.join(_SKILL_DIR, "assets")

# 从 BASE_URL 提取域名，用于构造 cookie 名
_DOMAIN = urlparse(BASE_URL).hostname or "crux2.devops.xiaohongshu.com"
# 从域名提取服务标识（如 crux2.devops.xiaohongshu.com → crux2）
_SERVICE_ID = _DOMAIN.split(".")[0]


class NoCookieError(Exception):
    """Cookie 缺失或失效，需要用户重新提供"""


class AuthProvider:
    """鉴权优先级: SSO Token → 环境变量 Cookie → .cookie 文件 → 无鉴权"""

    def get_headers(self) -> dict:
        # 1. SSO Token（自动构造 Cookie）
        sso_cookies = self._build_sso_cookies()
        if sso_cookies:
            return {"Cookie": sso_cookies}

        # 2. 环境变量
        env_cookie = os.environ.get("CRUX2_DATA_MAP_COOKIE", "")
        if env_cookie:
            return {"Cookie": env_cookie}

        # 3. Cookie 文件
        cookie = self.read_cookie()
        if cookie:
            return {"Cookie": cookie}

        # 4. 无鉴权
        return {}

    @staticmethod
    def _build_sso_cookies() -> str:
        """从 SSO 缓存构造 Cookie: 原始 Session Cookie + 推导的目标服务 Cookie"""
        try:
            from sso_auth import get_cached_auth_info
            auth_info = get_cached_auth_info()
        except ImportError:
            token = os.environ.get("SSO_ACCESS_TOKEN", "").strip()
            if not token:
                return ""
            auth_info = {
                "token": token,
                "userId": os.environ.get("SSO_USER_ID", "").strip() or None,
                "cookies": {},
            }

        if not auth_info or not auth_info.get("token"):
            return ""

        token = auth_info["token"]
        user_id = auth_info.get("userId") or ""
        cookie_dict = dict(auth_info.get("cookies") or {})

        cookie_dict[f"access-token-{_DOMAIN}"] = f"internal.{_SERVICE_ID}.{token}"
        cookie_dict["common-internal-access-token-prod"] = token
        cookie_dict["common-internal-access-token-beta"] = token
        cookie_dict["common-internal-access-token-sit"] = token

        if user_id:
            cookie_dict[f"x-user-id-{_DOMAIN}"] = user_id
            cookie_dict["x-user-id"] = user_id
            cookie_dict["e-sso-user-id"] = user_id
            cookie_dict["e-sso-user-id-sit"] = user_id
            cookie_dict["x-user-id-sit"] = user_id

        return "; ".join(f"{k}={v}" for k, v in cookie_dict.items())

    def read_cookie(self) -> str:
        """读取 .cookie 文件，返回有效内容；注释行和空文件返回空字符串"""
        try:
            lines = open(COOKIE_FILE, encoding="utf-8").readlines()
            content = "".join(l for l in lines if not l.strip().startswith("#")).strip()
            return content
        except FileNotFoundError:
            return ""

    def save_cookie(self, cookie: str) -> None:
        """将 cookie 写入 .cookie 文件（覆盖原内容）"""
        with open(COOKIE_FILE, "w", encoding="utf-8") as f:
            f.write(cookie.strip() + "\n")


_auth = AuthProvider()


def _headers(content_type: str | None = None) -> dict:
    h = {"accept": "application/json, text/plain, */*", **_auth.get_headers()}
    if content_type:
        h["Content-Type"] = content_type
    return h


def _check_auth_error(resp: requests.Response) -> None:
    """检测 401/无权限响应，并给出可操作的鉴权错误信息"""
    if resp.status_code == 401:
        cookie = _auth.read_cookie()
        if not cookie:
            raise NoCookieError("未在 assets/.cookie 检测到 Cookie，无法完成鉴权")
        raise NoCookieError("assets/.cookie 中 Cookie 可能已失效，请重新获取")
    resp.raise_for_status()


def api_get(path: str, params: dict | None = None) -> dict:
    """发起 GET 请求"""
    resp = requests.get(
        f"{BASE_URL}{path}",
        headers=_headers(),
        params=params,
        timeout=30,
    )
    _check_auth_error(resp)
    return resp.json()


def api_post(path: str, body: dict) -> dict:
    """发起 POST JSON 请求"""
    resp = requests.post(
        f"{BASE_URL}{path}",
        headers=_headers("application/json;charset=UTF-8"),
        json=body,
        timeout=30,
    )
    _check_auth_error(resp)
    return resp.json()

def get_data(resp: dict):
    """从 BaseHttpResponse<T> 取出 data 字段，失败时抛出 RuntimeError"""
    if not resp.get("success", True):
        raise RuntimeError(resp.get("msg", "request failed"))
    return resp.get("data")


def save_cookie(cookie: str) -> None:
    """外部调用：将 cookie 持久化到 assets/.cookie"""
    _auth.save_cookie(cookie)
    print(f"Cookie 已写入: {COOKIE_FILE}")
