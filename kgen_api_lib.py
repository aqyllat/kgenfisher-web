"""
KGeN API - Python Client
Semua API call KGeN tanpa UI.
Requires: pip install requests
"""

import time
import json
import random
import string
import base64
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, quote

try:
    from aptos_sdk.account import Account
    from aptos_sdk.account_address import AccountAddress
    from aptos_sdk.async_client import RestClient
    from aptos_sdk.transactions import EntryFunction, TransactionArgument, Serializer, RawTransaction, TransactionPayload
    from aptos_sdk.type_tag import TypeTag, StructTag
    APTOS_SDK_AVAILABLE = True
except ImportError:
    APTOS_SDK_AVAILABLE = False

BASE_URL = "https://prod-api-backend.kgen.io"
MAX_SPIN_BET = 5000
DAILY_SPIN_LIMIT = 5000

# ── Proxy Configuration ──
# Set ini untuk bypass IP ban. Contoh:
#   HTTP Proxy:   "http://username:password@proxy-server:port"
#   SOCKS5 Proxy: "socks5://127.0.0.1:9050"  (Tor)
#   No Proxy:     None
PROXY_URL = None

def set_proxy(proxy_url):
    """Set proxy for all KGen API calls. Pass None to disable."""
    global PROXY_URL
    PROXY_URL = proxy_url
    if proxy_url:
        print(f"  [*] Proxy aktif: {proxy_url}")
    else:
        print(f"  [*] Proxy dimatikan, pake IP langsung")

def get_proxy_dict():
    """Return proxy dict for requests library."""
    if PROXY_URL:
        return {"http": PROXY_URL, "https": PROXY_URL}
    return None

CAMPAIGNS = {
    "campaign1": {
        "id": "7585dbbc-0f88-48d8-b22c-7b640a45a79f",
        "name": "Airdrop Hub",
        "task_ids": [
            "97d8467a-26ee-4bee-85e1-3b38e1f423d7",
            "cf1be895-5740-40b6-90dd-d65e22fb5657",
            "47e22d76-9979-481a-bf05-efc8c6b40f52",
            "74d218d8-658a-4805-8a0d-ff5a885b0c03",
            "162f36c0-09fc-444a-bab9-ca79948d709d",
            "19cdf008-a848-4d8b-90f6-9d748c71004a",
            "a9cd389b-8747-4e21-a52e-0b6cb945a7c5",
            "9bd012c7-8d13-4f03-8a55-7e571f8e67bd",
            "6f2897f3-e57f-4ac3-a90f-7c57b14e1a06",
            "892f8292-e2e6-4358-9072-a9b016c843a0",
            "2d609c1f-d894-413d-93a6-46be5ae36c78",
            "0ddcd2ec-73d5-4dfd-b923-de3055424b1b",
            "8e8dc8f8-c81f-4957-95cc-b6f2a37c4571",
        ],
    },
    "campaign2": {
        "id": "7ed14636-0649-4bac-a00c-ddb5572eb0e0",
        "name": "KGeN Social",
        "task_ids": [
            "4f55e8bd-d4d1-494c-9f15-c4c8c0a9c684",
            "89b28777-e7f6-423b-b5e0-4124702a80bd",
            "8dbcecba-d769-4d7b-83dd-2df012fef0d1",
        ],
    },
    "campaign3": {
        "id": "2270e7db-9fc2-457f-9267-515462d2e023",
        "name": "Weekly Quest",
        "task_ids": [
            "c2c81cf0-75f8-48de-a4ac-94fd51701078",
            "35414d02-614f-422d-9bd2-937a790020c6",
            "f1992ca3-3e3c-4dac-92b8-f5d5e26b68ea",
        ],
    },
    "campaign4": {
        "id": "1702fe57-52eb-48d5-98ce-1cd37760f702",
        "name": "Community Quest",
        "task_ids": [
            "0e7d5a44-6b9d-400c-a693-6a53e3e35f98",
            "64c5b9b6-46ae-42a8-8287-967b90572f3c",
            "26faaa6b-666e-4fdc-9f3d-31918424bd26",
        ],
    },
}

MINTED_STATES = {
    "MINTED", "COMPLETED", "MintStateMinted", "MintStateCompleted",
    "MintStateMintComplete", "MINT_COMPLETE", "MintComplete",
}

TWITTER_CLIENT_ID = "SkhmaW1uUVRZYnY5VnZMa1QwMHE6MTpjaQ"
DISCORD_CLIENT_ID = "1178767510775017563"
SOCIAL_REDIRECT_URI = "https://prod-api-backend.kgen.io/social-auth/redirect-to-page"


def decode_jwt_payload(token):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1].replace("-", "+").replace("_", "/")
        pad = len(payload) % 4
        payload += "=" * (4 - pad) if pad else ""
        decoded = base64.b64decode(payload).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        return {}


def get_token_expiry(token):
    payload = decode_jwt_payload(token)
    if not payload.get("exp"):
        return {"expires_at": None, "is_expired": False}
    expires_at = payload["exp"] * 1000
    now = time.time() * 1000
    return {"expires_at": expires_at, "expires_in_ms": expires_at - now, "is_expired": now >= expires_at}


def is_token_valid(token):
    if not token:
        return False
    info = get_token_expiry(token)
    return not info.get("is_expired", False)


def refresh_kgen_token(refresh_token):
    """
    Refresh KGen Bearer token using the refresh token.
    Returns (new_bearer_token, new_refresh_token) or (None, None) if failed.
    """
    if not refresh_token: return None, None
    url = f"{BASE_URL}/authentication/token/refresh?refresh_token={refresh_token}&source=website"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            bt = data.get("token") or data.get("access_token") or data.get("accessToken")
            rt = data.get("refreshToken") or data.get("refresh_token")
            if bt: return bt, rt
        return None, None
    except:
        return None, None


def make_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Source": "website",
    }


def safe_request(method, url, headers, body=None, retries=7, timeout=30):
    for attempt in range(1, retries + 1):
        try:
            kwargs = {"headers": headers, "timeout": timeout}
            if body is not None:
                kwargs["json"] = body
            proxy = get_proxy_dict()
            if proxy:
                kwargs["proxies"] = proxy
            resp = getattr(requests, method.lower())(url, **kwargs)
            if resp.status_code in (401, 403):
                return {"_authError": True, "status": resp.status_code}
            if resp.status_code == 404:
                return {"_notFound": True, "status": 404}
            if resp.status_code == 429:
                try: data = resp.json()
                except: data = {}
                return {"_rateLimited": True, "status": 429, **data}
            if resp.status_code == 409:
                try: data = resp.json()
                except: data = {}
                return {"_conflict": True, "status": 409, **data}
            ct = resp.headers.get("Content-Type", "")
            if "application/json" not in ct:
                if attempt < retries:
                    time.sleep(2)
                    continue
                return {"_error": True, "status": resp.status_code, "message": f"Non-JSON after {retries} retries"}
            return resp.json()
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                return {"_error": True, "message": f"Failed after {retries} retries: {str(e)}"}
    return {"_error": True, "message": f"All {retries} retries failed"}


# ─── PROFILE ───

def get_profile(token):
    h = make_headers(token)
    data = safe_request("GET", f"{BASE_URL}/users/me/profile", h)
    if data.get("_authError"):
        raise Exception("Invalid or expired token (401/403)")
    if data.get("_error"):
        raise Exception(f"Profile request failed: {data.get('message')}")
    user_id = data.get("id") or data.get("userId")
    if not user_id:
        raise Exception("Could not get user ID")
    return {
        "id": user_id,
        "username": data.get("username") or data.get("displayName") or "User",
        "email": data.get("email") or data.get("emailId") or "",
    }


def get_full_profile(token):
    h = make_headers(token)
    data = safe_request("GET", f"{BASE_URL}/users/me/profile", h)
    if data.get("_authError"):
        raise Exception("Invalid or expired token")
    return data


def get_user_id(token):
    h = make_headers(token)
    data = safe_request("GET", f"{BASE_URL}/users/me/profile", h)
    if data.get("_authError"):
        raise Exception("Auth error getting user ID")
    return data.get("id") or data.get("userId") or ""


# ─── BALANCE ───

def get_game_balance(token):
    h = make_headers(token)
    data = safe_request("GET", f"{BASE_URL}/rkade/kgen/v2/balance", h)
    if data.get("_authError"):
        return {"k_point": 0, "rkgen_token": 0, "_authError": True}
    bal = (data.get("data") or {}).get("balances") or {}
    return {
        "k_point": (bal.get("k_point") or {}).get("balance", 0),
        "rkgen_token": (bal.get("rkgen_token") or {}).get("balance", 0),
        "kgen_token": (bal.get("kgen_token") or {}).get("balance", 0),
    }


# ─── SOCIAL ───

def get_oauth_url(token, provider, callback_url):
    h = make_headers(token)
    provider_upper = provider.upper()
    params = {
        "host": "INDIGG",
        "platform": "WEB",
        "provider": provider_upper,
        "redirectUri": f"{callback_url}?provider={provider_upper}"
    }
    url = f"{BASE_URL}/social-auth?{urlencode(params)}"
    resp = safe_request("GET", url, h, None)
    if resp.get("_error") or resp.get("_authError"):
        raise Exception(f"Failed to get OAuth URL: {resp}")
    
    redirect_url = (resp.get("data") or {}).get("redirectUrl")
    if not redirect_url:
        raise Exception(f"No redirectUrl in response: {resp}")
    
    # KGen sometimes returns www.twitter.com which can cause session issues
    if "www.twitter.com" in redirect_url:
        redirect_url = redirect_url.replace("www.twitter.com", "twitter.com")
        
    return redirect_url


def api_connect_social(token, code, provider):
    """Connect Twitter/Discord via OAuth code exchange"""
    h = make_headers(token)
    body = {
        "code": code,
        "provider": provider,
        "host": "INDIGG",
        "platform": "WEB",
    }
    if provider in ("TWITTER", "DISCORD"):
        body["redirectUri"] = SOCIAL_REDIRECT_URI
    resp = requests.post(f"{BASE_URL}/social-auth/authenticate", headers=h, json=body, timeout=30)
    if not resp.ok:
        try: detail = resp.json().get("message", "")
        except: detail = resp.text[:200]
        raise Exception(f"Connect {provider} failed ({resp.status_code}): {detail}")
    return resp.json()


def connect_steam_api(token, steam_id):
    h = make_headers(token)
    resp = requests.post(f"{BASE_URL}/social-auth/connect/steam", headers=h, json={"steamId": steam_id}, timeout=30)
    if not resp.ok:
        try: detail = resp.json().get("message", "")
        except: detail = resp.text[:200]
        raise Exception(f"Connect Steam failed ({resp.status_code}): {detail}")
    return resp.json()


def quick_check_social(tokens):
    result = {"TWITTER": False, "DISCORD": False, "STEAM": False}
    for t in tokens:
        try:
            profile = get_full_profile(t)
            if not profile or profile.get("_authError"):
                continue
            for key in profile:
                k = key.lower()
                val = profile[key]
                if val is None or val == "" or val is False:
                    continue
                if "twitter" in k: result["TWITTER"] = True
                if "discord" in k: result["DISCORD"] = True
                if "steam" in k: result["STEAM"] = True
            for la in (profile.get("linkedAccounts") or []):
                prov = (la.get("provider") or la.get("type") or "").upper()
                if "TWITTER" in prov: result["TWITTER"] = True
                if "DISCORD" in prov: result["DISCORD"] = True
                if "STEAM" in prov: result["STEAM"] = True
            break
        except:
            continue
    return result


def disconnect_social(token, providers=None, log=None):
    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")
    providers = providers or ["TWITTER", "DISCORD", "STEAM"]
    results = []
    for p in providers:
        h = make_headers(token)
        resp = safe_request("DELETE", f"{BASE_URL}/social-auth/disconnect", h, {"provider": p})
        ok = not resp.get("_authError") and resp.get("success") is not False
        _log(f"{p} {'disconnected' if ok else 'failed'}", "success" if ok else "error")
        results.append({"provider": p, "success": ok})
        time.sleep(1)
    return results


# ─── CAMPAIGNS ───

def _is_completed(item):
    for key in ["isCompleted", "completed"]:
        if key in item and isinstance(item[key], bool):
            return item[key]
    done_states = {"COMPLETED", "COMPLETE", "DONE", "CLAIMED", "SUCCESS"}
    for key in ["taskState", "progressState", "userTaskProgressState", "taskProgressState", "state", "status"]:
        if key in item and isinstance(item[key], str):
            if item[key].upper() in done_states:
                return True
    return False


def _get_completed_ids(details):
    ids = set()
    for d in details:
        if _is_completed(d):
            task_id = d.get("taskID") or d.get("taskId") or d.get("id")
            if task_id: ids.add(task_id)
    return ids


def run_campaign_tasks(token, campaign_key, log=None):
    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")

    camp = CAMPAIGNS.get(campaign_key)
    if not camp:
        raise Exception(f"Invalid campaign: {campaign_key}")

    user_id = get_user_id(token)
    h = make_headers(token)

    # Start campaign
    safe_request("POST", f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/{camp['id']}/start", h, {})

    # Get campaign data
    url = f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/by-state?status=LIVE,PUBLISHED,PAUSED"
    data = safe_request("GET", url, h)
    if data.get("_authError"):
        raise Exception("Auth error")

    target_campaign = None
    for c in (data.get("campaignsWithUserProgress") or []):
        if (c.get("campaignInfo") or {}).get("campaignID") == camp["id"]:
            target_campaign = c
            break

    done = set()
    tasks = []
    title_by_id = {}

    if target_campaign:
        progress = target_campaign.get("userCampaignProgressInfo") or {}
        done = _get_completed_ids(progress.get("progressDetails") or [])
        tasks = (target_campaign.get("campaignInfo") or {}).get("campaignTasks") or []
        for t in tasks:
            title_by_id[t["taskID"]] = t.get("title", "(no title)")

    if not tasks and camp["task_ids"]:
        tasks = [{"taskID": tid, "title": tid[:8] + "..."} for tid in camp["task_ids"]]
        for t in tasks:
            title_by_id[t["taskID"]] = t["title"]

    for tid in done:
        _log(f"[DONE] {title_by_id.get(tid, tid)}", "info")

    pending = [t for t in tasks if t["taskID"] not in done]
    _log(f"Pending: {len(pending)} tasks", "info")

    def validate_task(t):
        validate_url = f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/{camp['id']}/tasks/{t['taskID']}/validate"
        resp = safe_request("POST", validate_url, h, {})
        status = "✓" if not resp.get("_error") and not resp.get("_authError") else "✗"
        _log(f"[{status}] {t['title']}", "success" if status == "✓" else "error")
        return t["taskID"]

    attempted = []
    with ThreadPoolExecutor(max_workers=min(len(pending) or 1, 10)) as ex:
        futures = [ex.submit(validate_task, t) for t in pending]
        for f in as_completed(futures):
            attempted.append(f.result())

    time.sleep(1.5)
    latest = safe_request("GET", url, h)
    done_after = set()
    for lc in (latest.get("campaignsWithUserProgress") or []):
        if (lc.get("campaignInfo") or {}).get("campaignID") == camp["id"]:
            lp = lc.get("userCampaignProgressInfo") or {}
            done_after = _get_completed_ids(lp.get("progressDetails") or [])

    completed_now = [tid for tid in attempted if tid in done_after]
    _log(f"Done before: {len(done)} | New: {len(completed_now)} | Failed: {len(attempted) - len(completed_now)}", "info")
    return {"done_before": len(done), "completed_now": len(completed_now)}


def run_all_campaigns(token, log=None):
    """Fetch ALL campaigns dari API dan validate semua tasks (dinamis, ga hardcoded)."""
    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")
    
    user_id = get_user_id(token)
    h = make_headers(token)
    
    # Fetch ALL live campaigns
    url = f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/by-state?status=LIVE,PUBLISHED,PAUSED"
    data = safe_request("GET", url, h)
    if data.get("_authError"):
        raise Exception("Auth error")
    
    all_campaigns = data.get("campaignsWithUserProgress") or []
    _log(f"Total campaigns ditemukan: {len(all_campaigns)}", "info")
    
    results = {}
    total_new = 0
    
    # ── PHASE 1: Fire all tasks across all campaigns (no sleep, no re-check) ──
    for camp_data in all_campaigns:
        info = camp_data.get("campaignInfo") or {}
        camp_id = info.get("campaignID") or ""
        camp_name = info.get("name") or info.get("title") or camp_id[:12] + "..."
        
        if not camp_id:
            continue
        
        # Start campaign (fire & forget)
        safe_request("POST", f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/{camp_id}/start", h, {})
        
        # Get tasks & progress
        progress = camp_data.get("userCampaignProgressInfo") or {}
        done = _get_completed_ids(progress.get("progressDetails") or [])
        tasks = info.get("campaignTasks") or []
        
        pending = [t for t in tasks if (t.get("taskID") or "") not in done]
        
        if not pending:
            if tasks:
                _log(f"[{camp_name}] Semua {len(tasks)} task udah DONE ✅", "info")
            continue
        
        _log(f"[{camp_name}] {len(done)} done, {len(pending)} pending", "info")
        
        # Validate pending tasks — parallel, up to 15 workers
        def validate_task(t, _cid=camp_id, _cname=camp_name):
            tid = t.get("taskID", "")
            title = t.get("title", tid[:8] + "...")
            reward = t.get("rewardAmount", 0)
            validate_url = f"{BASE_URL}/platform-campaign-hub/s2s/airdrop-campaign/user-progress/{user_id}/campaigns/{_cid}/tasks/{tid}/validate"
            resp = safe_request("POST", validate_url, h, {})
            status = "✓" if not resp.get("_error") and not resp.get("_authError") else "✗"
            reward_str = f" (+{reward} KP)" if reward else ""
            _log(f"[{status}] {title}{reward_str}", "success" if status == "✓" else "error")
            return (tid, _cid)
        
        with ThreadPoolExecutor(max_workers=min(len(pending), 15)) as ex:
            futures = [ex.submit(validate_task, t) for t in pending]
            for f in as_completed(futures):
                f.result()  # just consume, no sleep needed
        
        results[camp_id] = {"name": camp_name, "done_before": len(done), "pending": len(pending)}
    
    # ── PHASE 2: Single bulk verification at the end ──
    latest = safe_request("GET", url, h)
    for lc in (latest.get("campaignsWithUserProgress") or []):
        lc_info = lc.get("campaignInfo") or {}
        lc_id = lc_info.get("campaignID") or ""
        if lc_id in results:
            lp = lc.get("userCampaignProgressInfo") or {}
            done_after = _get_completed_ids(lp.get("progressDetails") or [])
            r = results[lc_id]
            new_count = max(0, len(done_after) - r["done_before"])
            total_new += new_count
            _log(f"[{r['name']}] Done: {r['done_before']} → {len(done_after)} | New: {new_count}", "info")
    
    _log(f"Total tasks baru completed: {total_new}", "info")
    return results


# ─── SPIN ───

def auto_spin(token, game="wheel", bet=5000, max_spins=5, log=None):
    if game not in ("wheel", "slots"):
        raise Exception("Invalid game")
    bet = min(bet, MAX_SPIN_BET)

    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")

    h = make_headers(token)
    spin_count = 0
    total_bet = 0

    for i in range(max_spins):
        bal = get_game_balance(token)
        if bal["k_point"] < bet:
            _log(f"K-Points habis ({bal['k_point']})", "info")
            break

        spin_id = None
        for _ in range(5):
            data = safe_request("POST", f"{BASE_URL}/rkade/kgen/v2/spin/{game}", h, {"bet": {"amount": bet, "currency": "k_point"}})
            if data.get("_rateLimited"):
                _log("Daily limit (429)", "error")
                return {"spins": spin_count, "total_bet": total_bet}
            if data.get("_conflict"):
                time.sleep(1.5)
                continue
            spin_id = (data.get("data") or {}).get("spinId")
            if spin_id: break
            time.sleep(0.3)

        if not spin_id:
            _log(f"Spin #{i+1} failed", "error")
            continue

        spin_count += 1
        total_bet += bet

        for _ in range(10):
            pd = safe_request("GET", f"{BASE_URL}/rkade/kgen/v2/spin/{game}/{spin_id}", h)
            spin_data = pd.get("data") or {}
            if spin_data.get("status") == "completed":
                # Ambil hasil dari finalResult
                final = spin_data.get("finalResult") or {}
                won_amount = final.get("winnings", 0)
                reward_curr = final.get("rewardCurrency", "k_point").replace("k_point", "K-Points").replace("rkgen_token", "R-KGEN")
                break
            time.sleep(0.5)

        after = get_game_balance(token)
        _log(f"Spin #{spin_count} | Won: {won_amount} {reward_curr} | Bal: {after['k_point']}", "info")

    return {"spins": spin_count, "total_bet": total_bet}


# ─── MINT ───

def _is_minted_state(state):
    if not state: return False
    return state in MINTED_STATES or "MINTED" in state.upper() or "COMPLETE" in state.upper()


def get_pog_dashboard(token):
    h = make_headers(token)
    return safe_request("POST", f"{BASE_URL}/tps/pog/graphql/pog-dashboard", h, {
        "query": "{ pogDashboard { userId username mintState nftDetails { tokenId collectionName imageUrl } } }"
    })


def get_random_variants(token):
    h = make_headers(token)
    return safe_request("GET", f"{BASE_URL}/tps/pog/mint/random-variants", h)


def mint_nft(token, log=None):
    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")

    dashboard = get_pog_dashboard(token)
    if dashboard.get("_authError"):
        raise Exception("Auth error")

    pog = (dashboard.get("data") or {}).get("pogDashboard")
    if pog and _is_minted_state(pog.get("mintState")):
        _log(f"Already minted! ({pog.get('username', '?')})", "success")
        return {"already_minted": True}

    # Reserve username
    h = make_headers(token)
    username = "".join(random.choices(string.ascii_lowercase, k=5)) + str(random.randint(10000, 99999))
    _log(f"Reserving username: {username}", "info")
    safe_request("POST", f"{BASE_URL}/tps/pog/mint/state/MintStateReserveUsername", h, {"username": username})

    time.sleep(1)

    # Get variants & quick mint
    variants = get_random_variants(token)
    body = {}
    vd = variants.get("data") or variants
    if isinstance(vd, dict) and vd.get("categoryId"):
        body = {k: vd[k] for k in ("categoryId", "characterId", "styleId") if k in vd}
    elif isinstance(vd, list) and vd:
        pick = random.choice(vd)
        body = {"categoryId": pick.get("categoryId"), "characterId": pick.get("characterId"), "styleId": pick.get("styleId")}

    _log("Quick minting...", "info")
    result = safe_request("POST", f"{BASE_URL}/tps/pog/mint/quick-mint", h, body)
    if result.get("_error"):
        _log(f"Mint error: {result.get('message')}", "error")
    else:
        _log("Mint sent!", "success")
    return result


# ─── SWAP & WITHDRAW ───

def get_token_balances(token):
    h = make_headers(token)
    chains = "chains=Aptos&chains=BSC&chains=Base&chains=Haqq&chains=KlaytnKaia&chains=Kroma&chains=Polygon&chains=Zksync"
    return safe_request("GET", f"{BASE_URL}/wallets/tokens/balance/v2?{chains}", h)


async def _fund_aptos_wallet_async(pk_hex: str, to_address: str, amount_kgen: float = 0.02) -> bool:
    if not APTOS_SDK_AVAILABLE:
        print("  [!] aptos-sdk is not installed. Funding failed.")
        return False
        
    try:
        if pk_hex.startswith("0x"): pk_hex = pk_hex[2:]
        sender = Account.load_key(pk_hex)
        node_url = "https://fullnode.mainnet.aptoslabs.com/v1"
        rest_client = RestClient(node_url)

        # KGEN FA (Fungible Asset) Metadata Address on Aptos
        kgen_token_address = "0x2a8227993a4e38537a57caefe5e7e9a51327bf6cd732c1f56648f26f68304ebc"
        amount_in_smallest_units = int(amount_kgen * (10**8))  # Assuming 8 decimals for KGEN (standard)

        payload = EntryFunction.natural(
            "0x1::primary_fungible_store",
            "transfer",
            [TypeTag(StructTag.from_str("0x1::fungible_asset::Metadata"))],
            [
                TransactionArgument(AccountAddress.from_str_relaxed(kgen_token_address), Serializer.struct),
                TransactionArgument(AccountAddress.from_str_relaxed(to_address), Serializer.struct),
                TransactionArgument(amount_in_smallest_units, Serializer.u64),
            ]
        )

        sequence_number = await rest_client.account_sequence_number(sender.address())
        
        # Wrap the entry function logic in a proper transaction payload to avoid deserialization errors on the node
        txn_payload = TransactionPayload(payload)
        
        # Limit gas required via client config natively
        rest_client.client_config.max_gas_amount = 100000

        txn = await rest_client.create_bcs_signed_transaction(
            sender, 
            txn_payload
        )
        
        txn_hash = await rest_client.submit_bcs_transaction(txn)
        await rest_client.wait_for_transaction(txn_hash)
        await rest_client.close()
        return True
    except Exception as e:
        print(f"  [!] APTOS Funding Error: {e}")
        return False

def fund_aptos_wallet(pk_hex: str, to_address: str, amount_kgen: float = 0.02) -> bool:
    return asyncio.run(_fund_aptos_wallet_async(pk_hex, to_address, amount_kgen))


def auto_swap_and_withdraw(token, withdraw_address, log=None, fund_pk=None):
    def _log(msg, t="info"):
        if log: log(msg, t)
        else: print(f"  [{t.upper()}] {msg}")

    h = make_headers(token)
    balances = get_token_balances(token)
    if balances.get("_authError"):
        raise Exception("Auth error")

    rkgen = 0.0
    kgen = 0.0
    def scan(arr):
        nonlocal rkgen, kgen
        for t in arr:
            name = (t.get("token") or t.get("name") or "").upper()
            amt = float(t.get("totalTransferableAmount") or t.get("amount") or t.get("balance") or 0)
            if name in ("RKGEN", "R_KGEN"): rkgen = max(rkgen, amt)
            if name == "KGEN": kgen = max(kgen, amt)

    root = balances.get("data") or balances
    if isinstance(root, list): scan(root)
    elif isinstance(root, dict):
        for v in root.values():
            if isinstance(v, list): scan(v)

    _log(f"rKGEN={rkgen}, KGEN={kgen}", "info")

    if rkgen >= 1.0:
        _log(f"Swapping {rkgen} rKGEN → KGEN...", "info")
        ded = safe_request("POST", f"{BASE_URL}/swap/deductions", h, {
            "fromToken": "RKGEN", "toToken": "KGEN", "fromTokenAmount": rkgen,
            "fromTokenChain": "Aptos", "toTokenChain": "Aptos",
        })
        txn_id = (ded.get("data") or {}).get("txnId")
        if txn_id:
            safe_request("POST", f"{BASE_URL}/swap", h, {"txnId": txn_id})
            _log("Swap done! Updating balances...", "success")
            time.sleep(3)
            # Re-fetch balances after swap
            balances = get_token_balances(token)
            kgen = 0.0
            root = balances.get("data") or balances
            if isinstance(root, list): scan(root)
            elif isinstance(root, dict):
                for v in root.values():
                    if isinstance(v, list): scan(v)
    else:
        _log(f"Skip Swap: rKGEN ({rkgen}) < 1.0", "warning")

    # Get user's KGen platform internal deposit wallet
    wallets = safe_request("GET", f"{BASE_URL}/users/me/wallets", h)
    user_wallet = ""
    inner = wallets.get("data") or wallets
    if isinstance(inner, dict):
        user_wallet = inner.get("aptosWalletAddress") or inner.get("address") or ""
    elif isinstance(inner, list):
        for w in inner:
            if w.get("chain") == "Aptos": user_wallet = w.get("address", ""); break

    # Check for auto-fund condition
    if kgen >= 0.90 and kgen < 1.0:
        if fund_pk and user_wallet:
            required_kgen = 1.0 - kgen + 0.01  # Add slightly more than needed (e.g., 0.99 -> needs 0.02)
            _log(f"KGEN is {kgen}. Auto-funding {required_kgen:.2f} KGEN to {user_wallet[:14]}...", "info")
            success = fund_aptos_wallet(fund_pk, user_wallet, required_kgen)
            if success:
                _log("Auto-fund transaction confirmed! Waiting 15s for KGen indexing...", "success")
                time.sleep(15)
                # Re-fetch balances after external deposit
                balances = get_token_balances(token)
                kgen = 0.0
                root = balances.get("data") or balances
                if isinstance(root, list): scan(root)
                elif isinstance(root, dict):
                    for v in root.values():
                        if isinstance(v, list): scan(v)
            else:
                _log("Auto-fund failed.", "error")
        else:
            _log(f"KGEN is {kgen} but no Private Key provided. Skipping withdraw.", "warning")

    if kgen < 1.0:
        _log(f"KGEN too low for withdrawal ({kgen})", "warning")
        return {"swapped": rkgen >= 1.0, "withdrawn": False}

    if not withdraw_address:
        return {"swapped": rkgen >= 1.0, "withdrawn": False}

    _log(f"Withdrawing {kgen} KGEN to {withdraw_address[:14]}...", "info")

    send_ded = safe_request("POST", f"{BASE_URL}/send/deductions", h, {
        "token": "KGEN", "amount": kgen, "partner": "KGEN", "chain": "Aptos",
        "toWallet": withdraw_address, "fromWallet": user_wallet,
    })
    order_id = (send_ded.get("data") or {}).get("orderId")
    if order_id:
        safe_request("POST", f"{BASE_URL}/send/order", h, {"orderId": order_id})
        _log("Withdraw done!", "success")

    return {"swapped": rkgen >= 1.0, "withdrawn": True}
