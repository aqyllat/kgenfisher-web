"""
KGenFisher — Multi-Tenant Backend Server
FastAPI + WebSocket for real-time log streaming with JWT Auth
"""

import asyncio
import os
import time
import threading
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
import jwt
from jwt.exceptions import PyJWTError

# Import database models and utilities
from database import get_db, User, KGenAccount, get_password_hash, verify_password, create_access_token, SECRET_KEY, ALGORITHM

# Import existing bot library
from kgen_api_lib import (
    is_token_valid, get_profile, get_game_balance,
    run_all_campaigns, auto_spin, auto_swap_and_withdraw
)


# ── Auth Dependencies ──

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except PyJWTError:
        raise credentials_exception
    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exception
    return user


async def get_ws_current_user(token: str, db: Session):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user = db.query(User).filter(User.username == username).first()
        return user
    except:
        return None


# ── WebSocket Log Manager ──

class LogManager:
    def __init__(self):
        # Format: { user_id: [websocket_connections] }
        self.connections: dict[int, List[WebSocket]] = {}
        # Format: { user_id: [log_entries] }
        self.history: dict[int, List[dict]] = {}
        self.max_history = 500

    async def connect(self, ws: WebSocket, user_id: int):
        await ws.accept()
        if user_id not in self.connections:
            self.connections[user_id] = []
        if user_id not in self.history:
            self.history[user_id] = []
            
        self.connections[user_id].append(ws)
        # Send history
        for entry in self.history[user_id][-100:]:
            try:
                await ws.send_json(entry)
            except:
                pass

    def disconnect(self, ws: WebSocket, user_id: int):
        if user_id in self.connections and ws in self.connections[user_id]:
            self.connections[user_id].remove(ws)

    def log(self, user_id: int, msg: str, level: str = "info", account: str = ""):
        entry = {
            "msg": msg,
            "level": level,
            "account": account,
            "ts": time.time(),
        }
        if user_id not in self.history:
            self.history[user_id] = []
            
        self.history[user_id].append(entry)
        if len(self.history[user_id]) > self.max_history:
            self.history[user_id] = self.history[user_id][-self.max_history:]
            
        # Broadcast to user's connected clients
        if user_id in self.connections:
            for ws in list(self.connections[user_id]):
                try:
                    asyncio.run_coroutine_threadsafe(
                        ws.send_json(entry),
                        self._loop
                    )
                except:
                    pass

    def set_loop(self, loop):
        self._loop = loop


log_mgr = LogManager()

# ── Bot runner state ──
# Store state per user {user_id: {"running": bool, "action": str, ...}}
class BotState:
    def __init__(self):
        self.states = {}
    
    def get(self, user_id):
        if user_id not in self.states:
            self.states[user_id] = {"running": False, "action": "", "progress": 0, "total": 0}
        return self.states[user_id]
        
    def set(self, user_id, **kwargs):
        state = self.get(user_id)
        for k, v in kwargs.items():
            state[k] = v

bot_state = BotState()


# ── Worker thread functions ──

def _make_log(user_id: int, account_label=""):
    def _log(msg, t="info"):
        log_mgr.log(user_id, msg, t, account_label)
    return _log

def run_campaigns_worker(user_id: int, target_bearers: List[str]):
    bot_state.set(user_id, running=True, action="campaigns", total=len(target_bearers), progress=0)
    
    for i, bearer in enumerate(target_bearers):
        bot_state.set(user_id, progress=i + 1)
        label = f"#{i+1}"
        _log = _make_log(user_id, label)
        
        try:
            if not is_token_valid(bearer):
                _log("Token expired, skip", "warning")
                continue
            profile = get_profile(bearer)
            username = profile.get("username", "?")
            _log(f"Login: {username}", "success")
            
            run_all_campaigns(bearer, _log)
            
            bal = get_game_balance(bearer)
            _log(f"K-Points: {bal.get('k_point', 0):,}", "info")
        except Exception as e:
            _log(f"Error: {e}", "error")
    
    bot_state.set(user_id, running=False)
    log_mgr.log(user_id, "✅ Campaign selesai!", "success")

def run_spin_worker(user_id: int, target_bearers: List[str], bet=5000, max_spins=5):
    bot_state.set(user_id, running=True, action="spin", total=len(target_bearers), progress=0)
    
    for i, bearer in enumerate(target_bearers):
        bot_state.set(user_id, progress=i + 1)
        label = f"#{i+1}"
        _log = _make_log(user_id, label)
        
        try:
            if not is_token_valid(bearer):
                _log("Token expired, skip", "warning")
                continue
            profile = get_profile(bearer)
            _log(f"Login: {profile.get('username', '?')}", "info")
            auto_spin(bearer, bet=bet, max_spins=max_spins, log=_log)
        except Exception as e:
            _log(f"Error: {e}", "error")
    
    bot_state.set(user_id, running=False)
    log_mgr.log(user_id, "✅ Spin selesai!", "success")

def run_swap_worker(user_id: int, target_bearers: List[str]):
    bot_state.set(user_id, running=True, action="swap", total=len(target_bearers), progress=0)
    
    for i, bearer in enumerate(target_bearers):
        bot_state.set(user_id, progress=i + 1)
        label = f"#{i+1}"
        _log = _make_log(user_id, label)
        
        try:
            if not is_token_valid(bearer):
                _log("Token expired, skip", "warning")
                continue
            profile = get_profile(bearer)
            _log(f"Login: {profile.get('username', '?')}", "info")
            auto_swap_and_withdraw(bearer, _log)
        except Exception as e:
            _log(f"Error: {e}", "error")
    
    bot_state.set(user_id, running=False)
    log_mgr.log(user_id, "✅ Swap & Withdraw selesai!", "success")

def check_points_worker(user_id: int, target_bearers: List[str]):
    bot_state.set(user_id, running=True, action="check", total=len(target_bearers), progress=0)
    from kgen_api_lib import quick_check_social
    
    db = next(get_db())
    try:
        for i, bearer in enumerate(target_bearers):
            bot_state.set(user_id, progress=i + 1)
            label = f"#{i+1}"
            _log = _make_log(user_id, label)
            
            try:
                if not is_token_valid(bearer):
                    _log("Token expired", "warning")
                    continue
                profile = get_profile(bearer)
                bal = get_game_balance(bearer)
                socials = quick_check_social([bearer])
                
                uname = profile.get('username', '?')
                kp = bal.get('k_point', 0)
                tw = '✓' if socials.get('TWITTER') else '✗'
                dc = '✓' if socials.get('DISCORD') else '✗'
                
                _log(f"{uname} — KP: {kp:,} | TW: {tw} | DC: {dc}", "success")
                
                # Update cache in DB
                acc = db.query(KGenAccount).filter(KGenAccount.bearer_token == bearer, KGenAccount.user_id == user_id).first()
                if acc:
                    acc.username = uname
                    acc.points = kp
                    acc.twitter = 1 if socials.get('TWITTER') else 0
                    acc.discord = 1 if socials.get('DISCORD') else 0
                    db.commit()
            except Exception as e:
                _log(f"Error: {e}", "error")
    finally:
        db.close()
        
    bot_state.set(user_id, running=False)


# ── Pydantic Models ──

class UserRegister(BaseModel):
    username: str
    password: str

class RunRequest(BaseModel):
    account_ids: List[int]

class SpinRequest(BaseModel):
    account_ids: List[int]
    bet: int = 5000
    max_spins: int = 5

class AddAccountRequest(BaseModel):
    bearer: str
    refresh_token: Optional[str] = None


# ── FastAPI App ──

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Auto-migrate SQLite schema
    try:
        import sqlite3
        db_file = os.path.join(os.path.dirname(__file__), "kgenfisher.db")
        if os.path.exists(db_file):
            conn = sqlite3.connect(db_file)
            c = conn.cursor()
            try: c.execute("ALTER TABLE kgen_accounts ADD COLUMN twitter INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE kgen_accounts ADD COLUMN discord INTEGER DEFAULT 0")
            except: pass
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Migration error: {e}")

    log_mgr.set_loop(asyncio.get_event_loop())
    yield

app = FastAPI(title="KGenFisher SaaS Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── PUBLIC Proxy Endpoints (No Auth Required) ──
# These run on Railway's clean IP to bypass KGen's IP-level blocking
# Using Playwright (real headless Chrome) to bypass Cloudflare — CF can't block a real browser!

import requests as http_requests

KGEN_BASE = "https://prod-api-backend.kgen.io"
KGEN_REDIRECT_URI = "https://prod-api-backend.kgen.io/social-auth/redirect-to-page"
KGEN_GOOGLE_CLIENT_ID = "370849037649-pho1nskdq7jlj6alb8cudmsq6u3fg4bl.apps.googleusercontent.com"


def kgen_browser_fetch(url, method="POST", json_data=None, bearer=None):
    """
    Make KGen API call through a REAL headless Chrome browser.
    Cloudflare can't block this because it's a genuine browser with real JS engine + TLS.
    """
    from playwright.sync_api import sync_playwright
    import json as _json

    result = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        # Navigate to KGen's engage page first to get Cloudflare cookies
        try:
            page.goto("https://engage.kgen.io", wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(2000)  # Let CF challenge resolve
        except:
            pass  # Even if it fails, try the fetch anyway

        # Build the fetch JS
        headers_obj = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Source": "website",
        }
        if bearer:
            headers_obj["Authorization"] = f"Bearer {bearer}"

        fetch_script = """
        async (args) => {
            try {
                const opts = {
                    method: args.method,
                    headers: args.headers,
                    credentials: 'omit',
                };
                if (args.body && args.method !== 'GET') {
                    opts.body = JSON.stringify(args.body);
                }
                const resp = await fetch(args.url, opts);
                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) { body = text; }
                return { ok: resp.ok, status: resp.status, body: body };
            } catch(e) {
                return { ok: false, status: 0, body: e.toString() };
            }
        }
        """

        result = page.evaluate(fetch_script, {
            "url": url,
            "method": method,
            "headers": headers_obj,
            "body": json_data,
        })

        browser.close()

    return result


class ExchangeCodeRequest(BaseModel):
    code: str
    redirect_uri: Optional[str] = None


@app.post("/api/exchange-code")
def api_exchange_code(req: ExchangeCodeRequest):
    """
    Proxy endpoint: tuker Google OAuth code jadi KGen token.
    Jalan di Railway + Real Chromium browser = bypass Cloudflare + IP blocking.
    """
    redirect_uri = req.redirect_uri or KGEN_REDIRECT_URI

    payload = {
        "code": req.code,
        "provider": "GOOGLE",
        "platform": "WEB",
        "emailOptIn": True,
        "redirectUri": redirect_uri,
    }

    try:
        result = kgen_browser_fetch(
            f"{KGEN_BASE}/oauth/authenticate?source=website",
            method="POST",
            json_data=payload,
        )

        if result and result.get("ok"):
            raw = result.get("body", {})
            if isinstance(raw, str):
                return {"success": False, "error": raw[:500]}
            data = raw.get("data", raw)
            return {
                "success": True,
                "accessToken": data.get("accessToken") or data.get("token") or raw.get("token"),
                "refreshToken": data.get("refreshToken") or raw.get("refreshToken"),
                "idToken": data.get("idToken") or raw.get("idToken"),
                "raw": raw,
            }
        else:
            body = result.get("body", "") if result else "No result"
            status = result.get("status", 0) if result else 0
            error_text = body if isinstance(body, str) else str(body)
            return {
                "success": False,
                "status": status,
                "error": error_text[:500],
            }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.get("/api/google-oauth-url")
def api_google_oauth_url(state: str = ""):
    """Return the Google OAuth URL for KGen registration."""
    import urllib.parse
    params = urllib.parse.urlencode({
        "access_type": "offline",
        "client_id": KGEN_GOOGLE_CLIENT_ID,
        "include_granted_scopes": "true",
        "prompt": "consent",
        "redirect_uri": KGEN_REDIRECT_URI,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile",
        "state": state or "https://engage.kgen.io/?provider=GOOGLE&platform=WEB&emailOptIn=true",
    })
    return {"url": f"https://accounts.google.com/o/oauth2/v2/auth?{params}"}


@app.post("/api/verify-token")
def api_verify_token(req: AddAccountRequest):
    """Verify a KGen bearer token via real browser (bypasses Cloudflare)."""
    try:
        result = kgen_browser_fetch(
            f"{KGEN_BASE}/users/me/profile",
            method="GET",
            bearer=req.bearer,
        )
        if result and result.get("ok"):
            data = result.get("body", {})
            if isinstance(data, dict):
                return {
                    "valid": True,
                    "username": data.get("username", data.get("displayName", "?")),
                    "email": data.get("email", data.get("emailId", "?")),
                    "id": data.get("id", "?"),
                }
        return {"valid": False, "status": result.get("status", 0) if result else 0}
    except Exception as e:
        return {"valid": False, "error": str(e)}


@app.post("/api/refresh-token")
def api_refresh_token_endpoint(refresh_token: str):
    """Refresh a KGen token via real browser (bypasses Cloudflare)."""
    try:
        result = kgen_browser_fetch(
            f"{KGEN_BASE}/authentication/token/refresh?refresh_token={refresh_token}&source=website",
            method="GET",
        )
        if result and result.get("ok"):
            body = result.get("body", {})
            if isinstance(body, dict):
                return {"success": True, **body}
        return {"success": False, "status": result.get("status", 0) if result else 0}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Auth Routes ──

@app.post("/api/auth/register")
def register(user: UserRegister, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    hashed_password = get_password_hash(user.password)
    new_user = User(username=user.username, password_hash=hashed_password)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    access_token = create_access_token(data={"sub": new_user.username})
    return {"access_token": access_token, "token_type": "bearer", "username": new_user.username}


@app.post("/api/auth/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer", "username": user.username}


@app.get("/api/auth/me")
def get_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "id": current_user.id}


# ── API Routes (Protected) ──

@app.get("/api/accounts")
def api_list_accounts(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    accounts = db.query(KGenAccount).filter(KGenAccount.user_id == current_user.id).all()
    result = []
    for acc in accounts:
        bearer = acc.bearer_token
        result.append({
            "id": acc.id,
            "bearer_short": f"...{bearer[-15:]}" if len(bearer) > 15 else bearer,
            "has_refresh": bool(acc.refresh_token),
            "valid": bool(acc.is_valid) if acc.is_valid is not None else None,
            "username": acc.username,
            "points": acc.points,
            "twitter": bool(acc.twitter),
            "discord": bool(acc.discord),
        })
    return {"accounts": result, "total": len(result)}


@app.post("/api/accounts/{acc_id}/check")
def api_check_account(acc_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    acc = db.query(KGenAccount).filter(KGenAccount.id == acc_id, KGenAccount.user_id == current_user.id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
        
    try:
        from kgen_api_lib import quick_check_social
        valid = is_token_valid(acc.bearer_token)
        acc.is_valid = 1 if valid else 0
        if valid:
            profile = get_profile(acc.bearer_token)
            bal = get_game_balance(acc.bearer_token)
            socials = quick_check_social([acc.bearer_token])
            
            acc.username = profile.get("username", "?")
            acc.points = bal.get("k_point", 0)
            acc.twitter = 1 if socials.get("TWITTER") else 0
            acc.discord = 1 if socials.get("DISCORD") else 0
            
            db.commit()
            return {
                "id": acc.id,
                "valid": True,
                "username": acc.username,
                "points": acc.points,
                "twitter": bool(acc.twitter),
                "discord": bool(acc.discord),
            }
        else:
            db.commit()
            return {"id": acc.id, "valid": False}
    except Exception as e:
        return {"id": acc.id, "valid": False, "error": str(e)}


@app.post("/api/accounts")
def api_add_account(req: AddAccountRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    new_acc = KGenAccount(
        user_id=current_user.id,
        bearer_token=req.bearer,
        refresh_token=req.refresh_token
    )
    db.add(new_acc)
    db.commit()
    return {"ok": True}


@app.delete("/api/accounts/{acc_id}")
def api_delete_account(acc_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    acc = db.query(KGenAccount).filter(KGenAccount.id == acc_id, KGenAccount.user_id == current_user.id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Not found")
        
    db.delete(acc)
    db.commit()
    return {"ok": True}


@app.get("/api/status")
def api_status(current_user: User = Depends(get_current_user)):
    return bot_state.get(current_user.id)


def _get_bearers_for_run(db: Session, user_id: int, account_ids: List[int]) -> List[str]:
    accounts = db.query(KGenAccount).filter(KGenAccount.user_id == user_id, KGenAccount.id.in_(account_ids)).all()
    # Sort accounts to maintain order matching account_ids if possible
    acc_map = {acc.id: acc.bearer_token for acc in accounts}
    return [acc_map[aid] for aid in account_ids if aid in acc_map]


@app.post("/api/run/campaigns")
def api_run_campaigns(req: RunRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    u_state = bot_state.get(current_user.id)
    if u_state["running"]:
        return {"error": "Bot is already running for your account"}
        
    bearers = _get_bearers_for_run(db, current_user.id, req.account_ids)
    if not bearers:
        return {"error": "No valid accounts found"}
        
    threading.Thread(target=run_campaigns_worker, args=(current_user.id, bearers), daemon=True).start()
    return {"ok": True, "action": "campaigns"}


@app.post("/api/run/spin")
def api_run_spin(req: SpinRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    u_state = bot_state.get(current_user.id)
    if u_state["running"]:
        return {"error": "Bot is already running"}
        
    bearers = _get_bearers_for_run(db, current_user.id, req.account_ids)
    threading.Thread(target=run_spin_worker, args=(current_user.id, bearers, req.bet, req.max_spins), daemon=True).start()
    return {"ok": True, "action": "spin"}


@app.post("/api/run/swap")
def api_run_swap(req: RunRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    u_state = bot_state.get(current_user.id)
    if u_state["running"]:
        return {"error": "Bot is already running"}
        
    bearers = _get_bearers_for_run(db, current_user.id, req.account_ids)
    threading.Thread(target=run_swap_worker, args=(current_user.id, bearers), daemon=True).start()
    return {"ok": True, "action": "swap"}


@app.post("/api/run/check")
def api_run_check(req: RunRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    u_state = bot_state.get(current_user.id)
    if u_state["running"]:
        return {"error": "Bot is already running"}
        
    bearers = _get_bearers_for_run(db, current_user.id, req.account_ids)
    threading.Thread(target=check_points_worker, args=(current_user.id, bearers), daemon=True).start()
    return {"ok": True, "action": "check"}


# ── WebSocket ──

@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket, token: str = Query(...)):
    db = next(get_db())
    user = await get_ws_current_user(token, db)
    if not user:
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return
        
    await log_mgr.connect(ws, user.id)
    try:
        while True:
            await ws.receive_text()  # keep alive
    except WebSocketDisconnect:
        log_mgr.disconnect(ws, user.id)


# ── Serve frontend ──

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
if os.path.exists(WEB_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(WEB_DIR, "assets") if os.path.exists(os.path.join(WEB_DIR, "assets")) else WEB_DIR), name="assets")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(WEB_DIR, "index.html"))
    
    @app.get("/{path:path}")
    def serve_static(path: str):
        fp = os.path.join(WEB_DIR, path)
        if os.path.exists(fp) and os.path.isfile(fp):
            return FileResponse(fp)
        return FileResponse(os.path.join(WEB_DIR, "index.html"))


# ── Main ──

if __name__ == "__main__":
    import uvicorn
    # Start ngrok
    try:
        from pyngrok import ngrok
        print("\n  [*] Starting Ngrok Tunnel...")
        public_url = ngrok.connect(8000).public_url
        print(f"  [*] KGenFisher Public URL: {public_url}\n")
    except Exception as e:
        print(f"  [-] Failed to start ngrok: {e}")
        print("\n  [*] KGenFisher Local URL: http://localhost:8000\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
