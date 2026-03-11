// ── KGeN Dashboard — Frontend Logic ──

const API = window.location.origin;
const WS_URL = `ws://${window.location.host}/ws/logs`;

let accounts = [];
let ws = null;
let currentToken = localStorage.getItem('kgen_token');
let currentAction = 'login'; // login or register

// ── Init ──

document.addEventListener('DOMContentLoaded', async () => {
    if (currentToken) {
        const me = await api('/api/auth/me');
        if (me.username) {
            handleLoginSuccess(me.username);
        } else {
            showAuth();
        }
    } else {
        showAuth();
    }
});

// ── API helpers ──

async function api(path, method = 'GET', body = null) {
    const opts = {
        method,
        headers: { 'Content-Type': 'application/json' },
    };
    if (currentToken) {
        opts.headers['Authorization'] = `Bearer ${currentToken}`;
    }
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(`${API}${path}`, opts);
    return res.json();
}

// ── Auth Logic ──

function showAuth() {
    document.getElementById('authOverlay').style.display = 'flex';
    document.getElementById('btnLogout').style.display = 'none';
    document.getElementById('userBadge').style.display = 'none';
}

function switchAuthTab(action) {
    currentAction = action;
    document.getElementById('tabLogin').classList.toggle('active', action === 'login');
    document.getElementById('tabRegister').classList.toggle('active', action === 'register');
    document.getElementById('btnAuthSubmit').textContent = action === 'login' ? 'Login' : 'Register';
    document.getElementById('authError').style.display = 'none';
}

async function submitAuth() {
    const u = document.getElementById('authUsername').value.trim();
    const p = document.getElementById('authPassword').value;
    const err = document.getElementById('authError');
    err.style.display = 'none';

    if (!u || !p) {
        err.textContent = 'Username and password required';
        err.style.display = 'block';
        return;
    }

    try {
        let res;
        if (currentAction === 'register') {
            res = await api('/api/auth/register', 'POST', { username: u, password: p });
        } else {
            // OAuth2 requires form data
            const formData = new URLSearchParams();
            formData.append('username', u);
            formData.append('password', p);
            const rawRes = await fetch(`${API}/api/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: formData
            });
            res = await rawRes.json();
            if (!rawRes.ok) throw new Error(res.detail || 'Login failed');
        }

        if (res.access_token) {
            currentToken = res.access_token;
            localStorage.setItem('kgen_token', currentToken);
            handleLoginSuccess(res.username);
        } else if (res.detail || res.error) {
            throw new Error(res.detail || res.error);
        }
    } catch (e) {
        err.textContent = e.message;
        err.style.display = 'block';
    }
}

function handleLoginSuccess(username) {
    document.getElementById('authOverlay').style.display = 'none';
    document.getElementById('btnLogout').style.display = 'block';
    document.getElementById('userBadge').style.display = 'block';
    document.getElementById('userName').textContent = username;
    
    refreshAccounts();
    connectWebSocket();
    pollStatus();
}

function logout() {
    localStorage.removeItem('kgen_token');
    currentToken = null;
    if (ws) ws.close();
    accounts = [];
    document.getElementById('accountList').innerHTML = '';
    clearLogs();
    showAuth();
}

// ── Accounts ──

async function refreshAccounts() {
    const data = await api('/api/accounts');
    accounts = data.accounts || [];
    renderAccounts();
}

function renderAccounts() {
    const list = document.getElementById('accountList');
    if (!accounts.length) {
        list.innerHTML = '<div class="loading">No accounts found. Add one!</div>';
        return;
    }

    list.innerHTML = accounts.map((acc, i) => `
        <div class="account-card ${acc._selected !== false ? 'selected' : ''}" onclick="toggleAccount(${i})">
            <input type="checkbox" ${acc._selected !== false ? 'checked' : ''} 
                   onclick="event.stopPropagation(); toggleAccount(${i})" />
            <div class="account-info">
                <div class="account-name">
                    ${acc.username ? acc.username : `Account #${i + 1}`}
                    ${acc.valid === true ? '<span class="badge badge-valid">Active</span>' : 
                      acc.valid === false ? '<span class="badge badge-expired">Expired</span>' :
                      '<span class="badge badge-unknown">Unknown</span>'}
                    ${acc.has_refresh ? '<span class="badge badge-refresh">RT</span>' : ''}
                </div>
                <div class="account-meta">${acc.bearer_short}</div>
            </div>
            <div class="account-points" id="points-${i}">
                ${acc.points != null ? acc.points.toLocaleString() + ' KP' : '—'}
            </div>
        </div>
    `).join('');
}

function toggleAccount(i) {
    if (accounts[i]._selected === undefined) accounts[i]._selected = true;
    accounts[i]._selected = !accounts[i]._selected;
    renderAccounts();
    updateSelectAll();
}

function toggleSelectAll() {
    const checked = document.getElementById('selectAll').checked;
    accounts.forEach(a => a._selected = checked);
    renderAccounts();
}

function updateSelectAll() {
    const all = accounts.every(a => a._selected !== false);
    document.getElementById('selectAll').checked = all;
}

function getSelectedIndices() {
    return accounts
        .map((a, i) => a._selected !== false ? i : -1)
        .filter(i => i >= 0);
}

// ── Actions ──

async function runAction(action) {
    const indices = getSelectedIndices();
    if (!indices.length) {
        addLogEntry('Select at least one account!', 'warning');
        return;
    }

    const body = { indices };

    // Disable buttons
    setButtonsDisabled(true);
    addLogEntry(`Starting ${action} for ${indices.length} accounts...`, 'info');

    const data = await api(`/api/run/${action}`, 'POST', body);
    if (data.error) {
        addLogEntry(`Error: ${data.error}`, 'error');
        setButtonsDisabled(false);
    }
}

function setButtonsDisabled(disabled) {
    document.querySelectorAll('.action-bar .btn').forEach(b => b.disabled = disabled);
}

// ── Add/Delete Account ──

function showAddModal() {
    document.getElementById('addModal').style.display = 'flex';
    document.getElementById('inputBearer').focus();
}

function hideAddModal() {
    document.getElementById('addModal').style.display = 'none';
    document.getElementById('inputBearer').value = '';
    document.getElementById('inputRefresh').value = '';
}

async function addAccount() {
    const bearer = document.getElementById('inputBearer').value.trim();
    const refresh = document.getElementById('inputRefresh').value.trim();
    if (!bearer) return;

    await api('/api/accounts', 'POST', { bearer, refresh_token: refresh || null });
    hideAddModal();
    refreshAccounts();
    addLogEntry('Account added!', 'success');
}

// ── WebSocket Log Streaming ──

function connectWebSocket() {
    if (ws) ws.close();
    if (!currentToken) return;
    
    const wsUrlWithToken = `${WS_URL}?token=${currentToken}`;
    ws = new WebSocket(wsUrlWithToken);

    ws.onopen = () => {
        console.log('WebSocket connected');
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            addLogEntry(data.msg, data.level, data.account);
        } catch (e) {
            console.error('WS parse error', e);
        }
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting in 2s...');
        setTimeout(connectWebSocket, 2000);
    };

    ws.onerror = () => {
        ws.close();
    };
}

function addLogEntry(msg, level = 'info', account = '') {
    const viewer = document.getElementById('logViewer');
    const now = new Date();
    const time = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;

    const entry = document.createElement('div');
    entry.className = `log-entry log-${level}`;
    entry.innerHTML = `
        <span class="log-time">${time}</span>
        ${account ? `<span class="log-account">[${account}]</span>` : ''}
        <span class="log-msg">${escapeHtml(msg)}</span>
    `;

    viewer.appendChild(entry);
    viewer.scrollTop = viewer.scrollHeight;

    // Limit log entries to 500
    while (viewer.children.length > 500) {
        viewer.removeChild(viewer.firstChild);
    }
}

function clearLogs() {
    document.getElementById('logViewer').innerHTML = '';
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ── Status Polling ──

async function pollStatus() {
    try {
        const data = await api('/api/status');
        const badge = document.getElementById('statusBadge');
        const text = document.getElementById('statusText');
        const bar = document.getElementById('progressBar');

        if (data.running) {
            badge.classList.add('running');
            text.textContent = `${data.action} (${data.progress}/${data.total})`;
            setButtonsDisabled(true);

            bar.style.display = 'block';
            const pct = data.total > 0 ? (data.progress / data.total * 100) : 0;
            document.getElementById('progressFill').style.width = `${pct}%`;
            document.getElementById('progressText').textContent = `${data.action} — ${data.progress}/${data.total}`;
        } else {
            badge.classList.remove('running');
            text.textContent = 'Idle';
            setButtonsDisabled(false);
            bar.style.display = 'none';
        }
    } catch (e) {
        // server not reachable
    }

    setTimeout(pollStatus, 1500);
}
