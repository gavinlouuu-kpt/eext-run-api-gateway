/**
 * EasyEDA API Gateway 扩展
 *
 * 为 AI 编程工具（Claude Code、OpenCode、QwenCode 等）提供 WebSocket 桥接服务。
 * 扩展启动后自动扫描端口范围 49620-49629，发现 Bridge Server 并建立连接。
 *
 * ChatGPT 凭据来源（按优先级）：
 *   1. Chrome 扩展注入的 window.__easyedaGatewayToken（安装了 EasyEDA AI Gateway 插件时）
 *   2. 本扩展自身存储的凭据（通过设备码 OAuth 登录）
 */
import * as extensionConfig from '../extension.json';

// ─── 配置 ───────────────────────────────────────────────────────────────────
const WS_ID = 'ai-bridge';
const OPENAI_AUTH_BASE_URL = 'https://auth.openai.com';
const OPENAI_CODEX_CLIENT_ID = 'app_EMoamEEZ73f0CkXaXp7hrann';
const OPENAI_DEVICE_CODE_TIMEOUT_MS = 15 * 60_000;
const OPENAI_DEVICE_CODE_DEFAULT_INTERVAL_MS = 5_000;
const OPENAI_DEVICE_CODE_MIN_INTERVAL_MS = 1_000;
const OPENAI_DEVICE_CALLBACK_URL = `${OPENAI_AUTH_BASE_URL}/deviceauth/callback`;

const STORAGE_KEY_CHATGPT_ACCESS = 'chatgptAccessToken';
const STORAGE_KEY_CHATGPT_REFRESH = 'chatgptRefreshToken';
const STORAGE_KEY_CHATGPT_EXPIRES = 'chatgptExpiresAt';
const STORAGE_KEY_CHATGPT_EMAIL = 'chatgptEmail';
const STORAGE_KEY_CHATGPT_PLAN = 'chatgptPlanType';
const PORT_START = 49620;
const PORT_END = 49629;
const SERVICE_ID = 'easyeda-bridge';
const RETRY_DELAY_MS = 3000;
const MAX_RETRIES = 5;
const HEARTBEAT_INTERVAL_MS = 15000;
const HEARTBEAT_TIMEOUT_MS = 5000;
const CONNECTION_TIMEOUT_MS = 1500;
const STORAGE_KEY_AUTO_CONNECT = 'autoConnectEnabled';
const MBUS_TOPIC_STATUS = 'api-gateway-status';
const MBUS_TOPIC_CONTROL = 'api-gateway-control';

// Window variable / event set by the EasyEDA AI Gateway Chrome extension
const CHROME_EXT_TOKEN_KEY = '__easyedaGatewayToken';
const CHROME_EXT_TOKEN_EVENT = '__easyedaGatewayTokenUpdated';

// ─── 状态 ───────────────────────────────────────────────────────────────────
let currentPort: number | null = null;
let handshakeVerified = false;
let retryTimer: ReturnType<typeof setTimeout> | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let heartbeatPending = false;
let autoConnectEnabled = true;
let retryCount = 0;
let windowId: string | null = null;
let isConnecting = false;
let connectionSessionId = 0;
let messageBusRegistered = false;
let chromeExtTokenListener: ((event: Event) => void) | null = null;

interface ChatGPTCredentials {
	access: string;
	refresh: string;
	expiresAt: number;
	email?: string;
	planType?: string;
}

interface GatewayControlRequest {
	command: 'reconnect' | 'stop';
}

interface GatewayControlResponse {
	handled: boolean;
	connected: boolean;
	windowId: string | null;
}

// ─── Chrome Extension Token Bridge ────────────────────────────────────────────

/**
 * Returns the ChatGPT token injected by the EasyEDA AI Gateway Chrome extension,
 * or null if the Chrome extension is not installed / token not yet injected.
 */
function readChromeExtensionToken(): string | null {
	const token = (window as Record<string, unknown>)[CHROME_EXT_TOKEN_KEY];
	return typeof token === 'string' && token ? token : null;
}

/**
 * Returns the best available ChatGPT token:
 * Chrome extension token takes priority; falls back to locally stored credentials.
 */
function getEffectiveChatGPTToken(): string | undefined {
	const chromeToken = readChromeExtensionToken();
	if (chromeToken) return chromeToken;
	const creds = loadStoredCredentials();
	if (creds && !isTokenExpired(creds)) return creds.access;
	return undefined;
}

/**
 * Registers a window-level listener for token updates pushed by the Chrome extension.
 * Called once during activate(). Cleans up on deactivate().
 */
function registerChromeExtTokenListener(): void {
	if (chromeExtTokenListener) return;
	chromeExtTokenListener = (event: Event) => {
		const ce = event as CustomEvent<{ token: string | null }>;
		const token = ce.detail?.token;
		if (typeof token === 'string' && token && handshakeVerified && windowId) {
			// Notify the bridge server of the updated token without reconnecting
			try {
				eda.sys_WebSocket.send(WS_ID, JSON.stringify({
					type: 'token_update',
					windowId,
					chatgptToken: token,
					timestamp: Date.now(),
				}));
			}
			catch { /* ignore if WebSocket not ready */ }
		}
	};
	window.addEventListener(CHROME_EXT_TOKEN_EVENT, chromeExtTokenListener);
}

function unregisterChromeExtTokenListener(): void {
	if (chromeExtTokenListener) {
		window.removeEventListener(CHROME_EXT_TOKEN_EVENT, chromeExtTokenListener);
		chromeExtTokenListener = null;
	}
}

// ─── MessageBus ───────────────────────────────────────────────────────────────

function getConnectionStatus(): {
	connected: boolean;
	connecting: boolean;
	port: number | null;
	windowId: string | null;
} {
	return { connected: handshakeVerified, connecting: isConnecting, port: currentPort, windowId };
}

function ensureMessageBusServices(): void {
	if (messageBusRegistered)
		return;

	eda.sys_MessageBus.rpcService(MBUS_TOPIC_STATUS, () => getConnectionStatus());
	eda.sys_MessageBus.rpcService(MBUS_TOPIC_CONTROL, (request?: GatewayControlRequest): GatewayControlResponse => {
		if (request?.command === 'reconnect') {
			performReconnect();
		}
		else if (request?.command === 'stop') {
			performStopConnection(false);
		}
		return { handled: true, connected: handshakeVerified, windowId };
	});

	messageBusRegistered = true;
}

function nextConnectionSessionId(): number {
	connectionSessionId += 1;
	return connectionSessionId;
}

function isConnectionSessionActive(sessionId: number): boolean {
	return sessionId === connectionSessionId;
}

function closeWebSocket(): void {
	try { eda.sys_WebSocket.close(WS_ID); }
	catch { /* ignore */ }
}

function cancelConnectionFlow(resetRetryCount = true): void {
	nextConnectionSessionId();
	isConnecting = false;
	clearRetryTimer();
	stopHeartbeat();
	handshakeVerified = false;
	currentPort = null;
	windowId = null;
	if (resetRetryCount) retryCount = 0;
	closeWebSocket();
}

function performReconnect(): void {
	eda.sys_Message.showToastMessage(eda.sys_I18n.text('Reconnecting...'));
	cancelConnectionFlow();
	void scanAndConnect();
}

function performStopConnection(showToast = true): void {
	cancelConnectionFlow();
	if (showToast)
		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Connection stopped'));
}

async function dispatchControlCommand(command: GatewayControlRequest['command']): Promise<void> {
	try {
		const response = await eda.sys_MessageBus.rpcCall(MBUS_TOPIC_CONTROL, { command }, 500) as GatewayControlResponse;
		if (response?.handled) {
			if (command === 'stop')
				eda.sys_Message.showToastMessage(eda.sys_I18n.text('Connection stopped'));
			return;
		}
	}
	catch {}

	ensureMessageBusServices();
	if (command === 'reconnect') performReconnect();
	else performStopConnection();
}

// ─── ChatGPT OAuth ───────────────────────────────────────────────────────────

function decodeBase64Url(str: string): string {
	const padded = str.replace(/-/g, '+').replace(/_/g, '/');
	const rem = padded.length % 4;
	const b64 = rem ? padded + '='.repeat(4 - rem) : padded;
	return atob(b64);
}

function decodeJwtPayload(token: string): Record<string, unknown> | null {
	const parts = token.split('.');
	if (parts.length !== 3) return null;
	try {
		const raw = decodeBase64Url(parts[1]);
		const parsed = JSON.parse(raw);
		return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
	}
	catch { return null; }
}

function resolveAuthIdentity(accessToken: string): { email?: string; planType?: string } {
	const payload = decodeJwtPayload(accessToken);
	if (!payload) return {};
	const profile = payload['https://api.openai.com/profile'];
	const auth = payload['https://api.openai.com/auth'];
	const email
		= profile && typeof profile === 'object' && 'email' in profile && typeof (profile as Record<string, unknown>).email === 'string'
			? ((profile as Record<string, unknown>).email as string)
			: undefined;
	const planType
		= auth && typeof auth === 'object' && 'chatgpt_plan_type' in auth && typeof (auth as Record<string, unknown>).chatgpt_plan_type === 'string'
			? ((auth as Record<string, unknown>).chatgpt_plan_type as string)
			: undefined;
	return { email, planType };
}

function resolveTokenExpiry(accessToken: string): number | undefined {
	const payload = decodeJwtPayload(accessToken);
	const exp = payload?.exp;
	if (typeof exp === 'number' && Number.isFinite(exp) && exp > 0)
		return Math.trunc(exp) * 1000;
	return undefined;
}

function loadStoredCredentials(): ChatGPTCredentials | null {
	const access = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_CHATGPT_ACCESS);
	const refresh = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_CHATGPT_REFRESH);
	const expiresAt = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_CHATGPT_EXPIRES);
	if (typeof access !== 'string' || typeof refresh !== 'string' || typeof expiresAt !== 'number')
		return null;
	const email = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_CHATGPT_EMAIL);
	const planType = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_CHATGPT_PLAN);
	return {
		access,
		refresh,
		expiresAt,
		email: typeof email === 'string' ? email : undefined,
		planType: typeof planType === 'string' ? planType : undefined,
	};
}

function saveCredentials(creds: ChatGPTCredentials): void {
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_ACCESS, creds.access);
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_REFRESH, creds.refresh);
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_EXPIRES, creds.expiresAt);
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_EMAIL, creds.email ?? '');
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_PLAN, creds.planType ?? '');
}

function clearCredentials(): void {
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_ACCESS, '');
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_REFRESH, '');
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_EXPIRES, 0);
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_EMAIL, '');
	void eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_CHATGPT_PLAN, '');
}

function isTokenExpired(creds: ChatGPTCredentials): boolean {
	return creds.expiresAt < Date.now() + 60_000;
}

function chatgptRequestHeaders(): Record<string, string> {
	return {
		'Content-Type': 'application/json',
		'originator': 'eext-run-api-gateway',
		'version': extensionConfig.version,
		'User-Agent': `eext-run-api-gateway/${extensionConfig.version}`,
	};
}

function parseJsonObject(text: string): Record<string, unknown> | null {
	try {
		const parsed = JSON.parse(text);
		return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
	}
	catch { return null; }
}

function trimString(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function normalizePositiveMs(value: unknown): number | undefined {
	if (typeof value === 'number' && Number.isFinite(value) && value > 0)
		return Math.trunc(value * 1000);
	if (typeof value === 'string' && /^\d+$/.test(value.trim())) {
		const s = Number.parseInt(value.trim(), 10);
		return s > 0 ? s * 1000 : undefined;
	}
	return undefined;
}

async function requestDeviceCode(): Promise<{
	deviceAuthId: string;
	userCode: string;
	intervalMs: number;
}> {
	const response = await fetch(`${OPENAI_AUTH_BASE_URL}/api/accounts/deviceauth/usercode`, {
		method: 'POST',
		headers: chatgptRequestHeaders(),
		body: JSON.stringify({ client_id: OPENAI_CODEX_CLIENT_ID }),
	});
	const bodyText = await response.text();
	if (!response.ok)
		throw new Error(`Device code request failed (HTTP ${response.status}): ${bodyText.slice(0, 200)}`);
	const body = parseJsonObject(bodyText);
	const deviceAuthId = trimString(body?.device_auth_id);
	const userCode = trimString(body?.user_code) ?? trimString(body?.usercode);
	if (!deviceAuthId || !userCode)
		throw new Error('Device code response missing device_auth_id or user_code.');
	return {
		deviceAuthId,
		userCode,
		intervalMs: normalizePositiveMs(body?.interval) ?? OPENAI_DEVICE_CODE_DEFAULT_INTERVAL_MS,
	};
}

function pollDeviceCode(params: {
	deviceAuthId: string;
	userCode: string;
	intervalMs: number;
}): Promise<{ authorizationCode: string; codeVerifier: string }> {
	return new Promise((resolve, reject) => {
		const deadline = Date.now() + OPENAI_DEVICE_CODE_TIMEOUT_MS;

		const attempt = async () => {
			if (Date.now() >= deadline) {
				reject(new Error('ChatGPT device authorization timed out after 15 minutes.'));
				return;
			}
			try {
				const response = await fetch(`${OPENAI_AUTH_BASE_URL}/api/accounts/deviceauth/token`, {
					method: 'POST',
					headers: chatgptRequestHeaders(),
					body: JSON.stringify({ device_auth_id: params.deviceAuthId, user_code: params.userCode }),
				});
				const bodyText = await response.text();
				if (response.ok) {
					const body = parseJsonObject(bodyText);
					const authorizationCode = trimString(body?.authorization_code);
					const codeVerifier = trimString(body?.code_verifier);
					if (!authorizationCode || !codeVerifier) {
						reject(new Error('Device authorization response missing exchange code.'));
						return;
					}
					resolve({ authorizationCode, codeVerifier });
					return;
				}
				if (response.status === 403 || response.status === 404) {
					const remaining = Math.max(0, deadline - Date.now());
					const delay = Math.min(Math.max(params.intervalMs, OPENAI_DEVICE_CODE_MIN_INTERVAL_MS), remaining);
					setTimeout(attempt, delay);
					return;
				}
				reject(new Error(`Device authorization failed (HTTP ${response.status}): ${bodyText.slice(0, 200)}`));
			}
			catch (err) { reject(err); }
		};
		void attempt();
	});
}

async function exchangeDeviceCode(params: {
	authorizationCode: string;
	codeVerifier: string;
}): Promise<ChatGPTCredentials> {
	const body = new URLSearchParams({
		grant_type: 'authorization_code',
		code: params.authorizationCode,
		redirect_uri: OPENAI_DEVICE_CALLBACK_URL,
		client_id: OPENAI_CODEX_CLIENT_ID,
		code_verifier: params.codeVerifier,
	});
	const response = await fetch(`${OPENAI_AUTH_BASE_URL}/oauth/token`, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded',
			'originator': 'eext-run-api-gateway',
			'version': extensionConfig.version,
			'User-Agent': `eext-run-api-gateway/${extensionConfig.version}`,
		},
		body,
	});
	const bodyText = await response.text();
	if (!response.ok)
		throw new Error(`Token exchange failed (HTTP ${response.status}): ${bodyText.slice(0, 200)}`);
	const payload = parseJsonObject(bodyText);
	const access = trimString(payload?.access_token);
	const refresh = trimString(payload?.refresh_token);
	if (!access || !refresh)
		throw new Error('Token exchange succeeded but did not return OAuth tokens.');
	const expiresInMs
		= typeof payload?.expires_in === 'number' && payload.expires_in > 0
			? Math.trunc(payload.expires_in as number) * 1000
			: undefined;
	const expiresAt = expiresInMs ? Date.now() + expiresInMs : (resolveTokenExpiry(access) ?? Date.now());
	const identity = resolveAuthIdentity(access);
	return { access, refresh, expiresAt, ...identity };
}

async function refreshChatGPTToken(refreshToken: string): Promise<ChatGPTCredentials | null> {
	try {
		const body = new URLSearchParams({
			grant_type: 'refresh_token',
			refresh_token: refreshToken,
			client_id: OPENAI_CODEX_CLIENT_ID,
		});
		const response = await fetch(`${OPENAI_AUTH_BASE_URL}/oauth/token`, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'originator': 'eext-run-api-gateway',
				'version': extensionConfig.version,
				'User-Agent': `eext-run-api-gateway/${extensionConfig.version}`,
			},
			body,
		});
		if (!response.ok) return null;
		const payload = parseJsonObject(await response.text());
		const access = trimString(payload?.access_token);
		const refresh = trimString(payload?.refresh_token) ?? refreshToken;
		if (!access) return null;
		const expiresInMs
			= typeof payload?.expires_in === 'number' && payload.expires_in > 0
				? Math.trunc(payload.expires_in as number) * 1000
				: undefined;
		const expiresAt = expiresInMs ? Date.now() + expiresInMs : (resolveTokenExpiry(access) ?? Date.now());
		const identity = resolveAuthIdentity(access);
		return { access, refresh, expiresAt, ...identity };
	}
	catch { return null; }
}

// ─── 生命周期 ────────────────────────────────────────────────────────────────

// eslint-disable-next-line unused-imports/no-unused-vars
export function activate(status?: 'onStartupFinished', arg?: string): void {
	ensureMessageBusServices();
	registerChromeExtTokenListener();

	const storedValue = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT);
	autoConnectEnabled = storedValue !== false;

	// Only refresh stored (local) credentials if Chrome extension is not providing a token
	if (!readChromeExtensionToken()) {
		const creds = loadStoredCredentials();
		if (creds && isTokenExpired(creds)) {
			void refreshChatGPTToken(creds.refresh).then((refreshed) => {
				if (refreshed) saveCredentials(refreshed);
			});
		}
	}

	if (autoConnectEnabled) {
		void scanAndConnect();
	}
}

export function deactivate(): void {
	cancelConnectionFlow(false);
	unregisterChromeExtTokenListener();
}

// ─── 菜单操作 ────────────────────────────────────────────────────────────────

export function reconnect(): void {
	void dispatchControlCommand('reconnect');
}

export async function about(): Promise<void> {
	let status: string;
	let statusInfo = { connected: false, connecting: false, port: 0, windowId: null };
	try {
		statusInfo = await eda.sys_MessageBus.rpcCall(MBUS_TOPIC_STATUS, undefined, 300);
	}
	// eslint-disable-next-line unused-imports/no-unused-vars
	catch (e) {}

	if (statusInfo?.connected) {
		const portInfo = `Connected (port ${statusInfo.port})`;
		const windowInfo = statusInfo.windowId ? `\nWindow ID: ${statusInfo.windowId}` : '\nWindow ID: (not registered)';
		status = `${portInfo}${windowInfo}`;
	}
	else if (statusInfo?.connecting) {
		status = 'Connecting...';
	}
	else {
		status = 'Disconnected';
	}

	eda.sys_Dialog.showInformationMessage(
		`API Gateway v${extensionConfig.version}\n${status}`,
		'About',
	);
}

export async function toggleAutoConnect(): Promise<void> {
	const current = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT);
	const newValue = current !== false;
	await eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT, !newValue);
	const msgKey = !newValue ? 'Auto-Connect enabled' : 'Auto-Connect disabled';
	eda.sys_Message.showToastMessage(eda.sys_I18n.text(msgKey));
}

export function stopConnection(): void {
	void dispatchControlCommand('stop');
}

// ─── ChatGPT 菜单操作 ───────────────────────────────────────────────────────────

export async function loginWithChatGPT(): Promise<void> {
	// If the Chrome extension is providing a token, direct the user there instead
	if (readChromeExtensionToken()) {
		eda.sys_Dialog.showInformationMessage(
			'ChatGPT credentials are being provided by the EasyEDA AI Gateway Chrome extension.\n\nManage your login from the extension icon in the browser toolbar.',
			'ChatGPT Login',
		);
		return;
	}

	const existing = loadStoredCredentials();
	if (existing && !isTokenExpired(existing)) {
		const identity = existing.email ?? existing.planType ?? 'unknown';
		eda.sys_Dialog.showInformationMessage(
			`Already logged in as ${identity}.\nLogout first via "Logout ChatGPT" if you want to re-authenticate.`,
			'ChatGPT Login',
		);
		return;
	}

	try {
		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Requesting ChatGPT device code...'));
		const deviceCode = await requestDeviceCode();

		eda.sys_Dialog.showInformationMessage(
			`To authorize ChatGPT:\n\n1. Open your browser and visit:\n   https://auth.openai.com/codex/device\n\n2. Enter code:\n   ${deviceCode.userCode}\n\nClick OK after completing sign-in in your browser.`,
			'ChatGPT Device Login',
		);

		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Waiting for ChatGPT authorization...'));
		const authorization = await pollDeviceCode({
			deviceAuthId: deviceCode.deviceAuthId,
			userCode: deviceCode.userCode,
			intervalMs: deviceCode.intervalMs,
		});

		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Exchanging ChatGPT token...'));
		const creds = await exchangeDeviceCode(authorization);
		saveCredentials(creds);

		const planLabel = creds.planType ? ` (Plan: ${creds.planType})` : '';
		const emailLabel = creds.email ? ` — ${creds.email}` : '';
		eda.sys_Message.showToastMessage(
			`${eda.sys_I18n.text('ChatGPT login successful')}${planLabel}${emailLabel}`,
		);
	}
	catch (err: unknown) {
		const message = err instanceof Error ? err.message : String(err);
		eda.sys_Dialog.showInformationMessage(`ChatGPT login failed:\n${message}`, 'ChatGPT Login Error');
	}
}

export function logoutChatGPT(): void {
	clearCredentials();
	eda.sys_Message.showToastMessage(eda.sys_I18n.text('ChatGPT logged out'));
}

export function chatGPTStatus(): void {
	// Check Chrome extension token first
	const chromeToken = readChromeExtensionToken();
	if (chromeToken) {
		eda.sys_Dialog.showInformationMessage(
			'Token source: EasyEDA AI Gateway Chrome extension\nStatus: Active\n\nManage credentials via the Chrome extension icon in the browser toolbar.',
			'ChatGPT Status',
		);
		return;
	}

	const creds = loadStoredCredentials();
	if (!creds || !creds.access) {
		eda.sys_Dialog.showInformationMessage('Not logged in to ChatGPT.', 'ChatGPT Status');
		return;
	}
	const expired = isTokenExpired(creds);
	const status = expired ? 'Expired' : 'Active';
	const expiry = new Date(creds.expiresAt).toLocaleString();
	const lines = [
		`Status: ${status}`,
		creds.email ? `Email: ${creds.email}` : '',
		creds.planType ? `Plan: ${creds.planType}` : '',
		`Expires: ${expiry}`,
	].filter(Boolean).join('\n');
	eda.sys_Dialog.showInformationMessage(lines, 'ChatGPT Status');
}

// ─── 端口扫描与连接 ────────────────────────────────────────────────────────────

async function scanAndConnect(): Promise<void> {
	if (isConnecting) return;

	const sessionId = nextConnectionSessionId();
	isConnecting = true;
	clearRetryTimer();

	try {
		if (retryCount >= MAX_RETRIES) {
			eda.sys_Message.showToastMessage(eda.sys_I18n.text('Max retries reached'), ESYS_ToastMessageType.ERROR);
			return;
		}

		for (let port = PORT_START; port <= PORT_END; port++) {
			if (!isConnectionSessionActive(sessionId)) return;
			const found = await tryConnectToPort(port, sessionId);
			if (!isConnectionSessionActive(sessionId)) return;
			if (found) {
				currentPort = port;
				retryCount = 0;
				startHeartbeat(sessionId);
				return;
			}
		}

		retryCount++;
		console.warn(`[API-Gateway] No bridge server found on ports ${PORT_START}-${PORT_END}, retrying in ${RETRY_DELAY_MS}ms...`);
		eda.sys_Message.showToastMessage(
			`${eda.sys_I18n.text('Bridge not found, retrying in ', undefined, undefined, String(RETRY_DELAY_MS / 1000))} (${retryCount}/${MAX_RETRIES})`,
		);
		scheduleRetry(sessionId);
	}
	finally {
		if (isConnectionSessionActive(sessionId)) isConnecting = false;
	}
}

function tryConnectToPort(port: number, sessionId: number): Promise<boolean> {
	return new Promise((resolve) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout>;

		const settle = (success: boolean, _reason: string) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			if (!success && isConnectionSessionActive(sessionId)) closeWebSocket();
			resolve(success);
		};

		if (!isConnectionSessionActive(sessionId)) { resolve(false); return; }

		closeWebSocket();
		timer = setTimeout(() => settle(false, 'timeout'), CONNECTION_TIMEOUT_MS);
		handshakeVerified = false;

		try {
			eda.sys_WebSocket.register(
				WS_ID,
				`ws://127.0.0.1:${port}/eda`,
				async (event: MessageEvent) => {
					if (!isConnectionSessionActive(sessionId)) { settle(false, 'session cancelled'); return; }
					try {
						const msg = JSON.parse(event.data);
						if (msg.type === 'handshake') {
							if (msg.service === SERVICE_ID) {
								handshakeVerified = true;
								windowId = crypto.randomUUID();
								// Prefer Chrome extension token over locally stored token
								const chatgptToken = getEffectiveChatGPTToken();
								eda.sys_WebSocket.send(WS_ID, JSON.stringify({
									type: 'register',
									windowId,
									timestamp: Date.now(),
									...(chatgptToken ? { chatgptToken } : {}),
								}));
								eda.sys_Message.showToastMessage(
									`${eda.sys_I18n.text('Bridge connected (port ', undefined, undefined, String(port))})`,
								);
								settle(true, 'handshake OK');
							}
							else {
								console.warn(`[API-Gateway] Handshake failed: unexpected service "${msg.service}"`);
								settle(false, `wrong service: ${msg.service}`);
							}
							return;
						}
						if (!handshakeVerified) return;
						await handleMessage(msg);
					}
					catch (err) { console.error('[API-Gateway] Failed to handle message:', err); }
				},
				() => {},
			);
		}
		catch (e) {
			console.error('[API-Gateway] Failed to register WebSocket:', e);
			settle(false, `register threw: ${e}`);
		}
	});
}

// ─── 心跳检测 ────────────────────────────────────────────────────────────────

function startHeartbeat(sessionId: number): void {
	stopHeartbeat();
	heartbeatTimer = setInterval(() => {
		if (!isConnectionSessionActive(sessionId)) { stopHeartbeat(); return; }
		if (!handshakeVerified) return;
		try {
			heartbeatPending = true;
			eda.sys_WebSocket.send(WS_ID, JSON.stringify({ type: 'ping', id: `hb-${Date.now()}`, timestamp: Date.now() }));
			setTimeout(() => {
				if (!isConnectionSessionActive(sessionId)) return;
				if (heartbeatPending) {
					console.warn('[API-Gateway] Heartbeat timeout, reconnecting...');
					cancelConnectionFlow();
					void scanAndConnect();
				}
			}, HEARTBEAT_TIMEOUT_MS);
		}
		catch {
			cancelConnectionFlow();
			void scanAndConnect();
		}
	}, HEARTBEAT_INTERVAL_MS);
}

function stopHeartbeat(): void {
	if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
	heartbeatPending = false;
}

// ─── 重试 ─────────────────────────────────────────────────────────────────────

function scheduleRetry(sessionId: number): void {
	clearRetryTimer();
	retryTimer = setTimeout(() => {
		if (!isConnectionSessionActive(sessionId) || isConnecting) return;
		void scanAndConnect();
	}, RETRY_DELAY_MS);
}

function clearRetryTimer(): void {
	if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
}

// ─── 消息处理 ────────────────────────────────────────────────────────────────

interface BridgeMessage {
	type: 'execute' | 'ping' | 'pong' | 'handshake' | 'result' | 'error';
	id?: string;
	code?: string;
	service?: string;
	result?: unknown;
	error?: string;
	timestamp?: number;
}

async function handleMessage(msg: BridgeMessage): Promise<void> {
	if (msg.type === 'ping') {
		eda.sys_WebSocket.send(WS_ID, JSON.stringify({ type: 'pong', id: msg.id, timestamp: Date.now() }));
		return;
	}
	if (msg.type === 'pong') {
		heartbeatPending = false;
		return;
	}
	if (msg.type === 'execute' && msg.code) {
		try {
			const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
			const fn = new AsyncFunction('eda', msg.code);
			const result = await fn(eda);
			eda.sys_WebSocket.send(WS_ID, JSON.stringify({
				type: 'result',
				id: msg.id,
				result: result !== undefined ? result : null,
				timestamp: Date.now(),
			}));
		}
		catch (err: unknown) {
			eda.sys_WebSocket.send(WS_ID, JSON.stringify({
				type: 'error',
				id: msg.id,
				error: err instanceof Error ? err.message : String(err),
				timestamp: Date.now(),
			}));
		}
	}
}
