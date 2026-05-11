import type { IncomingMessage, ServerResponse } from 'node:http';
import { Buffer } from 'node:buffer';
import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { createServer } from 'node:http';
import process from 'node:process';
import { URL } from 'node:url';

import WebSocket, { WebSocketServer } from 'ws';

const SERVICE_ID = 'easyeda-bridge';
const PORT_START = readIntegerEnv('BRIDGE_PORT_START', 49620);
const PORT_END = readIntegerEnv('BRIDGE_PORT_END', 49629);
const HOST = process.env.BRIDGE_HOST ?? '127.0.0.1';
const TOKEN_TTL_SECONDS = readIntegerEnv('GPT_OAUTH_TOKEN_TTL_SECONDS', 3600);
const EXECUTE_TIMEOUT_MS = readIntegerEnv('BRIDGE_EXECUTE_TIMEOUT_MS', 40_000);
const MAX_EXECUTE_TIMEOUT_MS = 44_000;
const MAX_BODY_BYTES = 100_000;

const GPT_OAUTH_CLIENT_ID = process.env.GPT_OAUTH_CLIENT_ID ?? 'easyeda-gpt';
const GPT_OAUTH_CLIENT_SECRET = process.env.GPT_OAUTH_CLIENT_SECRET ?? 'dev-secret-change-me';
const GPT_OAUTH_SCOPE = 'easyeda.execute';
const DEV_BEARER_TOKEN = process.env.GATEWAY_DEV_BEARER_TOKEN;

interface BridgeMessage {
	type?: string;
	id?: string;
	windowId?: string;
	code?: string;
	result?: unknown;
	error?: string;
	chatgptToken?: string;
	timestamp?: number;
}

interface BridgeClient {
	ws: WebSocket;
	windowId: string | null;
	connectedAt: number;
	lastSeenAt: number;
	chatgptToken?: string;
}

interface PendingExecution {
	resolve: (value: unknown) => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
	windowId: string;
}

interface AuthCode {
	clientId: string;
	redirectUri: string;
	scope: string;
	expiresAt: number;
}

interface AccessToken {
	refreshToken: string;
	scope: string;
	expiresAt: number;
}

interface RefreshToken {
	scope: string;
}

const bridgeClients = new Map<string, BridgeClient>();
const pendingExecutions = new Map<string, PendingExecution>();
const authCodes = new Map<string, AuthCode>();
const accessTokens = new Map<string, AccessToken>();
const refreshTokens = new Map<string, RefreshToken>();

function readIntegerEnv(name: string, fallback: number): number {
	const raw = process.env[name];
	if (!raw)
		return fallback;
	const parsed = Number.parseInt(raw, 10);
	return Number.isFinite(parsed) ? parsed : fallback;
}

function randomToken(): string {
	return randomBytes(32).toString('base64url');
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function getString(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function safeEqual(left: string, right: string): boolean {
	const leftBuffer = Buffer.from(left);
	const rightBuffer = Buffer.from(right);
	return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function sendJson(res: ServerResponse, status: number, body: Record<string, unknown>): void {
	res.writeHead(status, {
		'Content-Type': 'application/json; charset=utf-8',
		'Access-Control-Allow-Origin': '*',
		'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
		'Access-Control-Allow-Headers': 'authorization,content-type',
	});
	res.end(JSON.stringify(body));
}

function sendText(res: ServerResponse, status: number, body: string, contentType = 'text/plain; charset=utf-8'): void {
	res.writeHead(status, {
		'Content-Type': contentType,
		'Access-Control-Allow-Origin': '*',
		'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
		'Access-Control-Allow-Headers': 'authorization,content-type',
	});
	res.end(body);
}

function sendNoContent(res: ServerResponse): void {
	res.writeHead(204, {
		'Access-Control-Allow-Origin': '*',
		'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
		'Access-Control-Allow-Headers': 'authorization,content-type',
	});
	res.end();
}

function sendError(res: ServerResponse, status: number, message: string): void {
	sendJson(res, status, { error: message });
}

function readRequestBody(req: IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let body = '';
		req.setEncoding('utf8');
		req.on('data', (chunk: string) => {
			body += chunk;
			if (Buffer.byteLength(body) > MAX_BODY_BYTES) {
				reject(new Error('Request body is too large.'));
				req.destroy();
			}
		});
		req.on('end', () => resolve(body));
		req.on('error', reject);
	});
}

async function readStructuredBody(req: IncomingMessage): Promise<Record<string, unknown>> {
	const raw = await readRequestBody(req);
	if (!raw)
		return {};

	const contentType = req.headers['content-type'] ?? '';
	if (Array.isArray(contentType) ? contentType.some(item => item.includes('application/json')) : contentType.includes('application/json')) {
		const parsed = JSON.parse(raw);
		if (!isRecord(parsed))
			throw new Error('JSON body must be an object.');
		return parsed;
	}

	const params = new URLSearchParams(raw);
	const body: Record<string, string> = {};
	for (const [key, value] of params.entries()) {
		body[key] = value;
	}
	return body;
}

function requireBearer(req: IncomingMessage): string | null {
	const header = req.headers.authorization;
	if (!header)
		return null;
	const value = Array.isArray(header) ? header[0] : header;
	if (!value.toLowerCase().startsWith('bearer '))
		return null;
	return value.slice('bearer '.length).trim() || null;
}

function isBearerAuthorized(token: string): boolean {
	if (DEV_BEARER_TOKEN && safeEqual(token, DEV_BEARER_TOKEN))
		return true;

	const access = accessTokens.get(token);
	if (!access)
		return false;

	if (access.expiresAt <= Date.now()) {
		accessTokens.delete(token);
		return false;
	}

	return access.scope.split(' ').filter(Boolean).includes(GPT_OAUTH_SCOPE);
}

function requireAuthorized(req: IncomingMessage, res: ServerResponse): boolean {
	const token = requireBearer(req);
	if (!token || !isBearerAuthorized(token)) {
		sendError(res, 401, 'Missing or invalid bearer token.');
		return false;
	}
	return true;
}

function validateOAuthClient(body: Record<string, unknown>): boolean {
	const clientId = getString(body.client_id);
	const clientSecret = getString(body.client_secret);
	return clientId === GPT_OAUTH_CLIENT_ID
		&& Boolean(clientSecret)
		&& safeEqual(clientSecret ?? '', GPT_OAUTH_CLIENT_SECRET);
}

function issueOAuthTokens(scope: string): Record<string, unknown> {
	const accessToken = randomToken();
	const refreshToken = randomToken();
	const expiresAt = Date.now() + TOKEN_TTL_SECONDS * 1000;

	accessTokens.set(accessToken, { refreshToken, scope, expiresAt });
	refreshTokens.set(refreshToken, { scope });

	return {
		access_token: accessToken,
		token_type: 'bearer',
		refresh_token: refreshToken,
		expires_in: TOKEN_TTL_SECONDS,
		scope,
	};
}

function handleAuthorize(req: IncomingMessage, res: ServerResponse, url: URL): void {
	const responseType = url.searchParams.get('response_type');
	const clientId = url.searchParams.get('client_id');
	const redirectUri = url.searchParams.get('redirect_uri');
	const state = url.searchParams.get('state');
	const scope = url.searchParams.get('scope') || GPT_OAUTH_SCOPE;

	if (responseType !== 'code' || clientId !== GPT_OAUTH_CLIENT_ID || !redirectUri || !state) {
		sendError(res, 400, 'Invalid OAuth authorization request.');
		return;
	}

	const code = randomToken();
	authCodes.set(code, {
		clientId,
		redirectUri,
		scope,
		expiresAt: Date.now() + 5 * 60_000,
	});

	const callbackUrl = new URL(redirectUri);
	callbackUrl.searchParams.set('code', code);
	callbackUrl.searchParams.set('state', state);
	res.writeHead(302, { Location: callbackUrl.toString() });
	res.end();
}

async function handleToken(req: IncomingMessage, res: ServerResponse): Promise<void> {
	let body: Record<string, unknown>;
	try {
		body = await readStructuredBody(req);
	}
	catch (err: unknown) {
		sendError(res, 400, err instanceof Error ? err.message : String(err));
		return;
	}

	if (!validateOAuthClient(body)) {
		sendError(res, 401, 'Invalid OAuth client credentials.');
		return;
	}

	const grantType = getString(body.grant_type);
	if (grantType === 'authorization_code') {
		const code = getString(body.code);
		const redirectUri = getString(body.redirect_uri);
		const authCode = code ? authCodes.get(code) : undefined;
		if (!code || !redirectUri || !authCode || authCode.expiresAt <= Date.now() || authCode.redirectUri !== redirectUri) {
			sendError(res, 400, 'Invalid or expired authorization code.');
			return;
		}

		authCodes.delete(code);
		sendJson(res, 200, issueOAuthTokens(authCode.scope));
		return;
	}

	if (grantType === 'refresh_token') {
		const refreshToken = getString(body.refresh_token);
		const refresh = refreshToken ? refreshTokens.get(refreshToken) : undefined;
		if (!refreshToken || !refresh) {
			sendError(res, 400, 'Invalid refresh token.');
			return;
		}

		sendJson(res, 200, issueOAuthTokens(refresh.scope));
		return;
	}

	sendError(res, 400, 'Unsupported OAuth grant type.');
}

function handleBridgeMessage(client: BridgeClient, raw: WebSocket.RawData): void {
	let msg: BridgeMessage;
	try {
		const parsed = JSON.parse(raw.toString());
		if (!isRecord(parsed))
			return;
		msg = parsed;
	}
	catch {
		return;
	}

	client.lastSeenAt = Date.now();

	if (msg.type === 'register') {
		const windowId = getString(msg.windowId) ?? randomUUID();
		if (client.windowId && client.windowId !== windowId)
			bridgeClients.delete(client.windowId);
		client.windowId = windowId;
		client.chatgptToken = getString(msg.chatgptToken);
		bridgeClients.set(windowId, client);
		console.warn(`[bridge] registered EasyEDA window ${windowId}${client.chatgptToken ? ' with ChatGPT token' : ''}`);
		return;
	}

	if (msg.type === 'ping') {
		client.ws.send(JSON.stringify({ type: 'pong', id: msg.id, timestamp: Date.now() }));
		return;
	}

	if ((msg.type === 'result' || msg.type === 'error') && msg.id) {
		const pending = pendingExecutions.get(msg.id);
		if (!pending)
			return;

		clearTimeout(pending.timer);
		pendingExecutions.delete(msg.id);
		if (msg.type === 'error') {
			pending.reject(new Error(getString(msg.error) ?? 'EasyEDA execution failed.'));
		}
		else {
			pending.resolve(msg.result ?? null);
		}
	}
}

function selectBridgeClient(windowId?: string): BridgeClient | null {
	if (windowId)
		return bridgeClients.get(windowId) ?? null;

	const connected = [...bridgeClients.values()].filter(client => client.ws.readyState === WebSocket.OPEN);
	return connected.length === 1 ? connected[0] : null;
}

function executeInEasyEda(code: string, windowId: string | undefined, timeoutMs: number): Promise<unknown> {
	const client = selectBridgeClient(windowId);
	if (!client || !client.windowId || client.ws.readyState !== WebSocket.OPEN) {
		throw new Error(windowId ? `EasyEDA window ${windowId} is not connected.` : 'Expected exactly one connected EasyEDA window.');
	}

	const id = randomUUID();
	const boundedTimeoutMs = Math.min(Math.max(timeoutMs, 1_000), MAX_EXECUTE_TIMEOUT_MS);

	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => {
			pendingExecutions.delete(id);
			reject(new Error(`EasyEDA execution timed out after ${boundedTimeoutMs}ms.`));
		}, boundedTimeoutMs);

		pendingExecutions.set(id, { resolve, reject, timer, windowId: client.windowId ?? '' });
		client.ws.send(JSON.stringify({
			type: 'execute',
			id,
			code,
			timestamp: Date.now(),
		}));
	});
}

function listWindows(): Array<Record<string, unknown>> {
	return [...bridgeClients.values()].map(client => ({
		windowId: client.windowId,
		connected: client.ws.readyState === WebSocket.OPEN,
		connectedAt: new Date(client.connectedAt).toISOString(),
		lastSeenAt: new Date(client.lastSeenAt).toISOString(),
		hasChatGPTToken: Boolean(client.chatgptToken),
	}));
}

async function handleExecute(req: IncomingMessage, res: ServerResponse): Promise<void> {
	let body: Record<string, unknown>;
	try {
		body = await readStructuredBody(req);
	}
	catch (err: unknown) {
		sendError(res, 400, err instanceof Error ? err.message : String(err));
		return;
	}

	const code = getString(body.code);
	if (!code) {
		sendError(res, 400, 'Missing code.');
		return;
	}

	const timeoutMs = typeof body.timeout_ms === 'number'
		? body.timeout_ms
		: EXECUTE_TIMEOUT_MS;
	const windowId = getString(body.window_id);

	try {
		const result = await executeInEasyEda(code, windowId, timeoutMs);
		sendJson(res, 200, { ok: true, result });
	}
	catch (err: unknown) {
		sendError(res, 502, err instanceof Error ? err.message : String(err));
	}
}

function renderOpenApi(baseUrl: string): string {
	return `openapi: 3.1.0
info:
  title: EasyEDA Run API Gateway
  version: 0.1.0
  description: Execute read-only or confirmed EasyEDA API code through the local bridge.
servers:
  - url: ${baseUrl}
security:
  - gptOAuth:
      - ${GPT_OAUTH_SCOPE}
components:
  securitySchemes:
    gptOAuth:
      type: oauth2
      flows:
        authorizationCode:
          authorizationUrl: ${baseUrl}/oauth/authorize
          tokenUrl: ${baseUrl}/oauth/token
          scopes:
            ${GPT_OAUTH_SCOPE}: Execute EasyEDA API calls through the bridge.
  schemas:
    ExecuteRequest:
      type: object
      required:
        - code
      properties:
        code:
          type: string
          maxLength: 50000
          description: JavaScript code executed with an eda argument inside EasyEDA.
        window_id:
          type: string
          description: Optional EasyEDA window ID when multiple windows are connected.
        timeout_ms:
          type: integer
          minimum: 1000
          maximum: ${MAX_EXECUTE_TIMEOUT_MS}
          default: ${EXECUTE_TIMEOUT_MS}
paths:
  /health:
    get:
      operationId: health
      summary: Check whether the gateway is running.
      security: []
      responses:
        '200':
          description: Gateway health.
  /status:
    get:
      operationId: getStatus
      summary: Get bridge connection status.
      responses:
        '200':
          description: Current status.
  /windows:
    get:
      operationId: listWindows
      summary: List connected EasyEDA windows.
      responses:
        '200':
          description: Connected windows.
  /execute:
    post:
      operationId: executeEasyEdaCode
      summary: Execute JavaScript inside EasyEDA.
      x-openai-isConsequential: true
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ExecuteRequest'
      responses:
        '200':
          description: Execution result.
`;
}

function resolveBaseUrl(req: IncomingMessage, port: number): string {
	const forwardedProto = req.headers['x-forwarded-proto'];
	const forwardedHost = req.headers['x-forwarded-host'];
	const proto = Array.isArray(forwardedProto) ? forwardedProto[0] : forwardedProto;
	const host = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost;
	if (proto && host)
		return `${proto}://${host}`;

	return process.env.GPT_ACTION_BASE_URL?.replace(/\/+$/, '') ?? `http://localhost:${port}`;
}

function createRequestHandler(port: number): (req: IncomingMessage, res: ServerResponse) => void {
	return (req, res) => {
		void (async () => {
			const url = new URL(req.url ?? '/', `http://${req.headers.host ?? `localhost:${port}`}`);

			if (req.method === 'OPTIONS') {
				sendNoContent(res);
				return;
			}

			if (req.method === 'GET' && url.pathname === '/health') {
				sendJson(res, 200, {
					service: SERVICE_ID,
					port,
					windows: bridgeClients.size,
				});
				return;
			}

			if (req.method === 'GET' && url.pathname === '/openapi.yaml') {
				sendText(res, 200, renderOpenApi(resolveBaseUrl(req, port)), 'application/yaml; charset=utf-8');
				return;
			}

			if (req.method === 'GET' && url.pathname === '/oauth/authorize') {
				handleAuthorize(req, res, url);
				return;
			}

			if (req.method === 'POST' && url.pathname === '/oauth/token') {
				await handleToken(req, res);
				return;
			}

			if (req.method === 'GET' && url.pathname === '/status') {
				if (!requireAuthorized(req, res))
					return;
				sendJson(res, 200, {
					service: SERVICE_ID,
					port,
					windows: bridgeClients.size,
					pendingExecutions: pendingExecutions.size,
				});
				return;
			}

			if (req.method === 'GET' && url.pathname === '/windows') {
				if (!requireAuthorized(req, res))
					return;
				sendJson(res, 200, { windows: listWindows() });
				return;
			}

			if (req.method === 'POST' && url.pathname === '/execute') {
				if (!requireAuthorized(req, res))
					return;
				await handleExecute(req, res);
				return;
			}

			sendError(res, 404, 'Not found.');
		})().catch((err: unknown) => {
			sendError(res, 500, err instanceof Error ? err.message : String(err));
		});
	};
}

async function listenOnAvailablePort(): Promise<{ server: ReturnType<typeof createServer>; port: number }> {
	for (let port = PORT_START; port <= PORT_END; port++) {
		const server = createServer(createRequestHandler(port));
		const wss = new WebSocketServer({ server, path: '/eda' });

		wss.on('connection', (ws) => {
			const client: BridgeClient = {
				ws,
				windowId: null,
				connectedAt: Date.now(),
				lastSeenAt: Date.now(),
			};

			ws.send(JSON.stringify({
				type: 'handshake',
				service: SERVICE_ID,
				timestamp: Date.now(),
			}));

			ws.on('message', raw => handleBridgeMessage(client, raw));
			ws.on('close', () => {
				if (client.windowId)
					bridgeClients.delete(client.windowId);
			});
		});

		const started = await new Promise<boolean>((resolve) => {
			server.once('error', () => resolve(false));
			server.listen(port, HOST, () => resolve(true));
		});

		if (started)
			return { server, port };

		wss.close();
		server.close();
	}

	throw new Error(`No available port in ${PORT_START}-${PORT_END}.`);
}

async function main(): Promise<void> {
	const { port } = await listenOnAvailablePort();
	console.warn(`[bridge] ${SERVICE_ID} listening at http://${HOST}:${port}`);
	console.warn(`[bridge] OpenAPI spec: http://${HOST}:${port}/openapi.yaml`);
	if (GPT_OAUTH_CLIENT_SECRET === 'dev-secret-change-me') {
		console.warn('[bridge] Using default GPT_OAUTH_CLIENT_SECRET; set a private value before exposing this service.');
	}
}

void main().catch((err: unknown) => {
	console.error(err instanceof Error ? err.message : String(err));
	process.exit(1);
});
