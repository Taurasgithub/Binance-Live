import { i as runWithoutCurrentAgent, r as runInLifecycleHostContext } from "./current-agent-DhoDkSnH.js";
import { i as _classPrivateFieldInitSpec, n as _classPrivateFieldSet2, r as _assertClassBrand, t as _classPrivateFieldGet2 } from "./classPrivateFieldGet2-DZBYAB34.js";
import { n as publishDiagnosticsEvent } from "./diagnostics-CaBjfz4J.js";
import { n as bindLifecycleCapability, r as lifecycleCapabilityId, t as LifecycleCapability } from "./capability-BjSKYpzg.js";
import { t as _classPrivateMethodInitSpec } from "./classPrivateMethodInitSpec-qMjJ6sHQ.js";
import { nanoid } from "nanoid";
import "cloudflare:workers";
//#region src/lifecycle/capability-runner.ts
var _resolveCapabilities = /* @__PURE__ */ new WeakMap();
var _capabilities$1 = /* @__PURE__ */ new WeakMap();
var _startPromise = /* @__PURE__ */ new WeakMap();
var _started = /* @__PURE__ */ new WeakMap();
var _CapabilityRunner_brand = /* @__PURE__ */ new WeakSet();
/**
* Runs ordered lifecycle phases for capabilities installed in a Durable Object.
*
* Capabilities are resolved lazily on the first phase and retained for the
* lifetime of this runner. Startup and alarms run in declaration order, and
* requests stop at the first response.
*/
var CapabilityRunner = class {
	/**
	* Create a lifecycle whose capabilities are resolved immediately before the
	* first phase.
	*
	* @param resolveCapabilities - Returns capabilities in their startup order.
	*/
	constructor(resolveCapabilities) {
		_classPrivateMethodInitSpec(this, _CapabilityRunner_brand);
		_classPrivateFieldInitSpec(this, _resolveCapabilities, void 0);
		_classPrivateFieldInitSpec(this, _capabilities$1, void 0);
		_classPrivateFieldInitSpec(this, _startPromise, void 0);
		_classPrivateFieldInitSpec(this, _started, false);
		_classPrivateFieldSet2(_resolveCapabilities, this, resolveCapabilities);
	}
	/**
	* Start every capability sequentially.
	*
	* Concurrent callers share one startup attempt. A failed attempt is not
	* cached, allowing the host to retry its complete startup phase.
	*
	* @param context - Properties supplied while resolving the Durable Object.
	*/
	async start(context) {
		if (_classPrivateFieldGet2(_started, this)) return;
		const pending = _classPrivateFieldGet2(_startPromise, this);
		if (pending) {
			await pending;
			return;
		}
		const attempt = _assertClassBrand(_CapabilityRunner_brand, this, _runStart).call(this, context);
		_classPrivateFieldSet2(_startPromise, this, attempt);
		try {
			await attempt;
		} catch (error) {
			if (_classPrivateFieldGet2(_startPromise, this) === attempt) _classPrivateFieldSet2(_startPromise, this, void 0);
			throw error;
		}
	}
	/**
	* Offer a request to each capability in declaration order.
	*
	* @param context - The request entering the Durable Object.
	* @returns The first capability response, or `undefined` when unhandled.
	*/
	async request(context) {
		await _assertClassBrand(_CapabilityRunner_brand, this, _ensureReady).call(this, "handle a request");
		for (const capability of _assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this)) {
			const response = await capability.onRequest?.(context);
			if (response !== void 0) return response;
		}
	}
	/** Return alarm requests from every installed capability. */
	async getAlarmContributions() {
		await _assertClassBrand(_CapabilityRunner_brand, this, _ensureReady).call(this, "contribute an alarm");
		const contributions = [];
		for (const capability of _assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this)) {
			const contribution = await capability.getNextAlarm?.();
			if (contribution !== void 0) contributions.push(contribution);
		}
		return contributions;
	}
	/** Route one message to an installed named capability. */
	async route(capabilityId, context) {
		await _assertClassBrand(_CapabilityRunner_brand, this, _ensureReady).call(this, "route a capability message");
		const capability = _assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this).find((candidate) => lifecycleCapabilityId(candidate) === capabilityId);
		if (!capability?.onRoute) throw new Error(`Lifecycle capability ${JSON.stringify(capabilityId)} cannot receive routed messages`);
		return capability.onRoute(context);
	}
	/** Dispose installed capabilities in reverse registration order. */
	async dispose() {
		for (const capability of [..._assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this)].reverse()) try {
			await capability.dispose?.();
		} catch (error) {
			console.error("Lifecycle capability disposal failed", error);
		}
	}
	/** Run every capability's alarm hook in declaration order. */
	async alarm() {
		await _assertClassBrand(_CapabilityRunner_brand, this, _ensureReady).call(this, "handle an alarm");
		for (const capability of _assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this)) await capability.onAlarm?.();
	}
};
async function _runStart(context) {
	for (const capability of _assertClassBrand(_CapabilityRunner_brand, this, _getCapabilities).call(this)) await capability.onStart?.(context);
	_classPrivateFieldSet2(_started, this, true);
}
async function _ensureReady(operation) {
	const pending = _classPrivateFieldGet2(_startPromise, this);
	if (pending) await pending;
	if (!_classPrivateFieldGet2(_started, this)) throw new Error(`Cannot ${operation} before the Durable Object lifecycle has started`);
}
function _getCapabilities() {
	if (!_classPrivateFieldGet2(_capabilities$1, this)) _classPrivateFieldSet2(_capabilities$1, this, Object.freeze([..._classPrivateFieldGet2(_resolveCapabilities, this).call(this)]));
	return _classPrivateFieldGet2(_capabilities$1, this);
}
//#endregion
//#region src/lifecycle/connection.ts
let _Symbol$iterator;
if (!("OPEN" in WebSocket)) {
	const WebSocketStatus = {
		CONNECTING: WebSocket.READY_STATE_CONNECTING,
		OPEN: WebSocket.READY_STATE_OPEN,
		CLOSING: WebSocket.READY_STATE_CLOSING,
		CLOSED: WebSocket.READY_STATE_CLOSED
	};
	Object.assign(WebSocket, WebSocketStatus);
	Object.assign(WebSocket.prototype, WebSocketStatus);
}
function tryGetManagedWebSocketMeta(ws) {
	try {
		const attachment = WebSocket.prototype.deserializeAttachment.call(ws);
		if (!attachment || typeof attachment !== "object") return null;
		if (!("__pk" in attachment)) return null;
		const pk = attachment.__pk;
		if (!pk || typeof pk !== "object") return null;
		const { id, tags } = pk;
		if (typeof id !== "string") return null;
		const { uri } = pk;
		return {
			id,
			tags: Array.isArray(tags) ? tags : [],
			uri: typeof uri === "string" ? uri : void 0
		};
	} catch {
		return null;
	}
}
function isManagedWebSocket(ws) {
	return tryGetManagedWebSocketMeta(ws) !== null;
}
var _cache = /* @__PURE__ */ new WeakMap();
/**
* Cache websocket attachments to avoid having to rehydrate them on every property access.
*/
var AttachmentCache = class {
	constructor() {
		_classPrivateFieldInitSpec(this, _cache, /* @__PURE__ */ new WeakMap());
	}
	get(ws) {
		let attachment = _classPrivateFieldGet2(_cache, this).get(ws);
		if (!attachment) {
			attachment = WebSocket.prototype.deserializeAttachment.call(ws);
			if (attachment !== void 0) _classPrivateFieldGet2(_cache, this).set(ws, attachment);
			else throw new Error("Missing managed WebSocket lifecycle attachment");
		}
		return attachment;
	}
	set(ws, attachment) {
		_classPrivateFieldGet2(_cache, this).set(ws, attachment);
		WebSocket.prototype.serializeAttachment.call(ws, attachment);
	}
};
const attachments = new AttachmentCache();
const connections = /* @__PURE__ */ new WeakSet();
const isWrapped = (ws) => {
	return connections.has(ws);
};
/**
* Wraps a WebSocket with Connection fields that rehydrate the
* socket attachments lazily only when requested.
*/
const createConnection = (ws) => {
	if (isWrapped(ws)) return ws;
	let initialState;
	if ("state" in ws) {
		initialState = ws.state;
		delete ws.state;
	}
	const connection = Object.defineProperties(ws, {
		id: {
			configurable: true,
			get() {
				return attachments.get(ws).__pk.id;
			}
		},
		uri: {
			configurable: true,
			get() {
				return attachments.get(ws).__pk.uri ?? null;
			}
		},
		tags: {
			configurable: true,
			get() {
				return attachments.get(ws).__pk.tags ?? [];
			}
		},
		state: {
			configurable: true,
			get() {
				return attachments.get(ws).__user ?? null;
			}
		},
		setState: {
			configurable: true,
			value: function setState(setState) {
				const state = setState instanceof Function ? setState(this.state) : setState;
				attachments.set(ws, {
					...attachments.get(ws),
					__user: state ?? null
				});
				return state;
			}
		}
	});
	if (initialState) connection.setState(initialState);
	connections.add(connection);
	return connection;
};
_Symbol$iterator = Symbol.iterator;
var ConnectionIterator = class {
	constructor(state, tag) {
		this.state = state;
		this.tag = tag;
		this.index = 0;
	}
	[_Symbol$iterator]() {
		return this;
	}
	next() {
		const sockets = this.sockets ?? (this.sockets = this.state.getWebSockets(this.tag));
		let socket;
		while (socket = sockets[this.index++]) if (socket.readyState === WebSocket.OPEN) {
			if (!isManagedWebSocket(socket)) continue;
			return {
				done: false,
				value: createConnection(socket)
			};
		}
		return {
			done: true,
			value: void 0
		};
	}
};
/**
* Deduplicate and validate connection tags.
* Returns the final tag array (always includes the connection id as the first tag).
*/
function prepareTags(connectionId, userTags) {
	const tags = [connectionId, ...userTags.filter((t) => t !== connectionId)];
	if (tags.length > 10) throw new Error("A connection can only have 10 tags, including the default id tag.");
	for (const tag of tags) {
		if (typeof tag !== "string") throw new Error(`A connection tag must be a string. Received: ${tag}`);
		if (tag === "") throw new Error("A connection tag must not be an empty string.");
		if (tag.length > 256) throw new Error("A connection tag must not exceed 256 characters");
	}
	return tags;
}
/** The platform-backed manager for hibernating WebSockets. */
var ConnectionManager = class {
	constructor(controller) {
		this.controller = controller;
	}
	getConnection(id) {
		const matching = this.controller.getWebSockets(id).filter((ws) => {
			return tryGetManagedWebSocketMeta(ws)?.id === id;
		});
		if (matching.length === 0) return void 0;
		if (matching.length === 1) return createConnection(matching[0]);
		throw new Error(`More than one connection found for id ${id}. Did you mean to use getConnections(tag) instead?`);
	}
	getConnections(tag) {
		return new ConnectionIterator(this.controller, tag);
	}
	accept(connection, options) {
		const tags = prepareTags(connection.id, options.tags);
		this.controller.acceptWebSocket(connection, tags);
		attachments.set(connection, {
			__pk: {
				id: connection.id,
				tags,
				uri: connection.uri ?? void 0
			},
			__user: null
		});
		return createConnection(connection);
	}
};
//#endregion
//#region src/lifecycle/transport-errors.ts
/** Standard `WebSocket.readyState` values. */
const CLOSING = 2;
const CLOSED = 3;
/**
* A retryable transport-teardown error ("Network connection lost" /
* "WebSocket peer disconnected") that fires on a connection which is already
* CLOSING/CLOSED is just the socket going away during or right after the close
* handshake - not an application error. Surfacing it via `onError` spams logs
* on every abrupt client disconnect, and even on clean closes when the peer
* tears down its transport before our reciprocal Close frame lands. Suppress
* it in that specific case only; genuine mid-connection (OPEN) errors still
* reach `onError`.
*
* Detection prefers the structured `retryable` flag over message text so it
* stays correct across `enhanced-error-serialization` (compat date
* >= 2026-04-21), with a substring fallback for older error shapes.
*/
function isBenignTeardownError(ws, error) {
	const state = ws.readyState;
	if (state !== CLOSING && state !== CLOSED) return false;
	if (typeof error !== "object" || error === null) return false;
	const typed = error;
	if (typed.retryable === true) return true;
	const message = typeof typed.message === "string" ? typed.message : "";
	return /Network connection lost|WebSocket peer disconnected/i.test(message);
}
//#endregion
//#region src/lifecycle/durable-object-lifecycle.ts
const LEGACY_NAME_STORAGE_KEY = "__ps_name";
/**
* Reserved WebSocket close codes the runtime synthesizes when there
* was no real Close frame from the peer:
*  - 1005 (NoStatusReceived) — peer's frame had no status code.
*  - 1006 (AbnormalClosure)  — peer dropped the underlying transport
*                              without sending a Close frame at all.
*  - 1015 (TLSHandshake)     — TLS failure during connection setup.
*
* These cannot legally appear in an outgoing Close frame, and — more
* importantly for our reciprocation path — there is no peer left to
* receive a reciprocating Close frame. Trying to send one anyway can
* succeed synchronously but fail asynchronously inside the runtime
* with "WebSocket peer disconnected" / "Network connection lost",
* which escapes a synchronous try/catch and surfaces as an unhandled
* promise rejection.
*/
function isReservedCloseCode(code) {
	return code === 1005 || code === 1006 || code === 1015;
}
/**
* Reciprocate a peer-initiated Close frame to complete the handshake.
*
* Best-effort: swallows synchronous errors from invalid codes,
* oversize reasons, or sockets that have already been closed by user
* code. Skips the reciprocation entirely when the peer didn't
* actually send a Close frame (reserved codes 1005/1006/1015) — in
* those cases the underlying transport is already gone and writing
* to it would fail asynchronously, which we can't catch here.
*
* Used by the hibernating close handler to complete real close handshakes.
*/
function closeQuietly(ws, code, reason) {
	if (isReservedCloseCode(code)) return;
	try {
		ws.close(code, reason);
	} catch {}
}
function mutableRequest(request) {
	return new Request(request);
}
function selectAlarm(contributions) {
	let ordinary = null;
	let exclusive = null;
	for (const contribution of contributions) {
		if (contribution === null) continue;
		const time = typeof contribution === "number" ? contribution : contribution.time;
		if (!Number.isFinite(time) || time < 0) throw new Error(`Invalid alarm contribution: ${String(time)}`);
		if (typeof contribution === "object" && contribution.exclusive) exclusive = exclusive === null ? time : Math.min(exclusive, time);
		else ordinary = ordinary === null ? time : Math.min(ordinary, time);
	}
	return exclusive ?? ordinary;
}
/**
* Decode props from the internal lifecycle props header.
*
* Handles both base64-encoded lifecycle props and, for
* backwards compatibility with stubs/requests created by older versions,
* raw JSON. Base64 never starts with `{` or `[`, so a leading brace/bracket
* unambiguously identifies the legacy raw-JSON form.
*/
function decodeProps(header) {
	const trimmed = header.trim();
	if (trimmed.startsWith("{") || trimmed.startsWith("[")) return JSON.parse(trimmed);
	const binary = atob(header);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return JSON.parse(new TextDecoder().decode(bytes));
}
const lifecycleEventSinks = /* @__PURE__ */ new WeakMap();
const lifecycleRouteTransports = /* @__PURE__ */ new WeakMap();
const lifecycleHostInvokers = /* @__PURE__ */ new WeakMap();
/** @internal Adapt the host invocation boundary at a composition root. */
function setLifecycleHostInvoker(lifecycle, invoker) {
	lifecycleHostInvokers.set(lifecycle, invoker);
}
/** @internal Supply a host's routed Lifecycle transport. */
function setLifecycleRouteTransport(lifecycle, transport) {
	lifecycleRouteTransports.set(lifecycle, transport);
}
/** @internal Adapt Lifecycle's default diagnostics sink at a composition root. */
function setLifecycleEventSink(lifecycle, sink) {
	lifecycleEventSinks.set(lifecycle, sink);
}
var _host = /* @__PURE__ */ new WeakMap();
var _ctx = /* @__PURE__ */ new WeakMap();
var _parentClassName = /* @__PURE__ */ new WeakMap();
var _capabilities = /* @__PURE__ */ new WeakMap();
var _capabilityRunner = /* @__PURE__ */ new WeakMap();
var _connectionManager = /* @__PURE__ */ new WeakMap();
var _status = /* @__PURE__ */ new WeakMap();
var _alarmRearmQueue = /* @__PURE__ */ new WeakMap();
var _rearmRequestedDuringStart = /* @__PURE__ */ new WeakMap();
var _pendingEvents = /* @__PURE__ */ new WeakMap();
var _alarmsDisabled = /* @__PURE__ */ new WeakMap();
var _capabilitiesLocked = /* @__PURE__ */ new WeakMap();
var _handlersInstalled = /* @__PURE__ */ new WeakMap();
var _Lifecycle_brand = /* @__PURE__ */ new WeakSet();
var _legacyName = /* @__PURE__ */ new WeakMap();
var _props = /* @__PURE__ */ new WeakMap();
/**
* Installs and coordinates the runtime lifecycle for a Durable Object.
*
* Construct this as an instance field on a class that directly extends
* `DurableObject`, then call {@link Lifecycle.installHandlers}
* from that class's constructor.
*
* @experimental The API surface may change before stabilizing.
*/
var Lifecycle = class Lifecycle {
	/**
	* Construct and install a lifecycle in one explicit operation.
	*
	* @param host - The Durable Object whose runtime handlers the lifecycle owns.
	* @returns The installed lifecycle.
	*/
	static install(host) {
		const lifecycle = new Lifecycle(host);
		lifecycle.installHandlers();
		return lifecycle;
	}
	/**
	* Bind a lifecycle to a Durable Object instance without mutating its handlers.
	*
	* @param host - The Durable Object whose runtime lifecycle this object owns.
	*/
	constructor(host) {
		_classPrivateMethodInitSpec(this, _Lifecycle_brand);
		_classPrivateFieldInitSpec(this, _host, void 0);
		_classPrivateFieldInitSpec(this, _ctx, void 0);
		_classPrivateFieldInitSpec(this, _parentClassName, void 0);
		_classPrivateFieldInitSpec(this, _capabilities, []);
		_classPrivateFieldInitSpec(this, _capabilityRunner, new CapabilityRunner(() => _classPrivateFieldGet2(_capabilities, this)));
		_classPrivateFieldInitSpec(this, _connectionManager, void 0);
		_classPrivateFieldInitSpec(this, _status, "zero");
		_classPrivateFieldInitSpec(this, _alarmRearmQueue, Promise.resolve());
		_classPrivateFieldInitSpec(this, _rearmRequestedDuringStart, false);
		_classPrivateFieldInitSpec(this, _pendingEvents, []);
		_classPrivateFieldInitSpec(this, _alarmsDisabled, false);
		_classPrivateFieldInitSpec(this, _capabilitiesLocked, false);
		_classPrivateFieldInitSpec(this, _handlersInstalled, false);
		_classPrivateFieldInitSpec(this, _legacyName, void 0);
		_classPrivateFieldInitSpec(this, _props, void 0);
		_classPrivateFieldSet2(_host, this, host);
		_classPrivateFieldSet2(_ctx, this, _classPrivateFieldGet2(_host, this).ctx);
		_classPrivateFieldSet2(_parentClassName, this, _classPrivateFieldGet2(_host, this).constructor.name);
		_classPrivateFieldSet2(_connectionManager, this, new ConnectionManager(_classPrivateFieldGet2(_ctx, this)));
	}
	/**
	* Install platform fetch, alarm, and hibernating WebSocket handlers.
	*
	* Existing handlers are preserved for framework-owned dispatch such as the
	* Agent's sub-agent router and alarm circuit breaker. Calling this method
	* more than once is an error.
	*/
	installHandlers() {
		if (_classPrivateFieldGet2(_handlersInstalled, this)) throw new Error("Durable Object lifecycle handlers are already installed");
		_classPrivateFieldSet2(_handlersInstalled, this, true);
		const handlers = {
			fetch: this.fetch.bind(this),
			alarm: this.alarm.bind(this),
			webSocketMessage: this.webSocketMessage.bind(this),
			webSocketClose: this.webSocketClose.bind(this),
			webSocketError: this.webSocketError.bind(this)
		};
		for (const [name, handler] of Object.entries(handlers)) {
			if (name in _classPrivateFieldGet2(_host, this)) continue;
			Object.defineProperty(_classPrivateFieldGet2(_host, this), name, {
				value: handler,
				configurable: true
			});
		}
	}
	/**
	* Add a reusable capability before this lifecycle starts.
	*
	* @param capability - The capability to add in dispatch order.
	* @returns This lifecycle.
	*/
	use(capability) {
		if (_classPrivateFieldGet2(_capabilitiesLocked, this)) throw new Error("Lifecycle capabilities must be added before startup");
		const capabilityId = lifecycleCapabilityId(capability);
		if (capabilityId && _classPrivateFieldGet2(_capabilities, this).some((candidate) => lifecycleCapabilityId(candidate) === capabilityId)) throw new Error(`Lifecycle capability ${JSON.stringify(capabilityId)} is already installed`);
		_classPrivateFieldGet2(_capabilities, this).push(capability);
		if (capability instanceof LifecycleCapability) bindLifecycleCapability(capability, _assertClassBrand(_Lifecycle_brand, this, _servicesForCapability).call(this, capability.capabilityId));
		return this;
	}
	/** @internal Deliver a generic capability envelope to this Lifecycle. */
	route(envelope) {
		return _assertClassBrand(_Lifecycle_brand, this, _dispatchRoute).call(this, envelope);
	}
	/**
	* Execute SQL queries against the Durable Object's database
	* @template T Type of the returned rows
	* @param strings SQL query template strings
	* @param values Values to be inserted into the query
	* @returns Array of query results
	*/
	sql(strings, ...values) {
		let query = "";
		try {
			query = strings.reduce((acc, str, i) => acc + str + (i < values.length ? "?" : ""), "");
			return [..._classPrivateFieldGet2(_ctx, this).storage.sql.exec(query, ...values)];
		} catch (error) {
			console.error(`failed to execute sql query: ${query}`, error);
			throw error;
		}
	}
	/**
	* Handle an incoming request for the owning Durable Object.
	*/
	async fetch(request) {
		try {
			const encodedProps = request.headers.get("x-agents-lifecycle-props");
			if (encodedProps) {
				_classPrivateFieldSet2(_props, this, decodeProps(encodedProps));
				request = mutableRequest(request);
				request.headers.delete("x-agents-lifecycle-props");
			}
			await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
			const url = new URL(request.url);
			if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
				const capabilityResponse = await runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).request({ request }));
				if (capabilityResponse !== void 0) return capabilityResponse;
				if (_classPrivateFieldGet2(_host, this).onRequest) return await runInLifecycleHostContext({
					host: _classPrivateFieldGet2(_host, this),
					request
				}, () => _classPrivateFieldGet2(_host, this).onRequest(request));
				return new Response("Not implemented", { status: 404 });
			} else {
				const { 0: clientWebSocket, 1: serverWebSocket } = new WebSocketPair();
				let connectionId = url.searchParams.get("_pk");
				if (!connectionId) connectionId = nanoid();
				let connection = Object.assign(serverWebSocket, {
					id: connectionId,
					uri: request.url,
					tags: [],
					state: null,
					setState(setState) {
						let state;
						if (setState instanceof Function) state = setState(this.state);
						else state = setState;
						this.state = state;
						return this.state;
					}
				});
				const ctx = { request };
				const tags = _classPrivateFieldGet2(_host, this).getConnectionTags ? await _classPrivateFieldGet2(_host, this).getConnectionTags(connection, ctx) : [];
				connection = _classPrivateFieldGet2(_connectionManager, this).accept(connection, { tags });
				await runInLifecycleHostContext({
					host: _classPrivateFieldGet2(_host, this),
					connection,
					request
				}, () => _classPrivateFieldGet2(_host, this).onConnect?.(connection, ctx));
				return new Response(null, {
					status: 101,
					webSocket: clientWebSocket
				});
			}
		} catch (err) {
			console.error(`Error in ${_classPrivateFieldGet2(_parentClassName, this)}:${_classPrivateFieldGet2(_ctx, this).id.name ?? "<unnamed>"} fetch:`, err);
			if (!(err instanceof Error)) throw err;
			if (request.headers.get("Upgrade") === "websocket") {
				const pair = new WebSocketPair();
				pair[1].accept();
				pair[1].send(JSON.stringify({ error: err.stack }));
				pair[1].close(1011, "Uncaught exception during session setup");
				return new Response(null, {
					status: 101,
					webSocket: pair[0]
				});
			} else return new Response(err.stack, { status: 500 });
		}
	}
	/** @internal Dispatch a hibernating WebSocket message. */
	async webSocketMessage(ws, message) {
		if (!isManagedWebSocket(ws)) return;
		try {
			const connection = createConnection(ws);
			await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
			return runInLifecycleHostContext({
				host: _classPrivateFieldGet2(_host, this),
				connection
			}, () => _classPrivateFieldGet2(_host, this).onMessage?.(connection, message));
		} catch (e) {
			console.error(`Error in ${_classPrivateFieldGet2(_parentClassName, this)}:${_classPrivateFieldGet2(_ctx, this).id.name ?? "<unnamed>"} webSocketMessage:`, e);
		}
	}
	/** @internal Dispatch and reciprocate a hibernating WebSocket close. */
	async webSocketClose(ws, code, reason, wasClean) {
		if (!isManagedWebSocket(ws)) return;
		try {
			const connection = createConnection(ws);
			await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
			await runInLifecycleHostContext({
				host: _classPrivateFieldGet2(_host, this),
				connection
			}, () => _classPrivateFieldGet2(_host, this).onClose?.(connection, code, reason, wasClean));
		} catch (e) {
			console.error(`Error in ${_classPrivateFieldGet2(_parentClassName, this)}:${_classPrivateFieldGet2(_ctx, this).id.name ?? "<unnamed>"} webSocketClose:`, e);
		} finally {
			closeQuietly(ws, code, reason);
		}
	}
	/** @internal Dispatch a hibernating WebSocket error. */
	async webSocketError(ws, error) {
		if (!isManagedWebSocket(ws)) return;
		if (isBenignTeardownError(ws, error)) return;
		try {
			const connection = createConnection(ws);
			await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
			return runInLifecycleHostContext({
				host: _classPrivateFieldGet2(_host, this),
				connection
			}, () => _classPrivateFieldGet2(_host, this).onError?.(connection, error));
		} catch (e) {
			console.error(`Error in ${_classPrivateFieldGet2(_parentClassName, this)}:${_classPrivateFieldGet2(_ctx, this).id.name ?? "<unnamed>"} webSocketError:`, e);
		}
	}
	/**
	* Start lifecycle capabilities and the owning Durable Object.
	*
	* Runtime fetch, alarm, and WebSocket entry points call this automatically.
	* RPC methods may call it explicitly because native RPC bypasses fetch.
	*
	* @param props - Optional properties supplied to capability and host startup.
	*/
	async start(props) {
		if (props !== void 0) _classPrivateFieldSet2(_props, this, props);
		await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
	}
	/**
	* The name used to address this Durable Object.
	*
	* Native `ctx.id.name` is authoritative. A read-only legacy storage fallback
	* lets objects created by older PartyServer releases migrate without new
	* name writes.
	*/
	get name() {
		const name = _classPrivateFieldGet2(_ctx, this).id.name ?? _classPrivateFieldGet2(_legacyName, this);
		if (name !== void 0) return name;
		throw new Error(`${_classPrivateFieldGet2(_parentClassName, this)} could not determine its Durable Object name. Address it with idFromName() or getByName(). In local development, update Wrangler/workerd and use a current compatibility_date. newUniqueId(), idFromString(), and names over 1,024 bytes do not expose ctx.id.name. Alarms created before 2026-03-15 must be rescheduled from a named fetch or RPC handler.`);
	}
	/** Send a message to all connected clients, except connection ids listed in `without` */
	broadcast(msg, without) {
		for (const connection of _classPrivateFieldGet2(_connectionManager, this).getConnections()) if (!without || !without.includes(connection.id)) _assertClassBrand(_Lifecycle_brand, this, _sendMessageToConnection).call(this, connection, msg);
	}
	/** Get a connection by connection id */
	getConnection(id) {
		return _classPrivateFieldGet2(_connectionManager, this).getConnection(id);
	}
	/** Get all managed connections, optionally filtered by tag. */
	getConnections(tag) {
		return _classPrivateFieldGet2(_connectionManager, this).getConnections(tag);
	}
	/**
	* Recompute the physical Durable Object alarm from every capability.
	*
	* Concurrent requests are serialized so a later durable-state change cannot
	* be overwritten by an earlier alarm calculation.
	*/
	async rearmAlarm() {
		if (_classPrivateFieldGet2(_alarmsDisabled, this)) return;
		if (_classPrivateFieldGet2(_status, this) === "starting") {
			_classPrivateFieldSet2(_rearmRequestedDuringStart, this, true);
			return;
		}
		if (_classPrivateFieldGet2(_status, this) === "zero") await this.start();
		const next = _classPrivateFieldGet2(_alarmRearmQueue, this).catch(() => {}).then(async () => {
			if (_classPrivateFieldGet2(_alarmsDisabled, this)) return;
			const contributions = await runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).getAlarmContributions());
			const hostContribution = await runInLifecycleHostContext({ host: _classPrivateFieldGet2(_host, this) }, () => _classPrivateFieldGet2(_host, this).getNextAlarm?.());
			if (hostContribution !== void 0) contributions.push(hostContribution);
			const alarm = selectAlarm(contributions);
			if (alarm === null) await _classPrivateFieldGet2(_ctx, this).storage.deleteAlarm();
			else await _classPrivateFieldGet2(_ctx, this).storage.setAlarm(alarm);
		});
		_classPrivateFieldSet2(_alarmRearmQueue, this, next);
		await next;
	}
	/** Dispose installed capabilities in reverse registration order. */
	async dispose() {
		await runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).dispose());
	}
	/** Permanently disable and clear alarms during explicit object teardown. */
	async disableAlarms() {
		_classPrivateFieldSet2(_alarmsDisabled, this, true);
		await _classPrivateFieldGet2(_alarmRearmQueue, this).catch(() => {});
		await _classPrivateFieldGet2(_ctx, this).storage.deleteAlarm();
	}
	/** Dispatch lifecycle and host alarm callbacks after startup. */
	async alarm() {
		await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
		await runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).alarm());
		await runInLifecycleHostContext({ host: _classPrivateFieldGet2(_host, this) }, () => _classPrivateFieldGet2(_host, this).onAlarm?.());
		await this.rearmAlarm();
	}
};
function _servicesForCapability(capabilityId) {
	const lifecycle = this;
	const envelope = (payload) => ({
		capability: capabilityId,
		source: lifecycleRouteTransports.get(lifecycle)?.source,
		payload
	});
	return Object.freeze({
		storage: _classPrivateFieldGet2(_ctx, this).storage,
		ready: () => _assertClassBrand(_Lifecycle_brand, this, _readyForCapabilityOperation).call(this),
		starting: () => _classPrivateFieldGet2(_status, this) === "starting",
		alarms: Object.freeze({
			rearm: () => this.rearmAlarm(),
			disabled: () => _classPrivateFieldGet2(_alarmsDisabled, this)
		}),
		runInHostContext: async (fn) => _assertClassBrand(_Lifecycle_brand, this, _runInHostBoundary).call(this, fn),
		events: Object.freeze({ emit: (type, payload) => _assertClassBrand(_Lifecycle_brand, this, _emitCapabilityEvent).call(this, {
			source: capabilityId,
			type,
			payload
		}) }),
		routes: Object.freeze({
			get source() {
				return lifecycleRouteTransports.get(lifecycle)?.source;
			},
			toRoot: (payload) => {
				const transport = lifecycleRouteTransports.get(lifecycle);
				return transport ? transport.toRoot(envelope(payload)) : _assertClassBrand(_Lifecycle_brand, this, _dispatchRoute).call(this, envelope(payload));
			},
			to: (target, payload) => {
				const transport = lifecycleRouteTransports.get(lifecycle);
				if (!transport) throw new Error("Lifecycle has no transport for routed capabilities");
				return transport.to(target, envelope(payload));
			}
		})
	});
}
/**
* Run a user callback inside the host invocation boundary — plain host
* context by default, or the composition root's substitute (Agent installs
* its tracing invocation scope).
*/
function _runInHostBoundary(fn) {
	const boundary = lifecycleHostInvokers.get(this);
	return Promise.resolve(boundary ? boundary(fn) : runInLifecycleHostContext({ host: _classPrivateFieldGet2(_host, this) }, fn));
}
async function _readyForCapabilityOperation() {
	if (_classPrivateFieldGet2(_status, this) === "starting" || _classPrivateFieldGet2(_status, this) === "started") return;
	await this.start();
}
async function _dispatchRoute(envelope) {
	await _assertClassBrand(_Lifecycle_brand, this, _ensureInitialized).call(this);
	return runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).route(envelope.capability, {
		source: envelope.source,
		payload: envelope.payload
	}));
}
function _emitCapabilityEvent(event) {
	if (event.source.trim() === "" || event.type.trim() === "") throw new Error("Lifecycle events require non-empty source and type");
	if (_classPrivateFieldGet2(_status, this) !== "started") {
		_classPrivateFieldGet2(_pendingEvents, this).push(event);
		return;
	}
	_assertClassBrand(_Lifecycle_brand, this, _publishCapabilityEvent).call(this, event);
}
function _publishCapabilityEvent(event) {
	runWithoutCurrentAgent(() => {
		const sink = lifecycleEventSinks.get(this);
		try {
			if (!sink) {
				publishDiagnosticsEvent({
					source: event.source,
					type: event.type,
					agent: _classPrivateFieldGet2(_parentClassName, this),
					name: this.name,
					payload: event.payload,
					timestamp: Date.now()
				});
				return;
			}
			const pending = sink(event);
			if (pending !== void 0) _classPrivateFieldGet2(_ctx, this).waitUntil(Promise.resolve(pending).catch((error) => {
				_assertClassBrand(_Lifecycle_brand, this, _reportEventSinkFailure).call(this, event, error);
			}));
		} catch (error) {
			_assertClassBrand(_Lifecycle_brand, this, _reportEventSinkFailure).call(this, event, error);
		}
	});
}
function _reportEventSinkFailure(event, error) {
	console.error(`Lifecycle event sink failed for ${event.source}:${event.type}`, error);
}
function _deliverPendingEvents() {
	for (const event of _classPrivateFieldGet2(_pendingEvents, this).splice(0)) _assertClassBrand(_Lifecycle_brand, this, _publishCapabilityEvent).call(this, event);
}
async function _ensureInitialized() {
	if (_classPrivateFieldGet2(_status, this) === "started") return;
	if (_classPrivateFieldGet2(_ctx, this).id.name === void 0 && _classPrivateFieldGet2(_legacyName, this) === void 0) _classPrivateFieldSet2(_legacyName, this, await _classPrivateFieldGet2(_ctx, this).storage.get(LEGACY_NAME_STORAGE_KEY));
	this.name;
	_classPrivateFieldSet2(_capabilitiesLocked, this, true);
	let error;
	await _classPrivateFieldGet2(_ctx, this).blockConcurrencyWhile(async () => {
		_classPrivateFieldSet2(_status, this, "starting");
		try {
			await runWithoutCurrentAgent(() => _classPrivateFieldGet2(_capabilityRunner, this).start({ props: _classPrivateFieldGet2(_props, this) }));
			await runInLifecycleHostContext({ host: _classPrivateFieldGet2(_host, this) }, () => _classPrivateFieldGet2(_host, this).onStart?.(_classPrivateFieldGet2(_props, this)));
			_classPrivateFieldSet2(_status, this, "started");
		} catch (cause) {
			_classPrivateFieldSet2(_status, this, "zero");
			error = cause;
		}
	});
	if (error) {
		_classPrivateFieldSet2(_rearmRequestedDuringStart, this, false);
		_classPrivateFieldGet2(_pendingEvents, this).length = 0;
		throw error;
	}
	_assertClassBrand(_Lifecycle_brand, this, _deliverPendingEvents).call(this);
	if (_classPrivateFieldGet2(_rearmRequestedDuringStart, this)) {
		_classPrivateFieldSet2(_rearmRequestedDuringStart, this, false);
		await this.rearmAlarm();
	}
}
function _sendMessageToConnection(connection, message) {
	try {
		connection.send(message);
	} catch (_e) {
		connection.close(1011, "Unexpected error");
	}
}
//#endregion
export { setLifecycleRouteTransport as i, setLifecycleEventSink as n, setLifecycleHostInvoker as r, Lifecycle as t };

//# sourceMappingURL=durable-object-lifecycle-D6nNQJJd.js.map