import { camelCaseToKebabCase, isInternalJsStubProp } from "./utils.js";
import { getAgentByName } from "./agent-routing.js";
import { AGENT_TOOL_MILESTONE_PART } from "./agent-tool-types.js";
import { n as getCurrentAgent$1, t as __DO_NOT_USE_WILL_BREAK__agentContext } from "./current-agent-DhoDkSnH.js";
import "./internal_context.js";
import "./types.js";
import { signAgentHeaders } from "./email.js";
import { sendAgentEmail } from "./email-send.js";
import { i as _classPrivateFieldInitSpec, n as _classPrivateFieldSet2, t as _classPrivateFieldGet2 } from "./classPrivateFieldGet2-DZBYAB34.js";
import { n as withInvocationScope, r as writeSpanAttributes, t as tracer } from "./cloudflare-Dzvc7V2N.js";
import { parseSubAgentPath } from "./sub-routing.js";
import { i as setLifecycleRouteTransport, n as setLifecycleEventSink, r as setLifecycleHostInvoker, t as Lifecycle } from "./durable-object-lifecycle-D6nNQJJd.js";
import { SqlError } from "./sql-error.js";
import { isDurableObjectMemoryLimitReset, isErrorRetryable, tryN, validateRetryOptions } from "./retries.js";
import { a as ensureMcpServerTable, d as DisposableStore, i as normalizeServerId, l as RPC_DO_PREFIX, o as MCPConnectionState, t as MCPClientManager } from "./client-jagG8a9_.js";
import { DurableObjectOAuthClientProvider } from "./mcp/client/do-oauth-client-provider.js";
import { genericObservability } from "./observability/index.js";
import { n as ensureScheduleTable, r as setSchedulerCallbackResolver, t as Scheduler } from "./scheduler-CR9RHGos.js";
import { AsyncLocalStorage } from "node:async_hooks";
import { nanoid } from "nanoid";
import { EmailMessage } from "cloudflare:email";
import { DurableObject, RpcTarget, exports } from "cloudflare:workers";
//#region src/observability/agent-span-attributes.ts
function agentSpanAttributes(input) {
	return {
		"instrumentation_scope.name": "agents",
		"instrumentation_scope.version": "1",
		"cloudflare.agents.session.id": input.sessionId,
		"cloudflare.agents.session.name": input.sessionName,
		"gen_ai.agent.name": input.agentClassName
	};
}
//#endregion
//#region src/index.ts
let _Symbol$dispose;
/**
* Enters an agent invocation: the context every handler reads, plus the span
* scope that stops invocation-bounded spans from outliving it. Scopes do not
* nest, so the outermost live entry point owns the boundary — pass
* `detached` for work that deliberately runs on past its caller.
*/
function runInInvocation(store, body, options) {
	return __DO_NOT_USE_WILL_BREAK__agentContext.run(store, () => withInvocationScope(body, options));
}
function isClosedWebSocketSendError(error) {
	return error instanceof TypeError && error.message.includes("WebSocket send() after close");
}
function sendRpcResponseIfOpen(connection, response) {
	try {
		connection.send(JSON.stringify(response));
		return true;
	} catch (error) {
		if (isClosedWebSocketSendError(error)) return false;
		throw error;
	}
}
const subAgentRpcReplyContext = new AsyncLocalStorage();
function sendFacetRpcResponseIfOpen(target, response) {
	try {
		return {
			sent: true,
			completion: Promise.resolve(target.send(JSON.stringify(response))).catch((error) => {
				if (!isClosedWebSocketSendError(error)) console.error("[Agent] Facet RPC response delivery failed:", error);
			})
		};
	} catch (error) {
		if (isClosedWebSocketSendError(error)) return {
			sent: false,
			completion: Promise.resolve()
		};
		throw error;
	}
}
const facetStreamingResponseDeliveryStates = /* @__PURE__ */ new WeakMap();
function createStreamingResponse(connection, id, facetReplyTarget) {
	const stream = new StreamingResponse(connection, id);
	if (facetReplyTarget) facetStreamingResponseDeliveryStates.set(stream, {
		replyTarget: facetReplyTarget,
		pending: /* @__PURE__ */ new Set()
	});
	return stream;
}
function trackFacetStreamingResponseDelivery(stream, completion) {
	const state = facetStreamingResponseDeliveryStates.get(stream);
	if (!state) return;
	state.pending.add(completion);
	completion.finally(() => state.pending.delete(completion));
}
async function waitForFacetStreamingResponseDeliveries(stream) {
	const state = facetStreamingResponseDeliveryStates.get(stream);
	if (!state) return;
	try {
		await Promise.all(state.pending);
	} finally {
		facetStreamingResponseDeliveryStates.delete(stream);
	}
}
/**
* Type guard for RPC request messages
*/
function isRPCRequest(msg) {
	return typeof msg === "object" && msg !== null && "type" in msg && msg.type === "rpc" && "id" in msg && typeof msg.id === "string" && "method" in msg && typeof msg.method === "string" && "args" in msg && Array.isArray(msg.args);
}
/**
* Type guard for state update messages
*/
function isStateUpdateMessage(msg) {
	return typeof msg === "object" && msg !== null && "type" in msg && msg.type === "cf_agent_state" && "state" in msg;
}
const callableMetadata = /* @__PURE__ */ new WeakMap();
var _connection2 = /* @__PURE__ */ new WeakMap();
var _broadcast = /* @__PURE__ */ new WeakMap();
var SubAgentConnectionBridge = class extends RpcTarget {
	constructor(connection, broadcast) {
		super();
		_classPrivateFieldInitSpec(this, _connection2, void 0);
		_classPrivateFieldInitSpec(this, _broadcast, void 0);
		_classPrivateFieldSet2(_connection2, this, connection);
		_classPrivateFieldSet2(_broadcast, this, broadcast);
	}
	send(message) {
		_classPrivateFieldGet2(_connection2, this).send(message);
	}
	close(code, reason) {
		_classPrivateFieldGet2(_connection2, this).close(code, reason);
	}
	setState(state) {
		return _classPrivateFieldGet2(_connection2, this).setState(state);
	}
	broadcast(ownerPath, message, without) {
		return _classPrivateFieldGet2(_broadcast, this)?.call(this, ownerPath, message, without);
	}
};
var _root = /* @__PURE__ */ new WeakMap();
var _connectionId = /* @__PURE__ */ new WeakMap();
var RootSubAgentConnectionBridge = class {
	constructor(root, connectionId) {
		_classPrivateFieldInitSpec(this, _root, void 0);
		_classPrivateFieldInitSpec(this, _connectionId, void 0);
		_classPrivateFieldSet2(_root, this, root);
		_classPrivateFieldSet2(_connectionId, this, connectionId);
	}
	send(message) {
		return _classPrivateFieldGet2(_root, this)._cf_sendToSubAgentConnection(_classPrivateFieldGet2(_connectionId, this), message);
	}
	close(code, reason) {
		return _classPrivateFieldGet2(_root, this)._cf_closeSubAgentConnection(_classPrivateFieldGet2(_connectionId, this), code, reason);
	}
	setState(state) {
		return _classPrivateFieldGet2(_root, this)._cf_setSubAgentConnectionState(_classPrivateFieldGet2(_connectionId, this), state);
	}
	broadcast(ownerPath, message, without) {
		return _classPrivateFieldGet2(_root, this)._cf_broadcastToSubAgent(ownerPath, message, without);
	}
};
/**
* Decorator that marks a method as callable by clients
* @param metadata Optional metadata about the callable method
*/
function callable(metadata = {}) {
	return function callableDecorator(target, _context) {
		if (!callableMetadata.has(target)) callableMetadata.set(target, metadata);
		return target;
	};
}
let didWarnAboutUnstableCallable = false;
/**
* Decorator that marks a method as callable by clients
* @deprecated this has been renamed to callable, and unstable_callable will be removed in the next major version
* @param metadata Optional metadata about the callable method
*/
const unstable_callable = (metadata = {}) => {
	if (!didWarnAboutUnstableCallable) {
		didWarnAboutUnstableCallable = true;
		console.warn("unstable_callable is deprecated, use callable instead. unstable_callable will be removed in the next major version.");
	}
	return callable(metadata);
};
function agentPathKey(path) {
	if (!path) return null;
	return path.map((step) => `${encodeURIComponent(step.className)}:${encodeURIComponent(step.name)}`).join("/");
}
const _fiberALS = new AsyncLocalStorage();
const DEFAULT_KEEP_ALIVE_INTERVAL_MS = 3e4;
const DEFAULT_AGENT_TOOL_RECOVERY_TIMEOUT_MS = 2e3;
const DEFAULT_AGENT_TOOL_RECOVERY_TOTAL_TIMEOUT_MS = 5e3;
const DESTROY_PENDING_KEY = "cf_agents_destroy_pending";
const DESTROY_ALARM_DELAY_MS = 1e3;
const FIBER_RECOVERY_MAX_BACKOFF_MS = 5 * 6e4;
const FIBER_RECOVERY_BACKOFF_MAX_EXP = 20;
const DEFAULT_AGENT_TOOL_REATTACH_NO_PROGRESS_TIMEOUT_MS = 12e4;
const DEFAULT_AGENT_TOOL_REATTACH_MAX_WINDOW_MS = Number.POSITIVE_INFINITY;
const DEFAULT_DETACHED_MAX_BUDGET_MS = 1440 * 60 * 1e3;
const DEFAULT_DETACHED_NO_PROGRESS_BUDGET_MS = 3600 * 1e3;
const DETACHED_DELIVERY_LEASE_MS = 6e4;
const DETACHED_BACKBONE_CADENCE_S = [
	5,
	15,
	30,
	120
];
const DETACHED_LIVE_COUNT_WARN_THRESHOLD = 50;
const DETACHED_RECONCILE_CALLBACK = "_cfDetachedReconcileTick";
const DETACHED_NOTIFY_CALLBACK = "_cfDetachedNotifyFinish";
const SUB_AGENT_IDENTITY_VERSION_LEGACY = "legacy";
const SUB_AGENT_IDENTITY_VERSION_PATH_V2 = "path-v2";
const SUB_AGENT_IDENTITY_PATH_V2_PREFIX = "cf-agents:v2:";
/**
* Schema version for the Agent's internal SQLite tables.
* Bump this when adding new tables, columns, or migrations.
* The constructor stores this as a row in cf_agents_state and checks it
* on wake to skip DDL on established DOs.
*/
const CURRENT_SCHEMA_VERSION = 11;
const SCHEMA_VERSION_ROW_ID = "cf_schema_version";
const STATE_ROW_ID = "cf_state_row_id";
const STATE_WAS_CHANGED = "cf_state_was_changed";
const DEFAULT_STATE = {};
async function sha256Hex(value) {
	const bytes = new TextEncoder().encode(value);
	const digest = await crypto.subtle.digest("SHA-256", bytes);
	return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
function pathV2IdentityName(logicalName, digest) {
	return `${SUB_AGENT_IDENTITY_PATH_V2_PREFIX}${encodeURIComponent(logicalName)}:${digest}`;
}
function logicalNameFromPathV2Identity(identityName) {
	if (!identityName.startsWith(SUB_AGENT_IDENTITY_PATH_V2_PREFIX)) return null;
	const rest = identityName.slice(13);
	const separator = rest.lastIndexOf(":");
	if (separator === -1) return null;
	try {
		return decodeURIComponent(rest.slice(0, separator));
	} catch {
		return null;
	}
}
/**
* Validate that a stored `parentPath` has the expected shape. Used
* when restoring from DO storage to guard against corrupted data.
*/
function isValidParentPath(value) {
	if (!Array.isArray(value)) return false;
	return value.every((entry) => entry != null && typeof entry === "object" && typeof entry.className === "string" && typeof entry.name === "string");
}
/**
* Internal key used to store the readonly flag in connection state.
* Prefixed with _cf_ to avoid collision with user state keys.
*/
const CF_READONLY_KEY = "_cf_readonly";
/**
* Internal key used to store the no-protocol flag in connection state.
* When set, protocol messages (identity, state sync, MCP servers) are not
* sent to this connection — neither on connect nor via broadcasts.
*/
const CF_NO_PROTOCOL_KEY = "_cf_no_protocol";
/**
* Internal key used to store voice call state in connection state.
* Used by the voice mixin to track whether a connection is in an active call.
*/
const CF_VOICE_IN_CALL_KEY = "_cf_voiceInCall";
/**
* Internal key used to remember the outer `/sub/...` URL for a
* WebSocket accepted by the parent on behalf of a child facet.
* Hibernated events then wake the parent, which forwards frames to
* the child over serializable RPC while keeping native WebSocket I/O
* parent-owned.
*/
const CF_SUB_AGENT_OUTER_URL_KEY = "_cf_subAgentOuterUrl";
const CF_SUB_AGENT_TAGS_KEY = "_cf_subAgentTags";
const SUB_AGENT_OUTER_URL_HEADER = "x-cf-agents-subagent-url";
/**
* The set of all internal keys stored in connection state that must be
* hidden from user code and preserved across setState calls.
*/
const CF_INTERNAL_KEYS = /* @__PURE__ */ new Set([
	CF_READONLY_KEY,
	CF_NO_PROTOCOL_KEY,
	CF_VOICE_IN_CALL_KEY,
	CF_SUB_AGENT_OUTER_URL_KEY,
	CF_SUB_AGENT_TAGS_KEY
]);
/** Check if a raw connection state object contains any internal keys. */
function rawHasInternalKeys(raw) {
	for (const key of Object.keys(raw)) if (CF_INTERNAL_KEYS.has(key)) return true;
	return false;
}
/** Return a copy of `raw` with all internal keys removed, or null if no user keys remain. */
function stripInternalKeys(raw) {
	const result = {};
	let hasUserKeys = false;
	for (const key of Object.keys(raw)) if (!CF_INTERNAL_KEYS.has(key)) {
		result[key] = raw[key];
		hasUserKeys = true;
	}
	return hasUserKeys ? result : null;
}
/** Return a copy containing only the internal keys present in `raw`. */
function extractInternalFlags(raw) {
	const result = {};
	for (const key of Object.keys(raw)) if (CF_INTERNAL_KEYS.has(key)) result[key] = raw[key];
	return result;
}
/** Max length for error strings broadcast to clients. */
const MAX_ERROR_STRING_LENGTH = 500;
/**
* Sanitize an error string before broadcasting to clients.
* MCP error strings may contain untrusted content from external OAuth
* providers — truncate and strip control characters to limit XSS risk.
*/
const CONTROL_CHAR_RE = /* @__PURE__ */ new RegExp("[\\u0000-\\u0008\\u000B\\u000C\\u000E-\\u001F\\u007F]", "g");
function sanitizeErrorString(error) {
	if (error === null) return null;
	let sanitized = error.replace(CONTROL_CHAR_RE, "");
	if (sanitized.length > MAX_ERROR_STRING_LENGTH) sanitized = sanitized.substring(0, MAX_ERROR_STRING_LENGTH) + "...";
	return sanitized;
}
/**
* Tracks which agent constructors have already emitted the onStateUpdate
* deprecation warning, so it fires at most once per class.
*/
const _onStateUpdateWarnedClasses = /* @__PURE__ */ new WeakSet();
/**
* Tracks which agent constructors have already emitted the
* sendIdentityOnConnect deprecation warning, so it fires at most once per class.
*/
const _sendIdentityWarnedClasses = /* @__PURE__ */ new WeakSet();
/**
* Default options for Agent configuration.
* Child classes can override specific options without spreading.
*/
const DEFAULT_AGENT_STATIC_OPTIONS = {
	/** Whether to send identity (name, agent) to clients on connect */
	sendIdentityOnConnect: true,
	/**
	* Timeout in seconds before a running interval schedule is considered "hung"
	* and force-reset. Increase this if you have callbacks that legitimately
	* take longer than 30 seconds.
	*/
	hungScheduleTimeoutSeconds: 30,
	/**
	* Interval in milliseconds for keepAlive() alarm heartbeats.
	* Lower values mean faster recovery after eviction but more frequent alarms.
	*/
	keepAliveIntervalMs: DEFAULT_KEEP_ALIVE_INTERVAL_MS,
	/** Default retry options for schedule(), queue(), and this.retry() */
	retry: {
		maxAttempts: 3,
		baseDelayMs: 100,
		maxDelayMs: 3e3
	},
	/** Timeout for internal framework fiber recovery hooks. */
	fiberRecoveryHookTimeoutMs: 1e4,
	/** Soft deadline for one interrupted-fiber recovery scan. */
	fiberRecoveryScanDeadlineMs: 1e4,
	/**
	* Maximum age of an unmanaged interrupted-fiber row before recovery gives
	* up. Bounds repeated retries of a `onFiberRecovered()` hook that keeps
	* throwing so a poison row cannot re-trigger forever across boots.
	*/
	fiberRecoveryMaxAgeMs: 1440 * 60 * 1e3,
	/**
	* No-progress budget (ms) for re-attaching to a still-running agent-tool
	* child after a deploy / parent recovery (#1630). Bounds how long the parent
	* waits with NO forward progress from the child; it resets on every forwarded
	* chunk, so a child that keeps streaming is never abandoned mid-flight. Only a
	* genuinely silent/hung child seals `interrupted` after a full window. Raise
	* for children with long quiet stretches between outputs.
	*/
	agentToolReattachNoProgressTimeoutMs: DEFAULT_AGENT_TOOL_REATTACH_NO_PROGRESS_TIMEOUT_MS,
	/**
	* Optional hard wall-clock ceiling (ms) on a single agent-tool re-attach
	* (#1630). Caps the total wait even as the no-progress budget re-arms across
	* stream-closes. Defaults to `Infinity` (no implicit cap), mirroring
	* chat-recovery's `maxRecoveryWork` (#1672): a healthy, still-advancing child
	* is followed for as long as it makes progress — a hung child is bounded by
	* the no-progress budget, and a content-runaway by the child's own
	* `maxRecoveryWork` / `shouldKeepRecovering`. Set a finite value to impose a
	* wall-clock cap (which also tears the child down on `window-exceeded`).
	*/
	agentToolReattachMaxWindowMs: DEFAULT_AGENT_TOOL_REATTACH_MAX_WINDOW_MS,
	detachedMaxBudgetMs: DEFAULT_DETACHED_MAX_BUDGET_MS,
	detachedNoProgressBudgetMs: DEFAULT_DETACHED_NO_PROGRESS_BUDGET_MS,
	/**
	* Consecutive alarm invocations that may end in a Durable Object memory-limit
	* reset (the isolate exceeded its 128 MB limit) before the alarm-boundary
	* circuit breaker stops the platform's auto-retry loop and seals the looping
	* work (#1825). A small budget tolerates a genuinely transient memory spike;
	* a deterministic OOM (the work's footprint, not the platform, is the cause)
	* is bounded here regardless of whether the in-DO recovery budgets could run.
	*/
	maxAlarmMemoryLimitStrikes: 3
};
/**
* Parse the raw `retry_options` TEXT column from a SQLite row into a
* typed `RetryOptions` object, or `undefined` if not set.
*/
function parseRetryOptions(row) {
	const raw = row.retry_options;
	if (typeof raw !== "string") return void 0;
	return JSON.parse(raw);
}
/**
* Resolve per-task retry options against class-level defaults and call
* `tryN`. This is the retry-execution path for queue flush; Scheduler owns
* its own copy for schedule callbacks.
*/
function resolveRetryConfig(taskRetry, defaults) {
	return {
		maxAttempts: taskRetry?.maxAttempts ?? defaults.maxAttempts,
		baseDelayMs: taskRetry?.baseDelayMs ?? defaults.baseDelayMs,
		maxDelayMs: taskRetry?.maxDelayMs ?? defaults.maxDelayMs
	};
}
/** Compatibility alias for the lifecycle-owned current Agent accessor. */
const getCurrentAgent = getCurrentAgent$1;
/**
* Restore Agent context when a public method is entered outside a Lifecycle
* hook, notably through native Durable Object RPC or cross-Agent re-entry.
* Lifecycle already owns context for its capability and semantic user hooks.
*/
function withAgentContext(method) {
	return function(...args) {
		const { agent } = getCurrentAgent();
		if (agent === this) return method.apply(this, args);
		return runInInvocation({
			agent: this,
			connection: void 0,
			request: void 0,
			email: void 0
		}, () => {
			return method.apply(this, args);
		});
	};
}
/**
* Base class for creating Agent implementations
* @template Env Environment type containing bindings
* @template State State type to store within the Agent
*/
var Agent = class Agent extends DurableObject {
	/** Run user initialization after lifecycle components have started. */
	onStart(_props) {}
	/** Handle an HTTP request not claimed by a lifecycle component. */
	onRequest(_request) {
		return new Response("Not implemented", { status: 404 });
	}
	/** Handle a newly accepted hibernating WebSocket connection. */
	onConnect(_connection, _context) {}
	/** Handle a message from a hibernating WebSocket connection. */
	onMessage(_connection, _message) {}
	/** Handle a hibernating WebSocket connection closing. */
	onClose(_connection, _code, _reason, _wasClean) {}
	/** Return tags persisted with a hibernating WebSocket connection. */
	getConnectionTags(_connection, _context) {
		return [];
	}
	/** @internal Ensure lifecycle startup before a native RPC implementation. */
	async __unsafe_ensureInitialized(props) {
		await this.lifecycle.start(props);
	}
	/**
	* Stable key for Workers AI session affinity (prefix-cache optimization).
	*
	* Uses the Durable Object ID, which is globally unique across all agent
	* classes and stable for the lifetime of the instance. Pass this value as
	* the `sessionAffinity` option when creating a Workers AI model so that
	* requests from the same agent instance are routed to the same backend
	* replica, improving KV-prefix-cache hit rates across conversation turns.
	*
	* @example
	* ```typescript
	* const workersai = createWorkersAI({ binding: this.env.AI });
	* const model = workersai("@cf/meta/llama-3.3-70b-instruct-fp8-fast", {
	*   sessionAffinity: this.sessionAffinity,
	* });
	* ```
	*/
	get sessionAffinity() {
		return this.ctx.id.toString();
	}
	/**
	* Current state of the Agent
	*/
	get state() {
		if (this._state !== DEFAULT_STATE) return this._state;
		const result = this.sql`
      SELECT state FROM cf_agents_state WHERE id = ${STATE_ROW_ID}
    `;
		if (result.length > 0) {
			const state = result[0].state;
			try {
				this._state = JSON.parse(state);
			} catch (e) {
				console.error("Failed to parse stored state, falling back to initialState:", e);
				if (this.initialState !== DEFAULT_STATE) {
					this._state = this.initialState;
					this._setStateInternal(this.initialState);
				} else {
					this.sql`DELETE FROM cf_agents_state WHERE id = ${STATE_ROW_ID}`;
					return;
				}
			}
			return this._state;
		}
		if (this.initialState === DEFAULT_STATE) return;
		this._setStateInternal(this.initialState);
		return this.initialState;
	}
	get _resolvedOptions() {
		if (this._cachedOptions) return this._cachedOptions;
		const ctor = this.constructor;
		const userRetry = ctor.options?.retry;
		this._cachedOptions = {
			sendIdentityOnConnect: ctor.options?.sendIdentityOnConnect ?? DEFAULT_AGENT_STATIC_OPTIONS.sendIdentityOnConnect,
			hungScheduleTimeoutSeconds: ctor.options?.hungScheduleTimeoutSeconds ?? DEFAULT_AGENT_STATIC_OPTIONS.hungScheduleTimeoutSeconds,
			keepAliveIntervalMs: ctor.options?.keepAliveIntervalMs ?? DEFAULT_AGENT_STATIC_OPTIONS.keepAliveIntervalMs,
			retry: {
				maxAttempts: userRetry?.maxAttempts ?? DEFAULT_AGENT_STATIC_OPTIONS.retry.maxAttempts,
				baseDelayMs: userRetry?.baseDelayMs ?? DEFAULT_AGENT_STATIC_OPTIONS.retry.baseDelayMs,
				maxDelayMs: userRetry?.maxDelayMs ?? DEFAULT_AGENT_STATIC_OPTIONS.retry.maxDelayMs
			},
			fiberRecoveryHookTimeoutMs: ctor.options?.fiberRecoveryHookTimeoutMs ?? DEFAULT_AGENT_STATIC_OPTIONS.fiberRecoveryHookTimeoutMs,
			fiberRecoveryScanDeadlineMs: ctor.options?.fiberRecoveryScanDeadlineMs ?? DEFAULT_AGENT_STATIC_OPTIONS.fiberRecoveryScanDeadlineMs,
			fiberRecoveryMaxAgeMs: ctor.options?.fiberRecoveryMaxAgeMs ?? DEFAULT_AGENT_STATIC_OPTIONS.fiberRecoveryMaxAgeMs,
			agentToolReattachNoProgressTimeoutMs: ctor.options?.agentToolReattachNoProgressTimeoutMs ?? DEFAULT_AGENT_STATIC_OPTIONS.agentToolReattachNoProgressTimeoutMs,
			agentToolReattachMaxWindowMs: ctor.options?.agentToolReattachMaxWindowMs ?? DEFAULT_AGENT_STATIC_OPTIONS.agentToolReattachMaxWindowMs,
			detachedMaxBudgetMs: ctor.options?.detachedMaxBudgetMs ?? DEFAULT_AGENT_STATIC_OPTIONS.detachedMaxBudgetMs,
			detachedNoProgressBudgetMs: ctor.options?.detachedNoProgressBudgetMs ?? DEFAULT_AGENT_STATIC_OPTIONS.detachedNoProgressBudgetMs,
			maxAlarmMemoryLimitStrikes: ctor.options?.maxAlarmMemoryLimitStrikes ?? DEFAULT_AGENT_STATIC_OPTIONS.maxAlarmMemoryLimitStrikes
		};
		return this._cachedOptions;
	}
	/**
	* Emit an observability event with auto-generated timestamp.
	* @internal
	*/
	_emit(type, payload = {}) {
		this.observability?.emit({
			type,
			agent: this._ParentClass.name,
			name: this.name,
			payload,
			timestamp: Date.now()
		});
	}
	_withAgentSpan(operation, storagePhase, attributes, run) {
		let agentId;
		try {
			agentId = this.name;
		} catch {
			agentId = void 0;
		}
		return tracer.withSpan(operation, {
			...agentSpanAttributes({
				agentClassName: this._ParentClass.name,
				sessionId: this.ctx.id.toString(),
				sessionName: agentId
			}),
			"cloudflare.agents.operation.name": operation,
			"cloudflare.agents.storage.grouped": true,
			"cloudflare.agents.storage.system": "durable_object",
			"cloudflare.agents.storage.phase": storagePhase,
			...attributes
		}, (span) => run((finishAttributes) => writeSpanAttributes(span, finishAttributes)), __DO_NOT_USE_WILL_BREAK__agentContext.getStore()?.connection === void 0 ? void 0 : { boundToInvocation: true });
	}
	/**
	* Execute SQL queries against the Agent's database
	* @template T Type of the returned rows
	* @param strings SQL query template strings
	* @param values Values to be inserted into the query
	* @returns Array of query results
	*/
	sql(strings, ...values) {
		let query = "";
		try {
			query = strings.reduce((acc, str, i) => acc + str + (i < values.length ? "?" : ""), "");
			return [...this.ctx.storage.sql.exec(query, ...values)];
		} catch (e) {
			throw new SqlError(query, e);
		}
	}
	/**
	* Create all internal tables and run migrations if needed.
	* Called by the constructor on every wake. Idempotent — skips DDL when
	* the stored schema version matches CURRENT_SCHEMA_VERSION.
	*
	* Protected so that test agents can re-run the real migration path
	* after manipulating DB state (since ctx.abort() is unavailable in
	* local dev and the constructor only runs once per DO instance).
	*/
	_ensureSchema() {
		this.sql`
      CREATE TABLE IF NOT EXISTS cf_agents_state (
        id TEXT PRIMARY KEY NOT NULL,
        state TEXT
      )
    `;
		const versionRow = this.sql`
      SELECT state FROM cf_agents_state WHERE id = ${SCHEMA_VERSION_ROW_ID}
    `;
		const schemaVersion = versionRow.length > 0 ? Number(versionRow[0].state) : 0;
		if (schemaVersion < CURRENT_SCHEMA_VERSION) {
			ensureMcpServerTable(this.ctx.storage);
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_queues (
          id TEXT PRIMARY KEY NOT NULL,
          payload TEXT,
          callback TEXT,
          created_at INTEGER DEFAULT (unixepoch())
        )
      `;
			const addColumnIfNotExists = (sql) => {
				try {
					this.ctx.storage.sql.exec(sql);
				} catch (error) {
					if (!(error instanceof Error ? error.message : String(error)).toLowerCase().includes("duplicate column")) throw error;
				}
			};
			addColumnIfNotExists("ALTER TABLE cf_agents_queues ADD COLUMN retry_options TEXT");
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_workflows (
          id TEXT PRIMARY KEY NOT NULL,
          workflow_id TEXT NOT NULL UNIQUE,
          workflow_name TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN (
            'queued', 'running', 'paused', 'errored',
            'terminated', 'complete', 'waiting',
            'waitingForPause', 'unknown'
          )),
          metadata TEXT,
          error_name TEXT,
          error_message TEXT,
          created_at INTEGER NOT NULL DEFAULT (unixepoch()),
          updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
          completed_at INTEGER
        )
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_workflows_status ON cf_agents_workflows(status)
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_workflows_name ON cf_agents_workflows(workflow_name)
      `;
			this.ctx.storage.sql.exec("DELETE FROM cf_agents_state WHERE id = ?", STATE_WAS_CHANGED);
			ensureScheduleTable(this.ctx.storage);
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_runs (
          id TEXT PRIMARY KEY NOT NULL,
          name TEXT NOT NULL,
          snapshot TEXT,
          created_at INTEGER NOT NULL
        )
      `;
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_facet_runs (
          owner_path TEXT NOT NULL,
          owner_path_key TEXT NOT NULL,
          run_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (owner_path_key, run_id)
        )
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_facet_runs_owner_path_key
        ON cf_agents_facet_runs(owner_path_key)
      `;
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_fibers (
          fiber_id TEXT PRIMARY KEY,
          idempotency_key TEXT UNIQUE,
          name TEXT NOT NULL,
          status TEXT NOT NULL,
          snapshot TEXT,
          metadata_json TEXT,
          error_message TEXT,
          created_at INTEGER NOT NULL,
          started_at INTEGER,
          completed_at INTEGER
        )
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_fibers_status_created
        ON cf_agents_fibers(status, created_at, fiber_id)
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_fibers_name_status_created
        ON cf_agents_fibers(name, status, created_at, fiber_id)
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_fibers_status_completed
        ON cf_agents_fibers(status, completed_at, created_at)
      `;
			this.sql`
        CREATE TABLE IF NOT EXISTS cf_agent_tool_runs (
          run_id TEXT PRIMARY KEY,
          parent_tool_call_id TEXT,
          agent_type TEXT NOT NULL,
          input_preview TEXT,
          input_redacted INTEGER NOT NULL DEFAULT 1,
          status TEXT NOT NULL,
          summary TEXT,
          output_json TEXT,
          error_message TEXT,
          interrupted_reason TEXT,
          child_still_running INTEGER,
          display_metadata TEXT,
          display_order INTEGER NOT NULL DEFAULT 0,
          started_at INTEGER NOT NULL,
          completed_at INTEGER
        )
      `;
			this.sql`
        CREATE INDEX IF NOT EXISTS idx_agent_tool_runs_parent_tool_call_id
        ON cf_agent_tool_runs(parent_tool_call_id, display_order)
      `;
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN output_json TEXT");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN interrupted_reason TEXT");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN child_still_running INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached INTEGER NOT NULL DEFAULT 0");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached_on_finish TEXT");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached_notify_source TEXT");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached_max_budget_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN finish_claimed_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN finish_delivered_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN give_up_claimed_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN give_up_delivered_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached_no_progress_budget_ms INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN last_progress_at INTEGER");
			addColumnIfNotExists("ALTER TABLE cf_agent_tool_runs ADD COLUMN detached_on_milestones TEXT");
			this.sql`
        INSERT OR REPLACE INTO cf_agents_state (id, state)
        VALUES (${SCHEMA_VERSION_ROW_ID}, ${String(CURRENT_SCHEMA_VERSION)})
      `;
		}
		this._schemaInitialization = {
			previousVersion: schemaVersion,
			currentVersion: CURRENT_SCHEMA_VERSION,
			migrated: schemaVersion < CURRENT_SCHEMA_VERSION
		};
	}
	constructor(ctx, env) {
		super(ctx, env);
		this.lifecycle = Lifecycle.install(this);
		this._state = DEFAULT_STATE;
		this._disposables = new DisposableStore();
		this._destroyed = false;
		this._rawStateAccessors = /* @__PURE__ */ new WeakMap();
		this._persistenceHookMode = "none";
		this._isFacet = false;
		this._protocolBroadcastExcludeIds = /* @__PURE__ */ new Set();
		this._cf_subAgentBridgeContext = new AsyncLocalStorage();
		this._cf_virtualSubAgentConnections = /* @__PURE__ */ new Map();
		this._cf_subAgentConnectionOperationTails = /* @__PURE__ */ new Map();
		this._parentPath = [];
		this._warnedChatRecoveryInOnStart = false;
		this._keepAliveRefs = 0;
		this._facetKeepAliveTokens = /* @__PURE__ */ new Set();
		this._runFiberActiveFibers = /* @__PURE__ */ new Set();
		this._managedFiberAbortControllers = /* @__PURE__ */ new Map();
		this._managedFiberExecutions = /* @__PURE__ */ new Map();
		this._managedFiberTerminalWaiters = /* @__PURE__ */ new Map();
		this._runFiberRecoveryInProgress = false;
		this._recoveryNoProgressScans = 0;
		this._detachedBackboneArming = Promise.resolve();
		this._detachedLiveCountWarned = false;
		this._ParentClass = Object.getPrototypeOf(this).constructor;
		this.initialState = DEFAULT_STATE;
		this.observability = genericObservability;
		this._flushingQueue = false;
		this.maxConcurrentAgentTools = Infinity;
		this._subAgentRegistryReady = false;
		const routeHost = this;
		setLifecycleRouteTransport(this.lifecycle, {
			get source() {
				return routeHost._lifecycleRouteAddress();
			},
			toRoot: (envelope) => this._routeLifecycleToRoot(envelope),
			to: (target, envelope) => this._routeLifecycleToTarget(target, envelope)
		});
		setLifecycleEventSink(this.lifecycle, (event) => {
			const payload = event.payload !== null && typeof event.payload === "object" && !Array.isArray(event.payload) ? event.payload : { value: event.payload };
			this._emit(event.type, payload);
		});
		setLifecycleHostInvoker(this.lifecycle, (run) => runInInvocation({
			agent: this,
			connection: void 0,
			request: void 0,
			email: void 0
		}, run));
		this.scheduler = new Scheduler({
			retry: this._resolvedOptions.retry,
			hungScheduleTimeoutSeconds: this._resolvedOptions.hungScheduleTimeoutSeconds,
			onError: (error) => runInInvocation({
				agent: this,
				connection: void 0,
				request: void 0,
				email: void 0
			}, () => this.onError(error))
		});
		setSchedulerCallbackResolver(this.scheduler, (name) => {
			const method = this[name];
			if (typeof method !== "function") return void 0;
			return (payload, schedule) => method.call(this, payload, schedule);
		});
		this.mcp = this._withAgentSpan("agent_initialization", "initialization", {}, (update) => {
			if (!wrappedClasses.has(this.constructor)) {
				this._autoWrapCustomMethods();
				wrappedClasses.add(this.constructor);
			}
			this._withAgentSpan("initialize_agent_storage", "initialization", {}, (updateStorage) => {
				this._ensureSchema();
				const schemaAttributes = {
					"cloudflare.agents.schema.version.previous": this._schemaInitialization?.previousVersion,
					"cloudflare.agents.schema.version.current": this._schemaInitialization?.currentVersion,
					"cloudflare.agents.schema.migrated": this._schemaInitialization?.migrated
				};
				updateStorage(schemaAttributes);
				update(schemaAttributes);
			});
			return new MCPClientManager(this._ParentClass.name, "0.0.1", {
				env: this.env,
				createAuthProvider: (callbackUrl) => this.createMcpOAuthProvider(callbackUrl)
			});
		});
		this.lifecycle.use(this.scheduler).use(this.mcp);
		let mcpBroadcastReady = false;
		this._disposables.add(this.mcp.onServerStateChanged(() => {
			if (mcpBroadcastReady) this.broadcastMcpServers();
		}));
		this._disposables.add(this.mcp.onObservabilityEvent((event) => {
			this.observability?.emit({
				...event,
				agent: this._ParentClass.name,
				name: this.name
			});
		}));
		{
			const proto = Object.getPrototypeOf(this);
			const hasOwnNew = Object.prototype.hasOwnProperty.call(proto, "onStateChanged");
			const hasOwnOld = Object.prototype.hasOwnProperty.call(proto, "onStateUpdate");
			if (hasOwnNew && hasOwnOld) throw new Error("[Agent] Cannot override both onStateChanged and onStateUpdate. Remove onStateUpdate — it has been renamed to onStateChanged.");
			if (hasOwnOld) {
				const ctor = this.constructor;
				if (!_onStateUpdateWarnedClasses.has(ctor)) {
					_onStateUpdateWarnedClasses.add(ctor);
					console.warn(`[Agent] onStateUpdate is deprecated. Rename to onStateChanged — the behavior is identical.`);
				}
			}
			const base = Agent.prototype;
			if (proto.onStateChanged !== base.onStateChanged) this._persistenceHookMode = "new";
			else if (proto.onStateUpdate !== base.onStateUpdate) this._persistenceHookMode = "old";
		}
		const _onAlarm = this.onAlarm.bind(this);
		this.onAlarm = async () => {
			if (this._destroyed) return;
			await _onAlarm();
			if (this._destroyed) return;
			await this._onAlarmHousekeeping();
		};
		const _onRequest = this.onRequest.bind(this);
		this.onRequest = (request) => {
			return runInInvocation({
				agent: this,
				connection: void 0,
				request,
				email: void 0
			}, () => this._tryCatch(() => _onRequest(request)));
		};
		const _onMessage = this.onMessage.bind(this);
		this.onMessage = async (connection, message) => {
			const replyBridge = subAgentRpcReplyContext.getStore()?.bridge;
			if (await __DO_NOT_USE_WILL_BREAK__agentContext.exit(() => this._cf_forwardSubAgentWebSocketMessage(connection, message, replyBridge))) return;
			this._ensureConnectionWrapped(connection);
			return runInInvocation({
				agent: this,
				connection,
				request: void 0,
				email: void 0
			}, async () => {
				if (typeof message !== "string") return this._tryCatch(() => _onMessage(connection, message));
				let parsed;
				try {
					parsed = JSON.parse(message);
				} catch (_e) {
					return this._tryCatch(() => _onMessage(connection, message));
				}
				if (isStateUpdateMessage(parsed)) {
					if (this.isConnectionReadonly(connection)) {
						connection.send(JSON.stringify({
							type: "cf_agent_state_error",
							error: "Connection is readonly"
						}));
						return;
					}
					try {
						this._setStateInternal(parsed.state, connection);
					} catch (e) {
						console.error("[Agent] State update rejected:", e);
						connection.send(JSON.stringify({
							type: "cf_agent_state_error",
							error: "State update rejected"
						}));
					}
					return;
				}
				if (isRPCRequest(parsed)) {
					try {
						const { id, method, args } = parsed;
						const methodFn = this[method];
						if (typeof methodFn !== "function") throw new Error(`Method ${method} does not exist`);
						if (!this._isCallable(method)) throw new Error(`Method ${method} is not callable`);
						const metadata = callableMetadata.get(methodFn);
						if (metadata?.streaming) {
							const stream = createStreamingResponse(connection, id, replyBridge);
							this._emit("rpc", {
								method,
								streaming: true
							});
							try {
								await methodFn.apply(this, [stream, ...args]);
							} catch (err) {
								console.error(`Error in streaming method "${method}":`, err);
								this._emit("rpc:error", {
									method,
									error: err instanceof Error ? err.message : String(err)
								});
								if (!stream.isClosed) stream.error(err instanceof Error ? err.message : String(err));
							}
							await waitForFacetStreamingResponseDeliveries(stream);
							return;
						}
						const result = await methodFn.apply(this, args);
						this._emit("rpc", {
							method,
							streaming: metadata?.streaming
						});
						const response = {
							done: true,
							id,
							result,
							success: true,
							type: "rpc"
						};
						if (replyBridge) await sendFacetRpcResponseIfOpen(replyBridge, response).completion;
						else sendRpcResponseIfOpen(connection, response);
					} catch (e) {
						const response = {
							error: e instanceof Error ? e.message : "Unknown error occurred",
							id: parsed.id,
							success: false,
							type: "rpc"
						};
						if (replyBridge) await sendFacetRpcResponseIfOpen(replyBridge, response).completion;
						else sendRpcResponseIfOpen(connection, response);
						console.error("RPC error:", e);
						this._emit("rpc:error", {
							method: parsed.method,
							error: e instanceof Error ? e.message : String(e)
						});
					}
					return;
				}
				return this._tryCatch(() => _onMessage(connection, message));
			});
		};
		const _onConnect = this.onConnect.bind(this);
		this.onConnect = async (connection, ctx) => {
			this._ensureConnectionWrapped(connection);
			const subAgentOuterUrl = ctx.request.headers.get(SUB_AGENT_OUTER_URL_HEADER);
			if (subAgentOuterUrl) this._unsafe_setConnectionFlag(connection, CF_SUB_AGENT_OUTER_URL_KEY, subAgentOuterUrl);
			if (await __DO_NOT_USE_WILL_BREAK__agentContext.exit(() => this._cf_forwardSubAgentWebSocketConnect(connection, ctx.request, { gate: false }))) return;
			return runInInvocation({
				agent: this,
				connection,
				request: ctx.request,
				email: void 0
			}, async () => {
				if (this.shouldConnectionBeReadonly(connection, ctx)) this.setConnectionReadonly(connection, true);
				if (this.shouldSendProtocolMessages(connection, ctx)) {
					if (this._resolvedOptions.sendIdentityOnConnect) {
						const ctor = this.constructor;
						if (ctor.options?.sendIdentityOnConnect === void 0 && !_sendIdentityWarnedClasses.has(ctor) && !this._isFacet) {
							if (!new URL(ctx.request.url).pathname.includes(this.name)) {
								_sendIdentityWarnedClasses.add(ctor);
								console.warn(`[Agent] ${ctor.name}: sending instance name "${this.name}" to clients via sendIdentityOnConnect (the name is not visible in the URL with custom routing). If this name is sensitive, add \`static options = { sendIdentityOnConnect: false }\` to opt out. Set it to true to silence this message.`);
							}
						}
						connection.send(JSON.stringify({
							name: this.name,
							agent: camelCaseToKebabCase(this._ParentClass.name),
							type: "cf_agent_identity"
						}));
					}
					const wasExcludedFromStateInitBroadcast = this._protocolBroadcastExcludeIds.has(connection.id);
					let currentState;
					this._protocolBroadcastExcludeIds.add(connection.id);
					try {
						currentState = this.state;
					} finally {
						if (!wasExcludedFromStateInitBroadcast) this._protocolBroadcastExcludeIds.delete(connection.id);
					}
					if (currentState !== void 0) connection.send(JSON.stringify({
						state: currentState,
						type: "cf_agent_state"
					}));
					connection.send(JSON.stringify({
						mcp: this.getMcpServers(),
						type: "cf_agent_mcp_servers"
					}));
				} else this._setConnectionNoProtocol(connection);
				this._emit("connect", { connectionId: connection.id });
				await this._replayAgentToolRuns(connection);
				return this._tryCatch(() => _onConnect(connection, ctx));
			});
		};
		const _onClose = this.onClose.bind(this);
		this.onClose = async (connection, code, reason, wasClean) => {
			if (await __DO_NOT_USE_WILL_BREAK__agentContext.exit(() => this._cf_forwardSubAgentWebSocketClose(connection, code, reason, wasClean))) return;
			return runInInvocation({
				agent: this,
				connection,
				request: void 0,
				email: void 0
			}, () => {
				this._emit("disconnect", {
					connectionId: connection.id,
					code,
					reason
				});
				return _onClose(connection, code, reason, wasClean);
			});
		};
		const _onStart = this.onStart.bind(this);
		const startAgent = async (props, update) => {
			return runInInvocation({
				agent: this,
				connection: void 0,
				request: void 0,
				email: void 0
			}, async () => {
				await this._restoreAgentFacetContext();
				await this._tryCatch(async () => {
					mcpBroadcastReady = true;
					this.broadcastMcpServers();
					const startupAgentToolRunIds = await this._withAgentSpan("recover_agent_work", "startup", {}, async () => {
						this._checkOrphanedWorkflows();
						await this._checkRunFibers();
						return this._agentToolRunRecoveryRunIds();
					});
					update({
						"cloudflare.agents.start.facet": this._isFacet,
						"cloudflare.agents.recovery.agent_tools.count": startupAgentToolRunIds.length
					});
					const chatRecoveryBefore = this.chatRecovery;
					const result = await this._withAgentSpan("run_user_on_start", "startup", {}, () => _onStart(props));
					const chatRecoveryAfter = this.chatRecovery;
					const chatRecoveryAfterMatters = typeof chatRecoveryAfter === "boolean" || typeof chatRecoveryAfter === "object" && chatRecoveryAfter !== null;
					if (!this._warnedChatRecoveryInOnStart && chatRecoveryBefore !== chatRecoveryAfter && chatRecoveryAfterMatters) {
						this._warnedChatRecoveryInOnStart = true;
						console.warn("[Agent] `chatRecovery` was assigned during onStart(). Chat recovery evaluates its budgets (and may seal an interrupted turn, firing onExhausted) on wake BEFORE onStart() runs, so a config set here is applied too late and the built-in defaults are used for the recovery that matters. Assign `chatRecovery` as a class field or in the constructor instead.");
					}
					this._scheduleAgentToolRunRecovery({ runIds: startupAgentToolRunIds });
					return result;
				});
			});
		};
		this.onStart = (props) => this._withAgentSpan("agent_start", "startup", {}, (update) => startAgent(props, update));
	}
	async _restoreAgentFacetContext() {
		await this._withAgentSpan("restore_agent_state", "startup", {}, async () => {
			if (await this.ctx.storage.get("cf_agents_is_facet")) this._isFacet = true;
			const storedFacetName = await this.ctx.storage.get("cf_agents_facet_name");
			if (typeof storedFacetName === "string") this._facetName = storedFacetName;
			const storedParentPath = await this.ctx.storage.get("cf_agents_parent_path");
			if (isValidParentPath(storedParentPath)) this._parentPath = storedParentPath;
			try {
				await this._cf_hydrateSubAgentConnectionsFromRoot();
			} catch (error) {
				console.warn("[Agent] Unable to hydrate sub-agent WebSocket connections:", error);
			}
		});
	}
	/**
	* Check for workflows referencing unknown bindings and warn with migration suggestion.
	*/
	_checkOrphanedWorkflows() {
		const orphaned = this.sql`
      SELECT 
        workflow_name,
        COUNT(*) as total,
        SUM(CASE WHEN status NOT IN ('complete', 'errored', 'terminated') THEN 1 ELSE 0 END) as active,
        SUM(CASE WHEN status IN ('complete', 'errored', 'terminated') THEN 1 ELSE 0 END) as completed
      FROM cf_agents_workflows 
      GROUP BY workflow_name
    `.filter((row) => !this._findWorkflowBindingByName(row.workflow_name));
		if (orphaned.length > 0) {
			const currentBindings = this._getWorkflowBindingNames();
			for (const { workflow_name: oldName, total, active, completed } of orphaned) {
				const suggestion = currentBindings.length === 1 ? `this.migrateWorkflowBinding('${oldName}', '${currentBindings[0]}')` : `this.migrateWorkflowBinding('${oldName}', '<NEW_BINDING_NAME>')`;
				const breakdown = active > 0 && completed > 0 ? ` (${active} active, ${completed} completed)` : active > 0 ? ` (${active} active)` : ` (${completed} completed)`;
				console.warn(`[Agent] Found ${total} workflow(s) referencing unknown binding '${oldName}'${breakdown}. If you renamed the binding, call: ${suggestion}`);
			}
		}
	}
	/**
	* Broadcast a protocol message only to connections that have protocol
	* messages enabled. Connections where shouldSendProtocolMessages returned
	* false are excluded automatically.
	* @param msg The JSON-encoded protocol message
	* @param excludeIds Additional connection IDs to exclude (e.g. the source)
	*/
	_broadcastProtocol(msg, excludeIds = []) {
		const exclude = [...excludeIds, ...this._protocolBroadcastExcludeIds];
		for (const conn of this.getConnections()) if (!this.isConnectionProtocolEnabled(conn)) exclude.push(conn.id);
		this.broadcast(msg, exclude);
	}
	_setStateInternal(nextState, source = "server") {
		this.validateStateChange(nextState, source);
		this._state = nextState;
		this.sql`
      INSERT OR REPLACE INTO cf_agents_state (id, state)
      VALUES (${STATE_ROW_ID}, ${JSON.stringify(nextState)})
    `;
		this._broadcastProtocol(JSON.stringify({
			state: nextState,
			type: "cf_agent_state"
		}), source !== "server" ? [source.id] : []);
		const { connection, request, email } = __DO_NOT_USE_WILL_BREAK__agentContext.getStore() || {};
		this.ctx.waitUntil((async () => {
			try {
				await runInInvocation({
					agent: this,
					connection,
					request,
					email
				}, async () => {
					this._emit("state:update");
					await this._callStatePersistenceHook(nextState, source);
				}, { detached: true });
			} catch (e) {
				try {
					await this.onError(e);
				} catch {}
			}
		})());
	}
	/**
	* Update the Agent's state
	* @param state New state to set
	* @throws Error if called from a readonly connection context
	*/
	setState(state) {
		const store = __DO_NOT_USE_WILL_BREAK__agentContext.getStore();
		if (store?.connection && this.isConnectionReadonly(store.connection)) throw new Error("Connection is readonly");
		this._setStateInternal(state, "server");
	}
	/**
	* Wraps connection.state and connection.setState so that internal
	* _cf_-prefixed flags (readonly, no-protocol) are hidden from user code
	* and cannot be accidentally overwritten.
	*
	* Idempotent — safe to call multiple times on the same connection.
	* After hibernation, the _rawStateAccessors WeakMap is empty but the
	* connection's state getter still reads from the persisted WebSocket
	* attachment. Calling this method re-captures the raw getter so that
	* predicate methods (isConnectionReadonly, isConnectionProtocolEnabled)
	* work correctly post-hibernation.
	*/
	_ensureConnectionWrapped(connection) {
		if (this._rawStateAccessors.has(connection)) return;
		const descriptor = Object.getOwnPropertyDescriptor(connection, "state");
		let getRaw;
		let setRaw;
		if (descriptor?.get) {
			getRaw = descriptor.get.bind(connection);
			setRaw = connection.setState.bind(connection);
		} else {
			let rawState = connection.state ?? null;
			getRaw = () => rawState;
			setRaw = (state) => {
				rawState = state;
				return rawState;
			};
		}
		this._rawStateAccessors.set(connection, {
			getRaw,
			setRaw
		});
		Object.defineProperty(connection, "state", {
			configurable: true,
			enumerable: true,
			get() {
				const raw = getRaw();
				if (raw != null && typeof raw === "object" && rawHasInternalKeys(raw)) return stripInternalKeys(raw);
				return raw;
			}
		});
		Object.defineProperty(connection, "setState", {
			configurable: true,
			writable: true,
			value(stateOrFn) {
				const raw = getRaw();
				const flags = raw != null && typeof raw === "object" ? extractInternalFlags(raw) : {};
				const hasFlags = Object.keys(flags).length > 0;
				let newUserState;
				if (typeof stateOrFn === "function") newUserState = stateOrFn(hasFlags ? stripInternalKeys(raw) : raw);
				else newUserState = stateOrFn;
				if (hasFlags) {
					if (newUserState != null && typeof newUserState === "object") return setRaw({
						...newUserState,
						...flags
					});
					return setRaw(flags);
				}
				return setRaw(newUserState);
			}
		});
	}
	/**
	* Mark a connection as readonly or readwrite
	* @param connection The connection to mark
	* @param readonly Whether the connection should be readonly (default: true)
	*/
	setConnectionReadonly(connection, readonly = true) {
		this._ensureConnectionWrapped(connection);
		const accessors = this._rawStateAccessors.get(connection);
		const raw = accessors.getRaw() ?? {};
		if (readonly) accessors.setRaw({
			...raw,
			[CF_READONLY_KEY]: true
		});
		else {
			const { [CF_READONLY_KEY]: _, ...rest } = raw;
			accessors.setRaw(Object.keys(rest).length > 0 ? rest : null);
		}
	}
	/**
	* Check if a connection is marked as readonly.
	*
	* Safe to call after hibernation — re-wraps the connection if the
	* in-memory accessor cache was cleared.
	* @param connection The connection to check
	* @returns True if the connection is readonly
	*/
	isConnectionReadonly(connection) {
		this._ensureConnectionWrapped(connection);
		return !!this._rawStateAccessors.get(connection).getRaw()?.[CF_READONLY_KEY];
	}
	/**
	* ⚠️ INTERNAL — DO NOT USE IN APPLICATION CODE. ⚠️
	*
	* Read an internal `_cf_`-prefixed flag from the raw connection state,
	* bypassing the user-facing state wrapper that strips internal keys.
	*
	* This exists for framework mixins (e.g. voice) that need to persist
	* flags in the connection attachment across hibernation. Application
	* code should use `connection.state` and `connection.setState()` instead.
	*
	* @internal
	*/
	_unsafe_getConnectionFlag(connection, key) {
		this._ensureConnectionWrapped(connection);
		return this._rawStateAccessors.get(connection).getRaw()?.[key];
	}
	/**
	* ⚠️ INTERNAL — DO NOT USE IN APPLICATION CODE. ⚠️
	*
	* Write an internal `_cf_`-prefixed flag to the raw connection state,
	* bypassing the user-facing state wrapper. The key must be registered
	* in `CF_INTERNAL_KEYS` so it is preserved across user `setState` calls
	* and hidden from `connection.state`.
	*
	* @internal
	*/
	_unsafe_setConnectionFlag(connection, key, value) {
		this._ensureConnectionWrapped(connection);
		const accessors = this._rawStateAccessors.get(connection);
		const raw = accessors.getRaw() ?? {};
		if (value === void 0) {
			const { [key]: _, ...rest } = raw;
			accessors.setRaw(Object.keys(rest).length > 0 ? rest : null);
		} else accessors.setRaw({
			...raw,
			[key]: value
		});
	}
	/**
	* Override this method to determine if a connection should be readonly on connect
	* @param _connection The connection that is being established
	* @param _ctx Connection context
	* @returns True if the connection should be readonly
	*/
	shouldConnectionBeReadonly(_connection, _ctx) {
		return false;
	}
	/**
	* Override this method to control whether protocol messages are sent to a
	* connection. Protocol messages include identity (CF_AGENT_IDENTITY), state
	* sync (CF_AGENT_STATE), and MCP server lists (CF_AGENT_MCP_SERVERS).
	*
	* When this returns `false` for a connection, that connection will not
	* receive any protocol text frames — neither on connect nor via broadcasts.
	* This is useful for binary-only clients (e.g. MQTT devices) that cannot
	* handle JSON text frames.
	*
	* The connection can still send and receive regular messages, use RPC, and
	* participate in all non-protocol communication.
	*
	* @param _connection The connection that is being established
	* @param _ctx Connection context (includes the upgrade request)
	* @returns True if protocol messages should be sent (default), false to suppress them
	*/
	shouldSendProtocolMessages(_connection, _ctx) {
		return true;
	}
	/**
	* Check if a connection has protocol messages enabled.
	* Protocol messages include identity, state sync, and MCP server lists.
	*
	* Safe to call after hibernation — re-wraps the connection if the
	* in-memory accessor cache was cleared.
	* @param connection The connection to check
	* @returns True if the connection receives protocol messages
	*/
	isConnectionProtocolEnabled(connection) {
		this._ensureConnectionWrapped(connection);
		return !this._rawStateAccessors.get(connection).getRaw()?.[CF_NO_PROTOCOL_KEY];
	}
	/**
	* Mark a connection as having protocol messages disabled.
	* Called internally when shouldSendProtocolMessages returns false.
	*/
	_setConnectionNoProtocol(connection) {
		this._ensureConnectionWrapped(connection);
		const accessors = this._rawStateAccessors.get(connection);
		const raw = accessors.getRaw() ?? {};
		accessors.setRaw({
			...raw,
			[CF_NO_PROTOCOL_KEY]: true
		});
	}
	/**
	* Called before the Agent's state is persisted and broadcast.
	* Override to validate or reject an update by throwing an error.
	*
	* IMPORTANT: This hook must be synchronous.
	*/
	validateStateChange(_nextState, _source) {}
	/**
	* Called after the Agent's state has been persisted and broadcast to all clients.
	* This is a notification hook — errors here are routed to onError and do not
	* affect state persistence or client broadcasts.
	*
	* @param state Updated state
	* @param source Source of the state update ("server" or a client connection)
	*/
	onStateChanged(_state, _source) {}
	/**
	* @deprecated Renamed to `onStateChanged` — the behavior is identical.
	* `onStateUpdate` will be removed in the next major version.
	*
	* Called after the Agent's state has been persisted and broadcast to all clients.
	* This is a server-side notification hook. For the client-side state callback,
	* see the `onStateUpdate` option in `useAgent` / `AgentClient`.
	*
	* @param state Updated state
	* @param source Source of the state update ("server" or a client connection)
	*/
	onStateUpdate(_state, _source) {}
	/**
	* Dispatch to the appropriate persistence hook based on the mode
	* cached in the constructor. No prototype walks at call time.
	*/
	async _callStatePersistenceHook(state, source) {
		switch (this._persistenceHookMode) {
			case "new":
				await this.onStateChanged(state, source);
				break;
			case "old":
				await this.onStateUpdate(state, source);
				break;
		}
	}
	/**
	* Called when the Agent receives an email via routeAgentEmail()
	* Override this method to handle incoming emails
	* @param payload Internal wire format — plain data + RpcTarget bridge
	*/
	async _onEmail(payload) {
		const email = {
			from: payload.from,
			to: payload.to,
			headers: payload.headers,
			rawSize: payload.rawSize,
			_secureRouted: payload._secureRouted,
			getRaw: () => payload._bridge.getRaw(),
			setReject: (reason) => payload._bridge.setReject(reason),
			forward: (rcptTo, headers) => payload._bridge.forward(rcptTo, headers),
			reply: (options) => payload._bridge.reply(options)
		};
		return runInInvocation({
			agent: this,
			connection: void 0,
			request: void 0,
			email
		}, async () => {
			this._emit("email:receive", {
				from: email.from,
				to: email.to,
				subject: email.headers.get("subject") ?? void 0
			});
			if ("onEmail" in this && typeof this.onEmail === "function") return this._tryCatch(() => this.onEmail(email));
			else {
				console.log("Received email from:", email.from, "to:", email.to);
				console.log("Subject:", email.headers.get("subject"));
				console.log("Implement onEmail(email: AgentEmail): Promise<void> in your agent to process emails");
			}
		});
	}
	/**
	* Reply to an email
	* @param email The email to reply to
	* @param options Options for the reply
	* @param options.secret Secret for signing agent headers (enables secure reply routing).
	*   Required if the email was routed via createSecureReplyEmailResolver.
	*   Pass explicit `null` to opt-out of signing (not recommended for secure routing).
	* @returns void
	*/
	async replyToEmail(email, options) {
		return this._tryCatch(async () => {
			if (email._secureRouted && options.secret === void 0) throw new Error("This email was routed via createSecureReplyEmailResolver. You must pass a secret to replyToEmail() to sign replies, or pass explicit null to opt-out (not recommended).");
			const agentName = camelCaseToKebabCase(this._ParentClass.name);
			const agentId = this.name;
			const { createMimeMessage } = await import("mimetext");
			const msg = createMimeMessage();
			msg.setSender({
				addr: email.to,
				name: options.fromName
			});
			msg.setRecipient(email.from);
			msg.setSubject(options.subject || `Re: ${email.headers.get("subject")}` || "No subject");
			msg.addMessage({
				contentType: options.contentType || "text/plain",
				data: options.body
			});
			const messageId = `<${agentId}@${email.from.split("@")[1]}>`;
			msg.setHeader("In-Reply-To", email.headers.get("Message-ID"));
			msg.setHeader("Message-ID", messageId);
			msg.setHeader("X-Agent-Name", agentName);
			msg.setHeader("X-Agent-ID", agentId);
			if (typeof options.secret === "string") {
				const signedHeaders = await signAgentHeaders(options.secret, agentName, agentId);
				msg.setHeader("X-Agent-Sig", signedHeaders["X-Agent-Sig"]);
				msg.setHeader("X-Agent-Sig-Ts", signedHeaders["X-Agent-Sig-Ts"]);
			}
			if (options.headers) for (const [key, value] of Object.entries(options.headers)) msg.setHeader(key, value);
			await email.reply({
				from: email.to,
				raw: msg.asRaw(),
				to: email.from
			});
			const rawSubject = email.headers.get("subject");
			this._emit("email:reply", {
				from: email.to,
				to: email.from,
				subject: options.subject ?? (rawSubject ? `Re: ${rawSubject}` : void 0)
			});
		});
	}
	/**
	* Send an outbound email via an Email Service binding.
	*
	* Automatically injects agent routing headers (X-Agent-Name, X-Agent-ID).
	* When `secret` is provided, signs headers with HMAC-SHA256 so that replies
	* can be routed back to this agent instance via createSecureReplyEmailResolver.
	*
	* @param options.binding The send_email binding (e.g. this.env.EMAIL)
	* @param options.to Recipient address(es)
	* @param options.from Sender address or {email, name} object
	* @param options.subject Email subject line
	* @param options.text Plain text body (at least one of text/html required)
	* @param options.html HTML body (at least one of text/html required)
	* @param options.replyTo Reply-to address
	* @param options.cc CC recipient(s)
	* @param options.bcc BCC recipient(s)
	* @param options.inReplyTo Message-ID of the email this is replying to (for threading)
	* @param options.headers Additional custom headers
	* @param options.secret Secret for signing agent routing headers
	* @returns The messageId from Email Service
	*/
	async sendEmail(options) {
		return this._tryCatch(async () => {
			const result = await sendAgentEmail(options, {
				agentName: camelCaseToKebabCase(this._ParentClass.name),
				agentId: this.name
			});
			const fromAddr = typeof options.from === "string" ? options.from : options.from.email;
			this._emit("email:send", {
				from: fromAddr,
				to: options.to,
				subject: options.subject
			});
			return result;
		});
	}
	async _tryCatch(fn) {
		try {
			return await fn();
		} catch (e) {
			throw this.onError(e);
		}
	}
	/**
	* Wrap public subclass methods that may be entered outside Lifecycle, such as
	* native Durable Object RPC. Lifecycle hooks already have Agent context.
	*/
	_autoWrapCustomMethods() {
		const basePrototypes = [Agent.prototype];
		const baseMethods = /* @__PURE__ */ new Set();
		for (const baseProto of basePrototypes) {
			let proto = baseProto;
			while (proto && proto !== Object.prototype) {
				const methodNames = Object.getOwnPropertyNames(proto);
				for (const methodName of methodNames) baseMethods.add(methodName);
				proto = Object.getPrototypeOf(proto);
			}
		}
		let proto = Object.getPrototypeOf(this);
		let depth = 0;
		while (proto && proto !== Object.prototype && depth < 10) {
			const methodNames = Object.getOwnPropertyNames(proto);
			for (const methodName of methodNames) {
				const descriptor = Object.getOwnPropertyDescriptor(proto, methodName);
				if (baseMethods.has(methodName) || methodName.startsWith("_") || !descriptor || !!descriptor.get || typeof descriptor.value !== "function") continue;
				const wrappedFunction = withAgentContext(this[methodName]);
				if (this._isCallable(methodName)) callableMetadata.set(wrappedFunction, callableMetadata.get(this[methodName]));
				this.constructor.prototype[methodName] = wrappedFunction;
			}
			proto = Object.getPrototypeOf(proto);
			depth++;
		}
	}
	onError(connectionOrError, error) {
		let theError;
		if (connectionOrError && error) {
			theError = error;
			console.error("Error on websocket connection:", connectionOrError.id, theError);
			console.error("Override onError(connection, error) to handle websocket connection errors");
		} else {
			theError = connectionOrError;
			console.error("Error on server:", theError);
			console.error("Override onError(error) to handle server errors");
		}
		throw theError;
	}
	/**
	* Render content (not implemented in base class)
	*/
	render() {
		throw new Error("Not implemented");
	}
	/**
	* Retry an async operation with exponential backoff and jitter.
	* Retries on all errors by default. Use `shouldRetry` to bail early on non-retryable errors.
	*
	* @param fn The async function to retry. Receives the current attempt number (1-indexed).
	* @param options Retry configuration.
	* @param options.maxAttempts Maximum number of attempts (including the first). Falls back to static options, then 3.
	* @param options.baseDelayMs Base delay in ms for exponential backoff. Falls back to static options, then 100.
	* @param options.maxDelayMs Maximum delay cap in ms. Falls back to static options, then 3000.
	* @param options.shouldRetry Predicate called with the error and next attempt number. Return false to stop retrying immediately. Default: retry all errors.
	* @returns The result of fn on success.
	* @throws The last error if all attempts fail or shouldRetry returns false.
	*/
	async retry(fn, options) {
		const defaults = this._resolvedOptions.retry;
		if (options) validateRetryOptions(options, defaults);
		return tryN(options?.maxAttempts ?? defaults.maxAttempts, fn, {
			baseDelayMs: options?.baseDelayMs ?? defaults.baseDelayMs,
			maxDelayMs: options?.maxDelayMs ?? defaults.maxDelayMs,
			shouldRetry: options?.shouldRetry
		});
	}
	/**
	* Queue a task to be executed in the future
	* @param callback Name of the method to call
	* @param payload Payload to pass to the callback
	* @param options Options for the queued task
	* @param options.retry Retry options for the callback execution
	* @returns The ID of the queued task
	*/
	async queue(callback, payload, options) {
		const id = nanoid(9);
		if (typeof callback !== "string") throw new Error("Callback must be a string");
		if (typeof this[callback] !== "function") throw new Error(`this.${callback} is not a function`);
		if (options?.retry) validateRetryOptions(options.retry, this._resolvedOptions.retry);
		const retryJson = options?.retry ? JSON.stringify(options.retry) : null;
		this.sql`
      INSERT OR REPLACE INTO cf_agents_queues (id, payload, callback, retry_options)
      VALUES (${id}, ${JSON.stringify(payload)}, ${callback}, ${retryJson})
    `;
		this._emit("queue:create", {
			callback,
			id
		});
		this._flushQueue().catch((e) => {
			console.error("Error flushing queue:", e);
		});
		return id;
	}
	async _flushQueue() {
		if (this._flushingQueue) return;
		this._flushingQueue = true;
		try {
			while (true) {
				const result = this.sql`
        SELECT * FROM cf_agents_queues
        ORDER BY created_at ASC
      `;
				if (!result || result.length === 0) break;
				for (const row of result || []) {
					const callback = this[row.callback];
					if (!callback) {
						console.error(`callback ${row.callback} not found`);
						await this.dequeue(row.id);
						continue;
					}
					const { connection, request, email } = __DO_NOT_USE_WILL_BREAK__agentContext.getStore() || {};
					await runInInvocation({
						agent: this,
						connection,
						request,
						email
					}, async () => {
						const { maxAttempts, baseDelayMs, maxDelayMs } = resolveRetryConfig(parseRetryOptions(row), this._resolvedOptions.retry);
						const parsedPayload = JSON.parse(row.payload);
						try {
							await tryN(maxAttempts, async (attempt) => {
								if (attempt > 1) this._emit("queue:retry", {
									callback: row.callback,
									id: row.id,
									attempt,
									maxAttempts
								});
								await callback.bind(this)(parsedPayload, row);
							}, {
								baseDelayMs,
								maxDelayMs
							});
						} catch (e) {
							console.error(`queue callback "${row.callback}" failed after ${maxAttempts} attempts`, e);
							this._emit("queue:error", {
								callback: row.callback,
								id: row.id,
								error: e instanceof Error ? e.message : String(e),
								attempts: maxAttempts
							});
							try {
								await this.onError(e);
							} catch {}
						} finally {
							this.dequeue(row.id);
						}
					}, { detached: true });
				}
			}
		} finally {
			this._flushingQueue = false;
		}
	}
	/**
	* Dequeue a task by ID
	* @param id ID of the task to dequeue
	*/
	dequeue(id) {
		this.sql`DELETE FROM cf_agents_queues WHERE id = ${id}`;
	}
	/**
	* Dequeue all tasks
	*/
	dequeueAll() {
		this.sql`DELETE FROM cf_agents_queues`;
	}
	/**
	* Dequeue all tasks by callback
	* @param callback Name of the callback to dequeue
	*/
	dequeueAllByCallback(callback) {
		this.sql`DELETE FROM cf_agents_queues WHERE callback = ${callback}`;
	}
	/**
	* Get a queued task by ID
	* @param id ID of the task to get
	* @returns The task or undefined if not found
	*/
	getQueue(id) {
		const result = this.sql`
      SELECT * FROM cf_agents_queues WHERE id = ${id}
    `;
		if (!result || result.length === 0) return void 0;
		const row = result[0];
		return {
			...row,
			payload: JSON.parse(row.payload),
			retry: parseRetryOptions(row)
		};
	}
	/**
	* Get all queues by key and value
	* @param key Key to filter by
	* @param value Value to filter by
	* @returns Array of matching QueueItem objects
	*/
	getQueues(key, value) {
		return this.sql`
      SELECT * FROM cf_agents_queues
    `.filter((row) => JSON.parse(row.payload)[key] === value).map((row) => ({
			...row,
			payload: JSON.parse(row.payload),
			retry: parseRetryOptions(row)
		}));
	}
	_facetRunRowsForPrefix(ownerPath) {
		return this.sql`
      SELECT owner_path, owner_path_key, run_id, created_at
      FROM cf_agents_facet_runs
    `.filter((row) => {
			try {
				const rowOwnerPath = JSON.parse(row.owner_path);
				return this._isSameAgentPathPrefix(ownerPath, rowOwnerPath);
			} catch {
				return false;
			}
		});
	}
	_deleteFacetRunRowsForPrefix(ownerPath) {
		for (const row of this._facetRunRowsForPrefix(ownerPath)) this.sql`
        DELETE FROM cf_agents_facet_runs
        WHERE owner_path_key = ${row.owner_path_key}
          AND run_id = ${row.run_id}
      `;
	}
	_lifecycleRouteAddress() {
		if (!this._isFacet) return void 0;
		const key = agentPathKey(this.selfPath);
		return key ? {
			key,
			data: JSON.stringify(this.selfPath)
		} : void 0;
	}
	async _routeLifecycleToRoot(envelope) {
		if (!this._isFacet) return this.lifecycle.route(envelope);
		return (await this._rootAlarmOwner())._cf_routeLifecycle(void 0, envelope);
	}
	async _routeLifecycleToTarget(target, envelope) {
		let targetPath;
		try {
			targetPath = JSON.parse(target.data);
		} catch {
			throw new Error("Lifecycle route target is not a valid Agent path");
		}
		const selfPath = this.selfPath;
		if (!this._isSameAgentPathPrefix(selfPath, targetPath)) throw new Error(`Lifecycle route does not descend from ${JSON.stringify(selfPath)}.`);
		if (selfPath.length === targetPath.length) return this.lifecycle.route(envelope);
		const next = targetPath[selfPath.length];
		if (!this.hasSubAgent(next.className, next.name)) {
			const stalePath = targetPath.slice(0, selfPath.length + 1);
			if (this._isFacet) await (await this._rootAlarmOwner())._cf_cleanupFacetPrefix(stalePath);
			else await this._cf_cleanupFacetPrefix(stalePath);
			return false;
		}
		return (await this._cf_resolveSubAgent(next.className, next.name))._cf_routeLifecycle(target, envelope);
	}
	/** Single native-RPC aperture for routed Lifecycle capabilities. */
	_cf_routeLifecycle(target, envelope) {
		return target ? this._routeLifecycleToTarget(target, envelope) : this.lifecycle.route(envelope);
	}
	async _rootAlarmOwner() {
		const root = this._parentPath[0];
		if (!root) throw new Error("Facet routing requires a root parent.");
		const binding = this.ctx.exports?.[root.className];
		if (!binding) throw new Error(`Unable to resolve root "${root.className}" for facet routing.`);
		return await getAgentByName(binding, root.name);
	}
	_cf_rootResolvesToSelf() {
		const root = this._parentPath[0];
		if (!root) return false;
		const binding = this.ctx.exports?.[root.className];
		if (!binding?.idFromName) return false;
		return binding.idFromName(root.name).equals(this.ctx.id);
	}
	/**
	* Clean root-owned bookkeeping for a sub-tree of facets. This
	* bulk-cancels schedules whose `owner_path` starts with the given
	* prefix and deletes root-side facet fiber recovery leases for the
	* same sub-tree. Used by `deleteSubAgent` and recursive facet
	* destroy. Emits `schedule:cancel` on this agent (the alarm-owning
	* root) for each schedule row removed — the facets being torn down
	* may not be alive to receive the events themselves.
	* @internal
	*/
	async _cf_cleanupFacetPrefix(ownerPath) {
		const prefix = agentPathKey(ownerPath);
		if (prefix) this.scheduler.__DO_NOT_USE_WILL_BREAK__cleanupRoutePrefix(prefix);
		this._deleteFacetRunRowsForPrefix(ownerPath);
		await this._rearmAlarm();
	}
	/**
	* Acquire a root-owned keepAlive ref on behalf of a descendant facet.
	* Facets share the root isolate but cannot set their own physical
	* alarm, so this lets facet work use the root alarm heartbeat.
	* @internal
	*/
	async _cf_acquireFacetKeepAlive(ownerPath) {
		const token = `${agentPathKey(ownerPath) ?? "unknown"}:${nanoid(9)}`;
		this._facetKeepAliveTokens.add(token);
		this._keepAliveRefs++;
		if (this._keepAliveRefs === 1) await this._rearmAlarm();
		return token;
	}
	/**
	* Release a root-owned keepAlive ref previously acquired for a facet.
	* Idempotent so disposer calls can safely race or run twice.
	* @internal
	*/
	async _cf_releaseFacetKeepAlive(token) {
		if (!this._facetKeepAliveTokens.delete(token)) return;
		this._keepAliveRefs = Math.max(0, this._keepAliveRefs - 1);
		await this._rearmAlarm();
	}
	/**
	* Register a facet's durable run row in the root-side index so root
	* alarm housekeeping can dispatch recovery checks into idle facets.
	* The facet remains authoritative for snapshots and recovery hooks.
	* @internal
	*/
	async _cf_registerFacetRun(ownerPath, runId) {
		const ownerPathJson = JSON.stringify(ownerPath);
		const ownerPathKey = agentPathKey(ownerPath);
		if (!ownerPathKey) throw new Error("_cf_registerFacetRun requires a non-empty owner path.");
		this.sql`
      INSERT OR REPLACE INTO cf_agents_facet_runs
        (owner_path, owner_path_key, run_id, created_at)
      VALUES
        (${ownerPathJson}, ${ownerPathKey}, ${runId}, ${Date.now()})
    `;
		await this._rearmAlarm();
	}
	/**
	* Remove a completed facet fiber from the root-side index.
	* @internal
	*/
	async _cf_unregisterFacetRun(ownerPath, runId) {
		const ownerPathKey = agentPathKey(ownerPath);
		this.sql`
      DELETE FROM cf_agents_facet_runs
      WHERE owner_path_key IS ${ownerPathKey}
        AND run_id = ${runId}
    `;
		await this._rearmAlarm();
	}
	/**
	* Schedule a task to be executed in the future
	*
	* Cron schedules are **idempotent by default** — calling `schedule("0 * * * *", "tick")`
	* multiple times with the same callback, cron expression, and payload returns
	* the existing schedule instead of creating a duplicate. Set `idempotent: false`
	* to override this.
	*
	* For delayed and scheduled (Date) types, set `idempotent: true` to opt in
	* to the same dedup behavior (matched on callback + payload). This is useful
	* when calling `schedule()` in `onStart()` to avoid accumulating duplicate
	* rows across Durable Object restarts.
	*
	* @template T Type of the payload data
	* @param when When to execute the task (Date, seconds delay, or cron expression)
	* @param callback Name of the method to call
	* @param payload Data to pass to the callback
	* @param options Options for the scheduled task
	* @param options.retry Retry options for the callback execution
	* @param options.idempotent Dedup by callback+payload. Defaults to `true` for cron, `false` otherwise.
	* @returns Schedule object representing the scheduled task
	*/
	schedule(when, callback, payload, options) {
		return this.scheduler.set(when, callback, payload, options);
	}
	/**
	* Schedule a task to run repeatedly at a fixed interval.
	*
	* This method is **idempotent** — calling it multiple times with the same
	* `callback`, `intervalSeconds`, and `payload` returns the existing schedule
	* instead of creating a duplicate. A different interval or payload is
	* treated as a distinct schedule and creates a new row.
	*
	* This makes it safe to call in `onStart()`, which runs on every Durable
	* Object wake:
	*
	* ```ts
	* async onStart() {
	*   // Only one schedule is created, no matter how many times the DO wakes
	*   await this.scheduleEvery(30, "tick");
	* }
	* ```
	*
	* @template T Type of the payload data
	* @param intervalSeconds Number of seconds between executions
	* @param callback Name of the method to call
	* @param payload Data to pass to the callback
	* @param options Options for the scheduled task
	* @param options.retry Retry options for the callback execution
	* @returns Schedule object representing the scheduled task
	*/
	scheduleEvery(intervalSeconds, callback, payload, options) {
		return this.scheduler.every(intervalSeconds, callback, payload, {
			retry: options?.retry,
			idempotent: options?._idempotent
		});
	}
	/**
	* Get a scheduled task by ID
	* @template T Type of the payload data
	* @param id ID of the scheduled task
	* @returns The Schedule object or undefined if not found
	* @deprecated Use {@link getScheduleById}. This synchronous API cannot cross
	* Durable Object boundaries and throws inside sub-agents.
	*/
	getSchedule(id) {
		return this.scheduler.__DO_NOT_USE_WILL_REMOVE__getSchedule(id);
	}
	/**
	* Get a scheduled task by ID.
	*
	* Unlike the deprecated synchronous {@link getSchedule}, this works inside
	* sub-agents by delegating to the top-level parent that owns the alarm.
	*
	* @param id ID of the scheduled task
	* @returns The Schedule object or undefined if not found
	*/
	getScheduleById(id) {
		return this.scheduler.get(id);
	}
	/**
	* Get scheduled tasks matching the given criteria
	* @template T Type of the payload data
	* @param criteria Criteria to filter schedules
	* @returns Array of matching Schedule objects
	* @deprecated Use {@link listSchedules}. This synchronous API cannot cross
	* Durable Object boundaries and throws inside sub-agents.
	*/
	getSchedules(criteria = {}) {
		return this.scheduler.__DO_NOT_USE_WILL_REMOVE__getSchedules(criteria);
	}
	/**
	* List scheduled tasks matching the given criteria.
	*
	* Unlike the deprecated synchronous {@link getSchedules}, this works inside
	* sub-agents by delegating to the top-level parent that owns the alarm.
	*
	* @param criteria Criteria to filter schedules
	* @returns Array of matching Schedule objects
	*/
	listSchedules(criteria = {}) {
		return this.scheduler.list(criteria);
	}
	/**
	* Cancel a scheduled task.
	*
	* Schedules are isolated by owner: a top-level agent's
	* `cancelSchedule(id)` only matches its own schedules, and a
	* sub-agent's `cancelSchedule(id)` only matches schedules it
	* created. To clear every schedule under a sub-agent (and its
	* descendants), call `parent.deleteSubAgent(Cls, name)` from the
	* parent — that bulk-cleans root-owned bookkeeping via
	* {@link _cf_cleanupFacetPrefix}.
	*
	* @param id ID of the task to cancel
	* @returns true if the task was cancelled, false if the task was not found
	*/
	cancelSchedule(id) {
		return this.scheduler.cancel(id);
	}
	/**
	* Keep the Durable Object alive via alarm heartbeats.
	* Returns a disposer function that stops the heartbeat when called.
	*
	* Use this when you have long-running work and need to prevent the
	* DO from going idle (eviction after ~70-140s of inactivity).
	* The heartbeat fires every `keepAliveIntervalMs` (default 30s) via the
	* alarm system, without creating schedule rows or emitting observability
	* events. Configure via `static options = { keepAliveIntervalMs: 5000 }`.
	*
	* In facets, delegates the physical heartbeat to the root parent
	* because facets do not have independent alarm slots.
	*
	* @example
	* ```ts
	* const dispose = await this.keepAlive();
	* try {
	*   // ... long-running work ...
	* } finally {
	*   dispose();
	* }
	* ```
	*/
	async keepAlive() {
		if (this._isFacet) {
			const root = await this._rootAlarmOwner();
			const token = await root._cf_acquireFacetKeepAlive(this.selfPath);
			let disposed = false;
			return () => {
				if (disposed) return;
				disposed = true;
				const release = root._cf_releaseFacetKeepAlive(token).catch((e) => {
					console.error("[Agent] Failed to release facet keepAlive:", e);
				});
				this.ctx.waitUntil(release);
			};
		}
		this._keepAliveRefs++;
		if (this._keepAliveRefs === 1) await this._rearmAlarm();
		let disposed = false;
		return () => {
			if (disposed) return;
			disposed = true;
			this._keepAliveRefs = Math.max(0, this._keepAliveRefs - 1);
			if (this._keepAliveRefs === 0) this.ctx.waitUntil(this._rearmAlarm().catch((e) => {
				console.error("[Agent] Failed to reschedule alarm after keepAlive dispose:", e);
			}));
		};
	}
	/**
	* Run an async function while keeping the Durable Object alive.
	* The heartbeat is automatically stopped when the function completes
	* (whether it succeeds or throws).
	*
	* This is the recommended way to use keepAlive — it guarantees cleanup
	* so you cannot forget to dispose the heartbeat.
	*
	* @example
	* ```ts
	* const result = await this.keepAliveWhile(async () => {
	*   const data = await longRunningComputation();
	*   return data;
	* });
	* ```
	*/
	async keepAliveWhile(fn) {
		const dispose = await this.keepAlive();
		try {
			return await fn();
		} finally {
			dispose();
		}
	}
	_isTerminalFiberStatus(status) {
		return status === "completed" || status === "aborted" || status === "interrupted" || status === "error";
	}
	_notifyManagedFiberTerminal(fiberId) {
		const row = this._readFiber(fiberId);
		if (row && !this._isTerminalFiberStatus(row.status)) return;
		const waiters = this._managedFiberTerminalWaiters.get(fiberId);
		if (!waiters) return;
		this._managedFiberTerminalWaiters.delete(fiberId);
		for (const resolve of waiters) resolve();
	}
	_waitForManagedFiberTerminal(fiberId) {
		const row = this._readFiber(fiberId);
		if (!row || this._isTerminalFiberStatus(row.status)) return Promise.resolve();
		return new Promise((resolve) => {
			let waiters = this._managedFiberTerminalWaiters.get(fiberId);
			if (!waiters) {
				waiters = /* @__PURE__ */ new Set();
				this._managedFiberTerminalWaiters.set(fiberId, waiters);
			}
			waiters.add(resolve);
		});
	}
	_normalizeFiberStatusFilter(status) {
		if (!status) return null;
		return new Set(Array.isArray(status) ? status : [status]);
	}
	_parseFiberJsonObject(value) {
		if (value === null) return null;
		try {
			const parsed = JSON.parse(value);
			if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
		} catch {}
		return null;
	}
	_parseFiberSnapshot(value) {
		if (value === null) return void 0;
		try {
			return JSON.parse(value);
		} catch {
			return;
		}
	}
	_fiberErrorMessage(error) {
		return error instanceof Error ? error.message : String(error);
	}
	_stringifyFiberSnapshot(snapshot) {
		return snapshot === void 0 ? null : JSON.stringify(snapshot);
	}
	_fiberRecoveryErrorMessage(result) {
		if (result.status === "error") return result.error === void 0 ? null : this._fiberErrorMessage(result.error);
		if (result.status === "aborted" || result.status === "interrupted") return result.reason ?? null;
		return null;
	}
	_applyManagedFiberRecoveryResult(fiberId, result) {
		const completedAt = Date.now();
		const snapshot = this._stringifyFiberSnapshot(result.snapshot);
		const errorMessage = this._fiberRecoveryErrorMessage(result);
		const metadata = result.status === "completed" && result.metadata !== void 0 ? JSON.stringify(result.metadata) : void 0;
		if (metadata !== void 0) {
			this.sql`
        UPDATE cf_agents_fibers
        SET status = ${result.status},
            snapshot = COALESCE(${snapshot}, snapshot),
            metadata_json = ${metadata},
            error_message = ${errorMessage},
            completed_at = ${completedAt}
        WHERE fiber_id = ${fiberId}
          AND status = 'interrupted'
      `;
			this._notifyManagedFiberTerminal(fiberId);
			return;
		}
		this.sql`
      UPDATE cf_agents_fibers
      SET status = ${result.status},
          snapshot = COALESCE(${snapshot}, snapshot),
          error_message = ${errorMessage},
          completed_at = ${completedAt}
      WHERE fiber_id = ${fiberId}
        AND status = 'interrupted'
    `;
		this._notifyManagedFiberTerminal(fiberId);
	}
	_settleManagedFiberExecution(fiberId, outcome, signal) {
		const completedAt = Date.now();
		if (outcome.ok) {
			this.sql`
        UPDATE cf_agents_fibers
        SET status = 'completed', completed_at = ${completedAt}
        WHERE fiber_id = ${fiberId} AND status = 'running'
      `;
			this._notifyManagedFiberTerminal(fiberId);
			return;
		}
		const message = this._fiberErrorMessage(outcome.error);
		const status = signal.aborted ? "aborted" : "error";
		this.sql`
      UPDATE cf_agents_fibers
      SET status = ${status},
          error_message = ${message},
          completed_at = ${completedAt}
      WHERE fiber_id = ${fiberId} AND status = 'running'
    `;
		this._notifyManagedFiberTerminal(fiberId);
	}
	_parseFiberRecoverySnapshot(fiberId, snapshotText) {
		if (!snapshotText) return null;
		try {
			return JSON.parse(snapshotText);
		} catch {
			console.warn(`[Agent] Corrupted snapshot for fiber ${fiberId}, treating as null`);
			return null;
		}
	}
	_fiberRecoveryPayload(ctx, managedRow, startedAt) {
		return {
			fiberId: ctx.id,
			fiberName: ctx.name,
			managed: managedRow !== null,
			recoveryReason: ctx.recoveryReason,
			elapsedMs: startedAt === void 0 ? void 0 : Date.now() - startedAt
		};
	}
	async _withFiberRecoveryTimeout(ctx, operation) {
		const timeoutMs = this._resolvedOptions.fiberRecoveryHookTimeoutMs;
		if (timeoutMs <= 0) return operation();
		let timer;
		try {
			return await Promise.race([operation(), new Promise((_, reject) => {
				timer = setTimeout(() => {
					reject(/* @__PURE__ */ new Error(`Fiber recovery hook timed out after ${timeoutMs}ms for "${ctx.name}" (${ctx.id})`));
				}, timeoutMs);
			})]);
		} finally {
			if (timer !== void 0) clearTimeout(timer);
		}
	}
	_recordFiberRecoveryFailure(ctx, managedRow, error, startedAt, reason = "handler_error") {
		const errorMessage = this._fiberErrorMessage(error);
		const completedAt = Date.now();
		if (managedRow) {
			this.sql`
        UPDATE cf_agents_fibers
        SET status = 'error',
            error_message = ${errorMessage},
            completed_at = ${completedAt}
        WHERE fiber_id = ${ctx.id}
          AND status = 'interrupted'
      `;
			this._notifyManagedFiberTerminal(ctx.id);
		}
		this._emit("fiber:recovery:failed", {
			...this._fiberRecoveryPayload(ctx, managedRow, startedAt),
			error: errorMessage,
			reason
		});
	}
	async _runFiberRecoveryHook(ctx, managedRow) {
		const startedAt = Date.now();
		this._emit("fiber:recovery:attempt", this._fiberRecoveryPayload(ctx, managedRow));
		try {
			const handled = await this._withFiberRecoveryTimeout(ctx, () => this._handleInternalFiberRecovery(ctx));
			if (!handled) {
				const recoveryResult = await this.onFiberRecovered(ctx);
				if (managedRow && recoveryResult) this._applyManagedFiberRecoveryResult(ctx.id, recoveryResult);
			}
			this._emit("fiber:recovery:handled", {
				...this._fiberRecoveryPayload(ctx, managedRow, startedAt),
				status: handled ? "internal" : managedRow ? "managed" : "user"
			});
			return true;
		} catch (e) {
			this._recordFiberRecoveryFailure(ctx, managedRow, e, startedAt);
			console.error(`[Agent] Fiber recovery failed for "${ctx.name}" (${ctx.id}):`, e);
			return false;
		}
	}
	_fiberInspectionFromRow(row) {
		const snapshot = this._parseFiberSnapshot(row.snapshot);
		const inspection = {
			fiberId: row.fiber_id,
			name: row.name,
			status: row.status,
			createdAt: row.created_at
		};
		if (row.idempotency_key !== null) inspection.idempotencyKey = row.idempotency_key;
		if (snapshot !== void 0) inspection.snapshot = snapshot;
		if (row.error_message !== null) inspection.error = row.error_message;
		const metadata = this._parseFiberJsonObject(row.metadata_json);
		if (metadata !== null) inspection.metadata = metadata;
		if (row.started_at !== null) inspection.startedAt = row.started_at;
		if (row.completed_at !== null) inspection.settledAt = row.completed_at;
		return inspection;
	}
	async _waitForManagedFiber(fiberId) {
		const row = this._readFiber(fiberId);
		if (!row || this._isTerminalFiberStatus(row.status)) return row ? this._fiberInspectionFromRow(row) : null;
		if (this._managedFiberExecutions.has(fiberId)) {
			await this._waitForManagedFiberTerminal(fiberId);
			return this.inspectFiber(fiberId);
		}
		await this._checkRunFibers();
		await this._waitForManagedFiberTerminal(fiberId);
		return this.inspectFiber(fiberId);
	}
	_readFiber(fiberId) {
		return this.sql`
      SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
             error_message, created_at, started_at, completed_at
      FROM cf_agents_fibers
      WHERE fiber_id = ${fiberId}
      LIMIT 1
    `[0] ?? null;
	}
	_readFiberByKey(idempotencyKey) {
		return this.sql`
      SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
             error_message, created_at, started_at, completed_at
      FROM cf_agents_fibers
      WHERE idempotency_key = ${idempotencyKey}
      LIMIT 1
    `[0] ?? null;
	}
	_listFiberRows(options) {
		const limit = Math.min(Math.max(options?.limit ?? 50, 1), 100);
		const statuses = this._normalizeFiberStatusFilter(options?.status);
		if (statuses) return [...statuses].flatMap((status) => this._listFiberRowsByStatus(status, limit, options?.name)).sort((a, b) => b.created_at === a.created_at ? b.fiber_id.localeCompare(a.fiber_id) : b.created_at - a.created_at).slice(0, limit);
		if (options?.name) return this.sql`
        SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
               error_message, created_at, started_at, completed_at
        FROM cf_agents_fibers
        WHERE name = ${options.name}
        ORDER BY created_at DESC, fiber_id DESC
        LIMIT ${limit}
      `;
		return this.sql`
      SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
             error_message, created_at, started_at, completed_at
      FROM cf_agents_fibers
      ORDER BY created_at DESC, fiber_id DESC
      LIMIT ${limit}
    `;
	}
	_listFiberRowsByStatus(status, limit, name) {
		if (name) return this.sql`
        SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
               error_message, created_at, started_at, completed_at
        FROM cf_agents_fibers
        WHERE status = ${status} AND name = ${name}
        ORDER BY created_at DESC, fiber_id DESC
        LIMIT ${limit}
      `;
		return this.sql`
      SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
             error_message, created_at, started_at, completed_at
      FROM cf_agents_fibers
      WHERE status = ${status}
      ORDER BY created_at DESC, fiber_id DESC
      LIMIT ${limit}
    `;
	}
	async inspectFiber(fiberId) {
		const row = this._readFiber(fiberId);
		return row ? this._fiberInspectionFromRow(row) : null;
	}
	async inspectFiberByKey(idempotencyKey) {
		const row = this._readFiberByKey(idempotencyKey);
		return row ? this._fiberInspectionFromRow(row) : null;
	}
	async listFibers(options) {
		return this._listFiberRows(options).map((row) => this._fiberInspectionFromRow(row));
	}
	async cancelFiber(fiberId, reason) {
		const row = this._readFiber(fiberId);
		if (!row || this._isTerminalFiberStatus(row.status)) return false;
		const now = Date.now();
		this.sql`
      UPDATE cf_agents_fibers
      SET status = 'aborted',
          error_message = ${reason ?? null},
          completed_at = ${now}
      WHERE fiber_id = ${fiberId}
        AND status IN ('pending', 'running')
    `;
		this._managedFiberAbortControllers.get(fiberId)?.abort(reason);
		this._notifyManagedFiberTerminal(fiberId);
		return true;
	}
	async cancelFiberByKey(idempotencyKey, reason) {
		const row = this._readFiberByKey(idempotencyKey);
		return row ? this.cancelFiber(row.fiber_id, reason) : false;
	}
	async resolveFiber(fiberId, result) {
		const row = this._readFiber(fiberId);
		if (!row || row.status !== "interrupted") return false;
		this._applyManagedFiberRecoveryResult(fiberId, result);
		return true;
	}
	async deleteFibers(options) {
		const terminalStatuses = [...this._normalizeFiberStatusFilter(options?.status) ?? /* @__PURE__ */ new Set([
			"completed",
			"aborted",
			"error"
		])].filter((status) => this._isTerminalFiberStatus(status));
		if (terminalStatuses.length === 0) return 0;
		const limit = Math.min(Math.max(options?.limit ?? 100, 1), 500);
		const settledBefore = options?.settledBefore?.getTime();
		const rows = terminalStatuses.flatMap((status) => this._listTerminalFiberRowsForDelete(status, limit, settledBefore)).sort((a, b) => a.completed_at === b.completed_at ? a.created_at - b.created_at : (a.completed_at ?? 0) - (b.completed_at ?? 0)).slice(0, limit);
		for (const row of rows) this.sql`
        DELETE FROM cf_agents_fibers
        WHERE fiber_id = ${row.fiber_id}
          AND status IN ('completed', 'aborted', 'interrupted', 'error')
      `;
		return rows.length;
	}
	_listTerminalFiberRowsForDelete(status, limit, settledBefore) {
		if (settledBefore !== void 0) return this.sql`
        SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
               error_message, created_at, started_at, completed_at
        FROM cf_agents_fibers
        WHERE status = ${status}
          AND completed_at IS NOT NULL
          AND completed_at < ${settledBefore}
        ORDER BY completed_at ASC, created_at ASC
        LIMIT ${limit}
      `;
		return this.sql`
      SELECT fiber_id, idempotency_key, name, status, snapshot, metadata_json,
             error_message, created_at, started_at, completed_at
      FROM cf_agents_fibers
      WHERE status = ${status}
      ORDER BY completed_at ASC, created_at ASC
      LIMIT ${limit}
    `;
	}
	/**
	* Run a function as a durable fiber. The fiber is registered in SQLite
	* before execution, checkpointable during execution via `ctx.stash()`,
	* and recoverable after eviction via `onFiberRecovered`.
	*
	* - Row created in `cf_agents_runs` at start, deleted on completion
	* - `keepAlive()` held for the duration — prevents idle eviction
	* - Inline (await result) or fire-and-forget (`void this.runFiber(...)`)
	*
	* @param name Informational name for debugging and recovery filtering
	* @param fn Async function to execute. Receives a FiberContext with stash/snapshot.
	* @returns The return value of fn
	*/
	async runFiber(name, fn) {
		return this._runFiberInternal(nanoid(), name, fn);
	}
	/**
	* Internal framework entry point for fibers that need to compose their own
	* recovery metadata with user checkpoint data while preserving the public
	* `this.stash()` behavior.
	*
	* This deliberately stays protected/internal rather than becoming a public
	* `runFiber()` option until the durable execution API needs this generality.
	* @internal
	*/
	async _runFiberWithStashWrapper(name, fn, options) {
		return this._runFiberInternal(nanoid(), name, fn, options);
	}
	async startFiber(name, fn, options) {
		const fiberId = options?.fiberId ?? nanoid();
		const idempotencyKey = options?.idempotencyKey;
		if (options?.fiberId !== void 0 && options.fiberId.trim() === "") throw new Error("fiberId must not be blank");
		if (options?.idempotencyKey !== void 0 && options.idempotencyKey.trim() === "") throw new Error("idempotencyKey must not be blank");
		const existingById = this._readFiber(fiberId);
		const existingByKey = idempotencyKey ? this._readFiberByKey(idempotencyKey) : null;
		if (existingById && existingByKey && existingById.fiber_id !== existingByKey.fiber_id) throw new Error("fiberId and idempotencyKey refer to different fibers");
		if (existingByKey && options?.fiberId && existingByKey.fiber_id !== fiberId) throw new Error("fiberId and idempotencyKey refer to different fibers");
		const existing = existingById ?? existingByKey;
		if (existing) {
			if (options?.waitForCompletion && !this._isTerminalFiberStatus(existing.status)) {
				const waited = await this._waitForManagedFiber(existing.fiber_id);
				if (waited) return {
					...waited,
					accepted: false
				};
				throw new Error(`Fiber ${existing.fiber_id} no longer exists`);
			}
			return {
				...this._fiberInspectionFromRow(existing),
				accepted: false
			};
		}
		const now = Date.now();
		this.sql`
      INSERT INTO cf_agents_fibers
        (fiber_id, idempotency_key, name, status, snapshot, metadata_json,
         error_message, created_at, started_at, completed_at)
      VALUES
        (${fiberId}, ${idempotencyKey ?? null}, ${name}, 'pending', NULL,
         ${options?.metadata ? JSON.stringify(options.metadata) : null}, NULL,
         ${now}, NULL, NULL)
    `;
		const row = this._readFiber(fiberId);
		if (!row) throw new Error(`Failed to create fiber ${fiberId}`);
		const execution = this._executeManagedFiber(fiberId, name, fn).catch((error) => {
			console.error(`[Agent] Managed fiber "${name}" (${fiberId}) failed:`, error);
		}).finally(() => {
			if (this._managedFiberExecutions.get(fiberId) === execution) this._managedFiberExecutions.delete(fiberId);
		});
		this._managedFiberExecutions.set(fiberId, execution);
		if (options?.waitForCompletion) {
			const completed = await this._waitForManagedFiber(fiberId);
			if (!completed) throw new Error(`Fiber ${fiberId} no longer exists`);
			return {
				...completed,
				accepted: true
			};
		}
		return {
			...this._fiberInspectionFromRow(row),
			accepted: true
		};
	}
	async _executeManagedFiber(fiberId, name, fn) {
		const row = this._readFiber(fiberId);
		if (!row || row.status !== "pending") return;
		const controller = new AbortController();
		this._managedFiberAbortControllers.set(fiberId, controller);
		const now = Date.now();
		this.sql`
      UPDATE cf_agents_fibers
      SET status = 'running', started_at = ${now}
      WHERE fiber_id = ${fiberId} AND status = 'pending'
    `;
		const updated = this._readFiber(fiberId);
		if (!updated || updated.status !== "running") {
			this._managedFiberAbortControllers.delete(fiberId);
			return;
		}
		let settled = false;
		try {
			await this._runFiberInternal(fiberId, name, fn, {
				signal: controller.signal,
				managed: true,
				beforeRunCleanup: (outcome) => {
					settled = true;
					this._settleManagedFiberExecution(fiberId, outcome, controller.signal);
				}
			});
		} catch (error) {
			if (!settled) this._settleManagedFiberExecution(fiberId, {
				ok: false,
				error
			}, controller.signal);
		} finally {
			this._managedFiberAbortControllers.delete(fiberId);
		}
	}
	async _runFiberInternal(id, name, fn, options) {
		const signal = options?.signal ?? new AbortController().signal;
		this._withAgentSpan("initialize_fiber", "fiber", {
			"cloudflare.agents.fiber.id": id,
			"cloudflare.agents.fiber.name": name
		}, () => {
			this.sql`
          INSERT INTO cf_agents_runs (id, name, snapshot, created_at)
          VALUES (${id}, ${name}, NULL, ${Date.now()})
        `;
		});
		const startedAt = Date.now();
		this._emit("fiber:run:started", {
			fiberId: id,
			fiberName: name,
			managed: options?.managed === true
		});
		this._runFiberActiveFibers.add(id);
		const writeSnapshot = (data) => {
			const snapshot = JSON.stringify(data);
			this._withAgentSpan("persist_fiber_snapshot", "fiber", {
				"cloudflare.agents.fiber.id": id,
				"cloudflare.agents.fiber.name": name
			}, () => {
				this.sql`
            UPDATE cf_agents_runs SET snapshot = ${snapshot}
            WHERE id = ${id}
          `;
				if (options?.managed) this.sql`
              UPDATE cf_agents_fibers SET snapshot = ${snapshot}
              WHERE fiber_id = ${id}
            `;
			});
		};
		let root;
		let registeredFacetRun = false;
		let dispose = () => {};
		try {
			if ("initialSnapshot" in (options ?? {})) writeSnapshot(options?.initialSnapshot);
			if (this._isFacet) {
				root = await this._rootAlarmOwner();
				await root._cf_registerFacetRun(this.selfPath, id);
				registeredFacetRun = true;
			}
			dispose = await this.keepAlive();
			const stash = (data) => {
				writeSnapshot(options?.wrapStash ? options.wrapStash(data) : data);
			};
			try {
				const result = await _fiberALS.run({
					id,
					signal,
					stash
				}, () => fn({
					id,
					signal,
					stash,
					snapshot: null
				}));
				options?.beforeRunCleanup?.({ ok: true });
				this._emit("fiber:run:completed", {
					fiberId: id,
					fiberName: name,
					managed: options?.managed === true,
					elapsedMs: Date.now() - startedAt
				});
				return result;
			} catch (error) {
				options?.beforeRunCleanup?.({
					ok: false,
					error
				});
				this._emit("fiber:run:failed", {
					fiberId: id,
					fiberName: name,
					managed: options?.managed === true,
					error: this._fiberErrorMessage(error),
					elapsedMs: Date.now() - startedAt
				});
				throw error;
			}
		} finally {
			this._runFiberActiveFibers.delete(id);
			this._withAgentSpan("finalize_fiber", "fiber", {
				"cloudflare.agents.fiber.id": id,
				"cloudflare.agents.fiber.name": name
			}, () => {
				this.sql`DELETE FROM cf_agents_runs WHERE id = ${id}`;
			});
			dispose();
			if (root && registeredFacetRun) try {
				await root._cf_unregisterFacetRun(this.selfPath, id);
			} catch (e) {
				console.error("[Agent] Failed to unregister facet fiber:", e);
			}
		}
	}
	/**
	* Checkpoint data for the currently executing fiber.
	* Uses AsyncLocalStorage to identify the correct fiber,
	* so it works correctly even with concurrent fibers.
	*
	* Throws if called outside a `runFiber` callback.
	*/
	stash(data) {
		const ctx = _fiberALS.getStore();
		if (!ctx) throw new Error("stash() called outside a fiber");
		ctx.stash(data);
	}
	/**
	* Called when an interrupted fiber is detected after restart.
	* Override to implement recovery (re-invoke work, notify clients, etc.).
	*
	* Internal framework fibers are filtered by `_handleInternalFiberRecovery`
	* before this hook runs — users only see their own fibers.
	*
	* Default: logs a warning.
	*/
	async onFiberRecovered(_ctx) {
		console.warn(`[Agent] Fiber "${_ctx.name}" (${_ctx.id}) was interrupted. Override onFiberRecovered to handle recovery.`);
	}
	/**
	* Override point for subclasses to handle internal (framework) fibers
	* before the user's recovery hook fires. Return `true` if handled.
	* @internal
	*/
	async _handleInternalFiberRecovery(_ctx) {
		return false;
	}
	/** @internal Detect fibers left by a dead process (runFiber system). */
	async _checkRunFibers() {
		if (this._runFiberRecoveryInProgress) return;
		this._runFiberRecoveryInProgress = true;
		const scanStartedAt = Date.now();
		const scanDeadlineMs = this._resolvedOptions.fiberRecoveryScanDeadlineMs;
		const fiberRecoveryMaxAgeMs = this._resolvedOptions.fiberRecoveryMaxAgeMs;
		let madeProgress = false;
		try {
			const rows = this.sql`SELECT id, name, snapshot, created_at FROM cf_agents_runs`;
			for (const row of rows) {
				if (scanDeadlineMs > 0 && Date.now() - scanStartedAt > scanDeadlineMs) {
					this._emit("fiber:recovery:skipped", {
						fiberId: row.id,
						fiberName: row.name,
						reason: "scan_deadline_exceeded",
						elapsedMs: Date.now() - scanStartedAt
					});
					break;
				}
				if (this._runFiberActiveFibers.has(row.id)) continue;
				const snapshot = this._parseFiberRecoverySnapshot(row.id, row.snapshot);
				const ctx = {
					id: row.id,
					name: row.name,
					snapshot,
					createdAt: row.created_at,
					recoveryReason: "interrupted"
				};
				const managedRow = this._readFiber(row.id);
				this._emit("fiber:recovery:detected", {
					...this._fiberRecoveryPayload(ctx, managedRow),
					elapsedMs: Date.now() - row.created_at
				});
				this._emit("fiber:run:interrupted", {
					fiberId: row.id,
					fiberName: row.name,
					managed: managedRow !== null,
					recoveryReason: "interrupted",
					elapsedMs: Date.now() - row.created_at
				});
				if (managedRow) {
					if (this._isTerminalFiberStatus(managedRow.status)) {
						this.sql`DELETE FROM cf_agents_runs WHERE id = ${row.id}`;
						madeProgress = true;
						this._notifyManagedFiberTerminal(row.id);
						continue;
					}
					const completedAt = Date.now();
					this.sql`
            UPDATE cf_agents_fibers
            SET status = 'interrupted',
                snapshot = ${row.snapshot},
                completed_at = ${completedAt}
            WHERE fiber_id = ${row.id}
              AND status IN ('pending', 'running')
          `;
					ctx.idempotencyKey = managedRow.idempotency_key ?? void 0;
					ctx.metadata = this._parseFiberJsonObject(managedRow.metadata_json);
					ctx.status = "interrupted";
				}
				const recovered = await this._runFiberRecoveryHook(ctx, managedRow);
				const tooOld = fiberRecoveryMaxAgeMs > 0 && Date.now() - row.created_at > fiberRecoveryMaxAgeMs;
				if (recovered || managedRow || tooOld) {
					if (!recovered && !managedRow && tooOld) this._emit("fiber:recovery:skipped", {
						fiberId: row.id,
						fiberName: row.name,
						reason: "max_age_exceeded",
						elapsedMs: Date.now() - row.created_at
					});
					this.sql`DELETE FROM cf_agents_runs WHERE id = ${row.id}`;
					madeProgress = true;
				}
				if (managedRow) this._notifyManagedFiberTerminal(row.id);
			}
			const ledgerOnlyRows = this.sql`
        SELECT f.fiber_id, f.idempotency_key, f.name, f.status, f.snapshot,
               f.metadata_json, f.error_message, f.created_at, f.started_at,
               f.completed_at
        FROM cf_agents_fibers f
        LEFT JOIN cf_agents_runs r ON r.id = f.fiber_id
        WHERE f.status IN ('pending', 'running')
          AND r.id IS NULL
      `;
			for (const row of ledgerOnlyRows) {
				if (scanDeadlineMs > 0 && Date.now() - scanStartedAt > scanDeadlineMs) {
					this._emit("fiber:recovery:skipped", {
						fiberId: row.fiber_id,
						fiberName: row.name,
						reason: "scan_deadline_exceeded",
						elapsedMs: Date.now() - scanStartedAt,
						managed: true
					});
					break;
				}
				if (this._runFiberActiveFibers.has(row.fiber_id)) continue;
				const snapshot = this._parseFiberRecoverySnapshot(row.fiber_id, row.snapshot);
				const completedAt = Date.now();
				this.sql`
          UPDATE cf_agents_fibers
          SET status = 'interrupted',
              completed_at = ${completedAt}
          WHERE fiber_id = ${row.fiber_id}
            AND status IN ('pending', 'running')
        `;
				const ctx = {
					id: row.fiber_id,
					name: row.name,
					snapshot,
					createdAt: row.created_at,
					idempotencyKey: row.idempotency_key ?? void 0,
					metadata: this._parseFiberJsonObject(row.metadata_json),
					status: "interrupted",
					recoveryReason: "interrupted"
				};
				this._emit("fiber:recovery:detected", {
					...this._fiberRecoveryPayload(ctx, row),
					elapsedMs: Date.now() - row.created_at
				});
				this._emit("fiber:run:interrupted", {
					fiberId: row.fiber_id,
					fiberName: row.name,
					managed: true,
					recoveryReason: "interrupted",
					elapsedMs: Date.now() - row.created_at
				});
				await this._runFiberRecoveryHook(ctx, row);
				madeProgress = true;
				this._notifyManagedFiberTerminal(row.fiber_id);
			}
		} finally {
			this._runFiberRecoveryInProgress = false;
			if (madeProgress) this._recoveryNoProgressScans = 0;
			else this._recoveryNoProgressScans = this._hasPendingFiberRecovery() ? this._recoveryNoProgressScans + 1 : 0;
		}
	}
	/** @internal */
	async _onAlarmHousekeeping() {
		await this._checkRunFibers();
		await this._checkFacetRunFibers();
	}
	_isSameAgentPathPrefix(prefix, path) {
		if (prefix.length > path.length) return false;
		return prefix.every((step, index) => step.className === path[index].className && step.name === path[index].name);
	}
	/**
	* Root-side scan for durable fibers owned by descendant facets.
	* `cf_agents_facet_runs` is only an index; actual snapshots and
	* recovery hooks live in each facet's own `cf_agents_runs` table.
	* @internal
	*/
	async _checkFacetRunFibers() {
		if (this._parentPath.length > 0) return;
		const rows = this.sql`
      SELECT owner_path, owner_path_key, run_id, created_at
      FROM cf_agents_facet_runs
      ORDER BY created_at ASC
    `;
		const firstRowByOwner = /* @__PURE__ */ new Map();
		for (const row of rows) if (!firstRowByOwner.has(row.owner_path_key)) firstRowByOwner.set(row.owner_path_key, row);
		for (const row of firstRowByOwner.values()) {
			let ownerPath;
			try {
				ownerPath = JSON.parse(row.owner_path);
			} catch (e) {
				console.warn(`[Agent] Corrupted facet fiber owner path for ${row.owner_path_key}; pruning stale lease.`, e);
				this.sql`
          DELETE FROM cf_agents_facet_runs
          WHERE owner_path_key = ${row.owner_path_key}
        `;
				continue;
			}
			try {
				if (await this._cf_checkRunFibersForFacet(ownerPath) === 0) this.sql`
            DELETE FROM cf_agents_facet_runs
            WHERE owner_path_key = ${row.owner_path_key}
          `;
			} catch (e) {
				console.error(`[Agent] Facet fiber recovery check failed for ${row.owner_path_key}:`, e);
			}
		}
	}
	/**
	* Dispatch a runFiber recovery check into the facet identified by
	* `ownerPath`. Returns the number of remaining local `cf_agents_runs`
	* rows on the target facet after recovery.
	* @internal
	*/
	async _cf_checkRunFibersForFacet(ownerPath) {
		const selfPath = this.selfPath;
		if (!this._isSameAgentPathPrefix(selfPath, ownerPath)) throw new Error(`Facet fiber owner path does not descend from ${JSON.stringify(selfPath)}.`);
		if (selfPath.length === ownerPath.length) {
			await this._checkRunFibers();
			return this.sql`
        SELECT COUNT(*) as count FROM cf_agents_runs
      `[0]?.count ?? 0;
		}
		const next = ownerPath[selfPath.length];
		if (!this.hasSubAgent(next.className, next.name)) return 0;
		return (await this._cf_resolveSubAgent(next.className, next.name))._cf_checkRunFibersForFacet(ownerPath);
	}
	/**
	* Invoke an RPC method on this Agent or a descendant facet identified
	* by a root-first path. Used by AgentWorkflow to route callbacks and
	* `this.agent` calls back to the exact sub-agent that started a workflow.
	* @internal
	*/
	async _cf_invokeAgentPath(targetPath, method, args) {
		await this.__unsafe_ensureInitialized();
		const selfPath = this.selfPath;
		if (!this._isSameAgentPathPrefix(selfPath, targetPath)) throw new Error(`Workflow origin path does not descend from ${JSON.stringify(selfPath)}.`);
		if (selfPath.length === targetPath.length) {
			const fn = this[method];
			if (isInternalJsStubProp(method) || method in Object.prototype || typeof fn !== "function") throw new Error(`Workflow origin method "${method}" is not callable on ${this.constructor.name}.`);
			return await fn.apply(this, args);
		}
		const next = targetPath[selfPath.length];
		if (!this.hasSubAgent(next.className, next.name)) throw new Error(`Workflow origin sub-agent ${next.className} "${next.name}" no longer exists.`);
		return await (await this._cf_resolveSubAgent(next.className, next.name))._cf_invokeAgentPath(targetPath, method, args);
	}
	/**
	* Recursively destroy a descendant facet identified by
	* `targetPath`. Walks down from `selfPath` until reaching the
	* target's immediate parent, where it cancels the target's
	* parent-owned schedules (and any descendants), removes the
	* target from the registry, and calls `ctx.facets.delete` to
	* wipe the target's storage.
	*
	* Called by a facet's own `destroy()` (via the root) so that
	* `this.destroy()` inside a sub-agent results in the same
	* cleanup as `parent.deleteSubAgent(Cls, name)` from the parent.
	* @internal
	*/
	async _cf_destroyDescendantFacet(targetPath) {
		const selfPath = this.selfPath;
		if (targetPath.length === 0) throw new Error("_cf_destroyDescendantFacet: target path must not be empty.");
		if (selfPath.length >= targetPath.length) throw new Error("_cf_destroyDescendantFacet: target must be a strict descendant.");
		if (!this._isSameAgentPathPrefix(selfPath, targetPath)) throw new Error("_cf_destroyDescendantFacet: target path does not descend from this agent.");
		if (this._parentPath.length === 0) await this._cf_cleanupFacetPrefix(targetPath);
		if (selfPath.length === targetPath.length - 1) {
			const target = targetPath[targetPath.length - 1];
			const ctx = this.ctx;
			if (!ctx.facets) throw new Error("destroy() (delegated from facet) is not supported in this runtime — `ctx.facets` is unavailable. Update to the latest `compatibility_date` in your wrangler.jsonc.");
			try {
				ctx.facets.delete(`${target.className}\0${target.name}`);
			} catch {}
			this._forgetSubAgent(target.className, target.name);
			return;
		}
		const next = targetPath[selfPath.length];
		if (!this.hasSubAgent(next.className, next.name)) return;
		await (await this._cf_resolveSubAgent(next.className, next.name))._cf_destroyDescendantFacet(targetPath);
	}
	/**
	* Whether any runFiber recovery work is still outstanding: orphaned
	* `cf_agents_runs` rows left by a dead process (excluding fibers currently
	* executing in memory, which already hold a keepAlive ref) or managed
	* ledger fibers stuck in a non-terminal state with no live run row.
	*
	* Used by `_rearmAlarm` to arm a follow-up alarm so multi-pass
	* recovery (e.g. after a scan-deadline yield, or while retrying a throwing
	* recovery hook) resumes instead of starving.
	* @internal
	*/
	_hasPendingFiberRecovery() {
		const runRows = this.sql`
      SELECT id FROM cf_agents_runs
    `;
		for (const row of runRows) if (!this._runFiberActiveFibers.has(row.id)) return true;
		return (this.sql`
      SELECT COUNT(*) AS count
      FROM cf_agents_fibers f
      LEFT JOIN cf_agents_runs r ON r.id = f.fiber_id
      WHERE f.status IN ('pending', 'running')
        AND r.id IS NULL
    `[0]?.count ?? 0) > 0;
	}
	async _rearmAlarm() {
		await this._withAgentSpan("schedule_agent_alarm", "alarm", {}, () => this.lifecycle.rearmAlarm());
	}
	/**
	* Contribute Agent-owned alarm work that has not yet been extracted into its
	* own capability. Scheduler contributes independently.
	* @internal
	*/
	async getNextAlarm() {
		const pendingDestroy = await this._pendingDestroyAlarm();
		if (pendingDestroy !== null) return {
			time: pendingDestroy,
			exclusive: true
		};
		const nowMs = Date.now();
		let nextTimeMs = null;
		if (this._keepAliveRefs > 0) nextTimeMs = nowMs + this._resolvedOptions.keepAliveIntervalMs;
		if (this._hasPendingFiberRecovery()) {
			const base = this._resolvedOptions.keepAliveIntervalMs;
			const exp = Math.min(this._recoveryNoProgressScans, FIBER_RECOVERY_BACKOFF_MAX_EXP);
			const recoveryMs = nowMs + Math.min(FIBER_RECOVERY_MAX_BACKOFF_MS, base * 2 ** exp);
			nextTimeMs = nextTimeMs === null ? recoveryMs : Math.min(nextTimeMs, recoveryMs);
		}
		if ((this.sql`
      SELECT COUNT(*) as count FROM cf_agents_facet_runs
    `[0]?.count ?? 0) > 0) {
			const facetRecoveryMs = nowMs + this._resolvedOptions.keepAliveIntervalMs;
			nextTimeMs = nextTimeMs === null ? facetRecoveryMs : Math.min(nextTimeMs, facetRecoveryMs);
		}
		const extensionAlarm = await this._getExtensionAlarm();
		if (extensionAlarm !== null) nextTimeMs = nextTimeMs === null ? extensionAlarm : Math.min(nextTimeMs, extensionAlarm);
		return nextTimeMs;
	}
	/** @internal Transitional alarm contribution for Agent subclasses. */
	_getExtensionAlarm() {
		return null;
	}
	/** Lifecycle alarm callback; Agent housekeeping runs after user alarm work. */
	onAlarm() {}
	/**
	* Run Lifecycle's alarm phase inside Agent's memory-limit circuit breaker.
	*
	* @remarks Use `this.schedule()` for named Agent callbacks. Reusable durable
	* work belongs in a capability with `getNextAlarm()` and `onAlarm()`.
	*/
	async alarm() {
		if (await this._hasPendingDestroy()) {
			await this.destroy();
			return;
		}
		try {
			await this._cf_runAlarmBody();
			await this._cf_clearAlarmMemoryLimitStrikes();
		} catch (error) {
			if (!isDurableObjectMemoryLimitReset(error)) throw error;
			await this._cf_handleAlarmMemoryLimitReset(error);
		}
	}
	/** Run the Lifecycle alarm phase inside Agent's OOM circuit breaker. */
	async _cf_runAlarmBody() {
		await this.lifecycle.alarm();
	}
	/**
	* The schedule-callback names whose alarm rows drive a recovery loop that can
	* deterministically OOM. The base agent has none; chat hosts (`Think`,
	* `AIChatAgent`) override this to return their recovery continuation callbacks
	* so the circuit breaker can surgically back them off / purge them WITHOUT
	* disturbing unrelated scheduled tasks. See {@link _cf_handleAlarmMemoryLimitReset}.
	*/
	_cf_recoveryAlarmCallbacks() {
		return [];
	}
	/**
	* Hook for a host to terminalize ("seal") any in-flight recovery work as an
	* out-of-memory exhaustion when the alarm circuit breaker trips at its strike
	* budget (#1825). Runs at the outermost alarm frame (post-unwind, so writes
	* can land). Default: no-op. Chat hosts override to fire `onExhausted` + the
	* terminal banner and persist the sealed incident.
	*/
	async _cf_sealMemoryLimitedRecovery() {}
	/**
	* Clear the durable memory-limit strike counter after a clean alarm so the
	* circuit breaker counts CONSECUTIVE resets rather than lifetime ones
	* (#1825). Reads first (cheap, usually cached) and only writes when a strike
	* is actually recorded, so the common no-strike path costs no write.
	* Best-effort: a stale strike only costs one extra tolerated spike later.
	*/
	async _cf_clearAlarmMemoryLimitStrikes() {
		try {
			const prior = await this.ctx.storage.get(Agent._CF_OOM_ALARM_STRIKES_KEY);
			if (typeof prior === "number" && prior > 0) await this.ctx.storage.delete(Agent._CF_OOM_ALARM_STRIKES_KEY);
		} catch {}
	}
	/**
	* Alarm-boundary circuit breaker for Durable Object memory-limit resets
	* (#1825). The in-DO recovery budgets (`chatRecovery.maxOomRetries` /
	* `maxRecoveryWork`) only engage if their code runs AND its writes land; a
	* severe OOM can defeat both — thrown before the budget runs (boot hydration),
	* or its own small writes also OOM under memory pressure. In that case the
	* error reaches {@link alarm} and, unhandled, the platform auto-retries the
	* alarm indefinitely (re-running the doomed, billable turn each cycle).
	*
	* This runs at the OUTERMOST frame: the heavy turn has unwound and GC has
	* reclaimed its footprint, so the small writes here can land where mid-turn
	* ones (e.g. give-up's incident read) OOMed. A durable strike counter tolerates
	* a few resets (a transient spike may clear), backing off the recovery rows so
	* the retry is not a hot loop. At the `maxAlarmMemoryLimitStrikes` budget it
	* seals the recovery work and purges the looping rows so the loop — and the
	* bill — stops. Each step is best-effort: even these tiny writes can OOM, but
	* swallowing (not re-throwing) still halts the platform's auto-retry, and a
	* later wake re-arms legitimate schedules.
	*/
	async _cf_handleAlarmMemoryLimitReset(error) {
		const key = Agent._CF_OOM_ALARM_STRIKES_KEY;
		let strikes = 1;
		try {
			const prior = await this.ctx.storage.get(key);
			strikes = (typeof prior === "number" ? prior : 0) + 1;
			await this.ctx.storage.put(key, strikes);
		} catch {}
		const limit = this._resolvedOptions.maxAlarmMemoryLimitStrikes;
		const sealed = strikes >= limit;
		const recoveryCallbacks = this._cf_recoveryAlarmCallbacks();
		console.error(`Alarm hit a Durable Object memory-limit reset (strike ${strikes}/${limit}${sealed ? ", sealing recovery" : ", will retry with backoff"}). Breaking the platform alarm-retry loop (#1825).`, error instanceof Error ? error.message : String(error));
		if (sealed) {
			this.scheduler.__DO_NOT_USE_WILL_BREAK__handleAlarmMemoryLimit({
				callbacks: recoveryCallbacks,
				sealed: true
			});
			try {
				await this._cf_sealMemoryLimitedRecovery();
			} catch {}
			try {
				await this.ctx.storage.delete(key);
			} catch {}
		} else {
			const backoffSeconds = Math.min(300, 30 * strikes);
			const nextTime = Math.floor(Date.now() / 1e3) + backoffSeconds;
			this.scheduler.__DO_NOT_USE_WILL_BREAK__handleAlarmMemoryLimit({
				callbacks: recoveryCallbacks,
				sealed: false,
				nextTime
			});
		}
		try {
			this._emit("alarm:memory_limit_reset", {
				strikes,
				limit,
				sealed,
				error: error instanceof Error ? error.message : String(error)
			});
		} catch {}
		try {
			await this._rearmAlarm();
		} catch {}
	}
	/**
	* Intercept incoming HTTP/WS requests whose URL contains a
	* `/sub/{child-class}/{child-name}` marker and forward them to
	* the facet. The `onBeforeSubAgent` hook fires first (authorize,
	* mutate, or short-circuit). If the hook doesn't return a
	* Response, the framework resolves the facet and hands the
	* request off.
	*
	* After a WebSocket upgrade completes, subsequent frames route
	* directly to the child — the parent is only on the path for the
	* initial request.
	*
	* @experimental The API surface may change before stabilizing.
	*/
	async fetch(request) {
		const ctx = this.ctx;
		const match = parseSubAgentPath(request.url, { knownClasses: ctx.exports ? Object.keys(ctx.exports) : void 0 });
		if (!match) return this.lifecycle.fetch(request);
		const decision = await this.onBeforeSubAgent(request, {
			className: match.childClass,
			name: match.childName
		});
		if (decision instanceof Response) return decision;
		const forwardReq = decision instanceof Request ? decision : request;
		if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
			const acceptHeaders = new Headers(forwardReq.headers);
			const routedUrl = new URL(forwardReq.url);
			routedUrl.pathname = new URL(request.url).pathname;
			acceptHeaders.set(SUB_AGENT_OUTER_URL_HEADER, routedUrl.toString());
			return this.lifecycle.fetch(new Request(forwardReq, { headers: acceptHeaders }));
		}
		return this._cf_forwardToFacet(forwardReq, match);
	}
	broadcast(msg, without) {
		if (this._isFacet) {
			this._cf_broadcastToParentSubAgent(msg, without);
			return;
		}
		for (const connection of this.lifecycle.getConnections()) {
			if (without?.includes(connection.id)) continue;
			if (this._cf_connectionHasSubAgentTarget(connection)) continue;
			connection.send(msg);
		}
	}
	getConnection(id) {
		if (this._isFacet) {
			const stored = this._cf_virtualSubAgentConnections.get(id);
			if (stored) return this._cf_createSubAgentBridgeConnection(stored.meta);
			return;
		}
		const connection = this.lifecycle.getConnection(id);
		if (!connection || this._cf_connectionHasSubAgentTarget(connection)) return;
		return connection;
	}
	*getConnections(tag) {
		if (this._isFacet) {
			for (const stored of this._cf_virtualSubAgentConnections.values()) if (!tag || stored.meta.tags.includes(tag)) yield this._cf_createSubAgentBridgeConnection(stored.meta);
			return;
		}
		for (const connection of this.lifecycle.getConnections(tag)) {
			if (this._cf_connectionHasSubAgentTarget(connection)) continue;
			yield connection;
		}
	}
	_cf_activeSubAgentBridge(connectionId) {
		const context = this._cf_subAgentBridgeContext.getStore();
		if (connectionId !== void 0 && context?.connectionId !== connectionId) return;
		return context?.bridge;
	}
	/**
	* Route a virtual sub-agent connection operation through its live frame
	* bridge, or through the durable root Agent after that frame completes.
	* All operations share one per-connection queue. Facet broadcasts wait for
	* older queued operations; failures do not block later work.
	*/
	_cf_routeSubAgentConnectionOperation(connectionId, operationName, operation) {
		const activeBridge = this._cf_activeSubAgentBridge(connectionId);
		const previousConnectionOperation = this._cf_subAgentConnectionOperationTails.get(connectionId);
		let pending;
		if (activeBridge && !previousConnectionOperation) try {
			pending = Promise.resolve(operation(activeBridge)).then(() => {});
		} catch (error) {
			pending = Promise.reject(error);
		}
		else pending = (previousConnectionOperation ?? Promise.resolve()).then(async () => {
			await operation(new RootSubAgentConnectionBridge(await this._rootAlarmOwner(), connectionId));
		});
		const completion = pending.catch((error) => {
			this._cf_reportSubAgentConnectionOperationFailure(connectionId, operationName, error);
		});
		this._cf_subAgentConnectionOperationTails.set(connectionId, completion);
		this.ctx.waitUntil(completion);
		completion.then(() => {
			if (this._cf_subAgentConnectionOperationTails.get(connectionId) === completion) this._cf_subAgentConnectionOperationTails.delete(connectionId);
		});
	}
	/**
	* Route a facet broadcast after every older connection operation.
	*
	* This barrier is intentionally one-way: facet startup can broadcast before
	* a child connection has finished initializing its tags and protocol flags.
	* Making those later connection operations wait would let the next frame
	* observe stale root-owned metadata.
	*/
	async _cf_routeSubAgentBroadcast(ownerPath, message, without, upstreamBridge) {
		const activeBridge = upstreamBridge ?? this._cf_activeSubAgentBridge();
		const previousOperations = /* @__PURE__ */ new Set([...this._cf_subAgentBroadcastOperationTail ? [this._cf_subAgentBroadcastOperationTail] : [], ...this._cf_subAgentConnectionOperationTails.values()]);
		let pending;
		if (activeBridge && previousOperations.size === 0) try {
			pending = Promise.resolve(activeBridge.broadcast(ownerPath, message, without));
		} catch (error) {
			pending = Promise.reject(error);
		}
		else pending = Promise.all(previousOperations).then(async () => {
			await (await this._rootAlarmOwner())._cf_broadcastToSubAgent(ownerPath, message, without);
		});
		const completion = pending.catch((error) => {
			console.error("[Agent] Sub-agent broadcast operation failed:", {
				operation: "broadcast",
				error
			});
		});
		this._cf_subAgentBroadcastOperationTail = completion;
		this.ctx.waitUntil(completion);
		completion.then(() => {
			if (this._cf_subAgentBroadcastOperationTail === completion) this._cf_subAgentBroadcastOperationTail = void 0;
		});
		await completion;
	}
	_cf_reportSubAgentConnectionOperationFailure(connectionId, operation, error) {
		console.error("[Agent] Sub-agent connection operation failed:", {
			connectionId,
			operation,
			error
		});
	}
	async _cf_broadcastToParentSubAgent(message, without) {
		await this._cf_routeSubAgentBroadcast(this.selfPath, message, without);
	}
	async _cf_broadcastToSubAgent(ownerPath, message, without) {
		if (this._isFacet) {
			await this._cf_routeSubAgentBroadcast(ownerPath, message, without);
			return;
		}
		for (const connection of this.lifecycle.getConnections()) {
			if (without?.includes(connection.id)) continue;
			const targetPath = this._cf_subAgentTargetPath(connection);
			if (!targetPath) continue;
			if (!this._isSameAgentPath(targetPath, ownerPath)) continue;
			connection.send(message);
		}
	}
	async _cf_subAgentConnectionMetas(ownerPath) {
		const metas = [];
		for (const connection of this.lifecycle.getConnections()) {
			const meta = this._cf_subAgentConnectionMetaForPath(connection, ownerPath);
			if (meta) metas.push(meta);
		}
		return metas;
	}
	async _cf_sendToSubAgentConnection(connectionId, message) {
		const connection = this.lifecycle.getConnection(connectionId);
		if (!connection || !this._cf_connectionHasSubAgentTarget(connection)) return;
		connection.send(message);
	}
	async _cf_closeSubAgentConnection(connectionId, code, reason) {
		const connection = this.lifecycle.getConnection(connectionId);
		if (!connection || !this._cf_connectionHasSubAgentTarget(connection)) return;
		connection.close(code, reason);
	}
	async _cf_setSubAgentConnectionState(connectionId, state) {
		const connection = this.lifecycle.getConnection(connectionId);
		if (!connection || !this._cf_connectionHasSubAgentTarget(connection)) return null;
		this._ensureConnectionWrapped(connection);
		connection.setState(state);
		return this._cf_getForwardedSubAgentState(connection);
	}
	_cf_subAgentConnectionMetaForPath(connection, ownerPath) {
		this._ensureConnectionWrapped(connection);
		const outerUri = this._unsafe_getConnectionFlag(connection, CF_SUB_AGENT_OUTER_URL_KEY);
		if (typeof outerUri !== "string") return null;
		const target = this._cf_subAgentPathFromOuterUri(outerUri, ownerPath);
		if (!target) return null;
		const raw = this._cf_getRawConnectionState(connection);
		const rawTags = raw != null && typeof raw === "object" ? raw[CF_SUB_AGENT_TAGS_KEY] : void 0;
		const tags = Array.isArray(rawTags) ? rawTags.filter((tag) => typeof tag === "string") : [...connection.tags];
		return {
			id: connection.id,
			uri: target.uri,
			tags,
			state: this._cf_getForwardedSubAgentState(connection)
		};
	}
	_cf_subAgentTargetPath(connection) {
		this._ensureConnectionWrapped(connection);
		const outerUri = this._unsafe_getConnectionFlag(connection, CF_SUB_AGENT_OUTER_URL_KEY);
		if (typeof outerUri !== "string") return null;
		return this._cf_subAgentPathFromOuterUri(outerUri)?.path ?? null;
	}
	_cf_subAgentPathFromOuterUri(outerUri, stopAt) {
		const ctx = this.ctx;
		const knownClasses = ctx.exports ? Object.keys(ctx.exports) : void 0;
		const path = [...this.selfPath];
		let currentUrl = outerUri;
		while (true) {
			const match = parseSubAgentPath(currentUrl, { knownClasses });
			if (!match) break;
			path.push({
				className: match.childClass,
				name: match.childName
			});
			const rewritten = new URL(currentUrl);
			rewritten.pathname = match.remainingPath;
			currentUrl = rewritten.toString();
			if (stopAt && this._isSameAgentPath(path, stopAt)) return {
				path,
				uri: currentUrl
			};
		}
		if (path.length === this.selfPath.length) return null;
		if (stopAt) return null;
		return {
			path,
			uri: currentUrl
		};
	}
	_isSameAgentPath(a, b) {
		if (a.length !== b.length) return false;
		return a.every((step, index) => step.className === b[index]?.className && step.name === b[index]?.name);
	}
	_cf_connectionHasSubAgentTarget(connection) {
		this._ensureConnectionWrapped(connection);
		return typeof this._unsafe_getConnectionFlag(connection, CF_SUB_AGENT_OUTER_URL_KEY) === "string";
	}
	_cf_connectionTargetsSubAgent(connection) {
		if (!connection.uri) return false;
		const ctx = this.ctx;
		return parseSubAgentPath(connection.uri, { knownClasses: ctx.exports ? Object.keys(ctx.exports) : void 0 }) !== null;
	}
	/**
	* Returns true when the current request is addressed to a child facet of
	* this agent rather than to this agent itself.
	*
	* Chat-style subclasses wrap `onConnect` before the base Agent forwarding
	* wrapper runs, so they need a request-level check to avoid sending their
	* own protocol frames on sockets that are about to be forwarded to a child.
	*/
	_cf_requestTargetsSubAgent(request) {
		const ctx = this.ctx;
		return parseSubAgentPath(request.url, { knownClasses: ctx.exports ? Object.keys(ctx.exports) : void 0 }) !== null;
	}
	async _cf_forwardSubAgentWebSocketConnect(connection, request, options) {
		const routed = await this._cf_resolveSubAgentConnection(connection, request, options);
		if (!routed) return false;
		await routed.child._cf_handleSubAgentWebSocketConnect(this._cf_createSubAgentConnectionBridge(connection), routed.meta);
		return true;
	}
	_cf_createSubAgentConnectionBridge(connection) {
		const upstreamBroadcastBridge = this._isFacet ? this._cf_activeSubAgentBridge(connection.id) : void 0;
		return new SubAgentConnectionBridge(connection, (ownerPath, message, without) => {
			if (upstreamBroadcastBridge) return this._cf_routeSubAgentBroadcast(ownerPath, message, without, upstreamBroadcastBridge);
			return this._cf_broadcastToSubAgent(ownerPath, message, without);
		});
	}
	async _cf_forwardSubAgentWebSocketMessage(connection, message, replyBridge) {
		const routed = await this._cf_resolveSubAgentConnection(connection);
		if (!routed) return false;
		const bridge = this._cf_createSubAgentConnectionBridge(connection);
		await routed.child._cf_handleSubAgentWebSocketMessage(message, bridge, routed.meta, replyBridge ?? bridge);
		return true;
	}
	async _cf_forwardSubAgentWebSocketClose(connection, code, reason, wasClean) {
		const routed = await this._cf_resolveSubAgentConnection(connection);
		if (!routed) return false;
		await routed.child._cf_handleSubAgentWebSocketClose(code, reason, wasClean, this._cf_createSubAgentConnectionBridge(connection), routed.meta);
		return true;
	}
	async _cf_resolveSubAgentConnection(connection, request, options = { gate: false }) {
		this._ensureConnectionWrapped(connection);
		const outerUri = this._unsafe_getConnectionFlag(connection, CF_SUB_AGENT_OUTER_URL_KEY);
		const uri = typeof outerUri === "string" ? outerUri : connection.uri;
		if (!uri) return null;
		const ctx = this.ctx;
		let match = parseSubAgentPath(uri, { knownClasses: ctx.exports ? Object.keys(ctx.exports) : void 0 });
		if (!match) return null;
		if (this._ParentClass.name === match.childClass && this.name === match.childName) {
			const tailUri = new URL(uri);
			tailUri.pathname = match.remainingPath;
			match = parseSubAgentPath(tailUri.toString(), { knownClasses: ctx.exports ? Object.keys(ctx.exports) : void 0 });
			if (!match) return null;
		}
		let forwardReq = request;
		if (request && options.gate) {
			const decision = await this.onBeforeSubAgent(request, {
				className: match.childClass,
				name: match.childName
			});
			if (decision instanceof Response) {
				connection.close(1008, "Sub-agent connection rejected");
				return null;
			}
			forwardReq = decision instanceof Request ? decision : request;
		}
		const child = await this._cf_resolveSubAgent(match.childClass, match.childName);
		const childUri = new URL(forwardReq?.url ?? uri);
		childUri.pathname = match.remainingPath;
		const raw = this._cf_getRawConnectionState(connection);
		const rawTags = raw != null && typeof raw === "object" ? raw[CF_SUB_AGENT_TAGS_KEY] : void 0;
		const tags = Array.isArray(rawTags) ? rawTags.filter((tag) => typeof tag === "string") : [...connection.tags];
		return {
			child,
			meta: {
				id: connection.id,
				uri: childUri.toString(),
				tags,
				state: this._cf_getForwardedSubAgentState(connection),
				requestHeaders: forwardReq ? [...forwardReq.headers] : void 0
			}
		};
	}
	async _cf_handleSubAgentWebSocketConnect(bridge, meta) {
		await this._cf_runWithSubAgentBridge(bridge, meta.id, async () => {
			const connection = this._cf_createSubAgentBridgeConnection(meta);
			const request = new Request(meta.uri ?? "http://placeholder/", { headers: meta.requestHeaders });
			if (await this._cf_forwardSubAgentWebSocketConnect(connection, request, { gate: true })) return;
			if (this.shouldConnectionBeReadonly(connection, { request })) this.setConnectionReadonly(connection, true);
			if (!this.shouldSendProtocolMessages(connection, { request })) this._setConnectionNoProtocol(connection);
			const childTags = await this.getConnectionTags(connection, { request });
			connection.tags = [connection.id, ...childTags.filter((tag) => tag !== connection.id)];
			this._cf_storeVirtualSubAgentConnection(connection);
			await this.onConnect(connection, { request });
			this._cf_storeVirtualSubAgentConnection(connection);
		});
	}
	async _cf_handleSubAgentWebSocketMessage(message, bridge, meta, replyBridge = bridge) {
		const connection = this._cf_createSubAgentBridgeConnection(meta);
		this._cf_storeVirtualSubAgentConnection(connection);
		const replyContext = { bridge: replyBridge };
		try {
			await subAgentRpcReplyContext.run(replyContext, () => this._cf_runWithSubAgentBridge(bridge, meta.id, () => this.onMessage(connection, message)));
		} finally {
			replyContext.bridge = void 0;
		}
	}
	async _cf_handleSubAgentWebSocketClose(code, reason, wasClean, bridge, meta) {
		const connection = this._cf_createSubAgentBridgeConnection(meta);
		this._cf_storeVirtualSubAgentConnection(connection);
		await this._cf_runWithSubAgentBridge(bridge, meta.id, () => this.onClose(connection, code, reason, wasClean));
		this._cf_virtualSubAgentConnections.delete(meta.id);
	}
	async _cf_runWithSubAgentBridge(bridge, connectionId, fn) {
		const context = {
			bridge,
			connectionId
		};
		try {
			return await this._cf_subAgentBridgeContext.run(context, fn);
		} finally {
			context.bridge = void 0;
		}
	}
	_cf_createSubAgentBridgeConnection(meta) {
		let stored = this._cf_virtualSubAgentConnections.get(meta.id);
		if (stored) {
			stored.meta = meta;
			if (stored.connection) {
				stored.connection.uri = meta.uri;
				stored.connection.tags = meta.tags;
				return stored.connection;
			}
		} else {
			stored = { meta };
			this._cf_virtualSubAgentConnections.set(meta.id, stored);
		}
		const owner = this;
		const getStored = () => this._cf_virtualSubAgentConnections.get(meta.id) ?? stored;
		const updateStoredState = (nextState) => {
			const current = this._cf_virtualSubAgentConnections.get(meta.id);
			if (current) current.meta = {
				...current.meta,
				state: nextState
			};
		};
		const connection = {
			id: meta.id,
			uri: meta.uri,
			tags: meta.tags,
			get state() {
				return getStored().meta.state;
			},
			setState(next) {
				const currentState = getStored().meta.state;
				const state = typeof next === "function" ? next(currentState) : next;
				updateStoredState(state);
				owner._cf_routeSubAgentConnectionOperation(meta.id, "setState", (bridge) => bridge.setState(state));
				return state;
			},
			send(message) {
				owner._cf_routeSubAgentConnectionOperation(meta.id, "send", (bridge) => bridge.send(message));
			},
			close(code, reason) {
				owner._cf_routeSubAgentConnectionOperation(meta.id, "close", (bridge) => bridge.close(code, reason));
			},
			addEventListener() {},
			removeEventListener() {}
		};
		stored.connection = connection;
		this._ensureConnectionWrapped(connection);
		return connection;
	}
	_cf_storeVirtualSubAgentConnection(connection) {
		this._unsafe_setConnectionFlag(connection, CF_SUB_AGENT_TAGS_KEY, [...connection.tags]);
		const stored = this._cf_virtualSubAgentConnections.get(connection.id);
		this._cf_virtualSubAgentConnections.set(connection.id, {
			meta: {
				id: connection.id,
				uri: connection.uri,
				tags: [...connection.tags],
				state: this._cf_getRawConnectionState(connection)
			},
			connection: stored?.connection ?? connection
		});
	}
	async _cf_hydrateSubAgentConnectionsFromRoot() {
		if (!this._isFacet || this._parentPath.length === 0) return;
		if (this._cf_rootResolvesToSelf()) return;
		const metas = await (await this._rootAlarmOwner())._cf_subAgentConnectionMetas(this.selfPath);
		for (const meta of metas) this._cf_virtualSubAgentConnections.set(meta.id, { meta });
	}
	_cf_getRawConnectionState(connection) {
		this._ensureConnectionWrapped(connection);
		return this._rawStateAccessors.get(connection)?.getRaw() ?? null;
	}
	_cf_getForwardedSubAgentState(connection) {
		const raw = this._cf_getRawConnectionState(connection);
		if (raw == null || typeof raw !== "object") return raw;
		const { [CF_SUB_AGENT_OUTER_URL_KEY]: _, ...rest } = raw;
		return Object.keys(rest).length > 0 ? rest : null;
	}
	/**
	* Parent-side middleware hook. Fires before a request is
	* forwarded into a facet sub-agent. Mirrors `onBeforeConnect` /
	* `onBeforeRequest`.
	*
	*   - return `void` (default) → forward the original request
	*   - return `Request`        → forward this (modified) request
	*   - return `Response`       → return this response to the
	*                               client; do not wake the child
	*
	* Default implementation: return void (permissive).
	*
	* The hook receives the **original** request with its URL intact —
	* including the `/sub/{class}/{name}` segment. The routing
	* decision for which facet to wake is fixed at parse time, so if
	* you return a modified `Request`, its headers, body, method, and
	* query string flow through to the child, but the **pathname**
	* the child sees is always the tail after `/sub/{class}/{name}`.
	* Customize via headers/body rather than URL-rewriting.
	*
	* WebSocket upgrade requests flow through this hook the same way as
	* plain HTTP. If you return a mutated `Request`, make sure it still
	* carries the original `Upgrade: websocket` and `Sec-WebSocket-*`
	* headers — the simplest safe recipe is to clone the incoming
	* request's headers (via `new Headers(req.headers)`) and only add
	* or replace entries, rather than constructing a fresh `Headers`
	* object from scratch.
	*
	* @experimental The API surface may change before stabilizing.
	*
	* @example
	* ```ts
	* class Inbox extends Agent {
	*   override async onBeforeSubAgent(req, { className, name }) {
	*     // Strict registry gate
	*     if (!this.hasSubAgent(className, name)) {
	*       return new Response("Not found", { status: 404 });
	*     }
	*   }
	* }
	* ```
	*/
	async onBeforeSubAgent(_request, _child) {}
	/**
	* Resolve the facet Fetcher for the match and forward the
	* request to it with `/sub/{class}/{name}` stripped.
	*
	* @internal
	*/
	async _cf_forwardToFacet(req, match) {
		let fetcher;
		try {
			fetcher = await this._cf_resolveSubAgent(match.childClass, match.childName);
		} catch (err) {
			const message = err instanceof Error ? err.message : String(err);
			console.error("[agents] sub-agent route failed:", message);
			if (/null character/i.test(message) || /reserved/i.test(message)) return new Response("Bad Request", { status: 400 });
			return new Response("Not Found", { status: 404 });
		}
		const rewritten = new URL(req.url);
		rewritten.pathname = match.remainingPath;
		const forwardedHeaders = new Headers(req.headers);
		const forwardedInit = {
			method: req.method,
			headers: forwardedHeaders
		};
		if (req.headers.get("Upgrade")?.toLowerCase() === "websocket") forwardedHeaders.set(SUB_AGENT_OUTER_URL_HEADER, req.url);
		if (req.body && req.method !== "GET" && req.method !== "HEAD") forwardedInit.body = req.body;
		const forwarded = new Request(rewritten, forwardedInit);
		return fetcher.fetch(forwarded);
	}
	/**
	* Bridge method used by `getSubAgentByName`. Resolves the facet
	* on each call (idempotent via `subAgent`) and dispatches one
	* RPC method. Stateless — no cached references.
	*
	* @internal
	*/
	async _cf_invokeSubAgent(className, name, method, args) {
		const stub = await this._cf_resolveSubAgent(className, name);
		return await this._cf_invokeStubMethod(stub, className, method, args);
	}
	/**
	* Bridge method used by `parentAgent()` when the requested parent is
	* itself a facet (and therefore has no top-level env namespace).
	* The root receives the full root-first target path, then each hop
	* delegates to the next facet using that facet's own `ctx.facets`.
	*
	* @internal
	*/
	async _cf_invokeSubAgentPath(path, method, args) {
		const [self, next, ...rest] = path;
		if (!self) throw new Error(`Sub-agent path invocation requires a non-empty path.`);
		const ownClassName = this.constructor.name;
		if (self.className !== ownClassName || self.name !== this.name) throw new Error(`Sub-agent path invocation reached ${ownClassName}("${this.name}") but expected ${self.className}("${self.name}").`);
		if (!next) return await this._cf_invokeStubMethod(this, this.constructor.name, method, args);
		const child = await this._cf_resolveSubAgent(next.className, next.name);
		if (rest.length === 0) return await this._cf_invokeStubMethod(child, next.className, method, args);
		return await child._cf_invokeSubAgentPath([next, ...rest], method, args);
	}
	async _cf_invokeStubMethod(stub, className, method, args) {
		const handle = stub;
		if (typeof handle[method] !== "function") throw new Error(`Method "${method}" not found on ${className}.`);
		return await handle[method](...args);
	}
	/**
	* Initialize this agent as a facet in a single RPC.
	*
	* Runs entirely inside the child's isolate, so every storage write
	* and `onStart()` I/O is owned by the child DO. This replaces the
	* previous "construct a Request in the parent DO and `stub.fetch()`
	* it on the child" handshake, whose native I/O was tied to the
	* parent and triggered "Cannot perform I/O on behalf of a different
	* Durable Object" on the child.
	*
	* We set `_isFacet` eagerly (before `__unsafe_ensureInitialized`
	* runs `onStart()`) so any code that legitimately branches on it
	* — e.g. skipping parent-owned alarms in schedule guards — sees
	* the flag during the first `onStart()` run. Protocol broadcasts are
	* suppressed only during this bootstrap window; afterward, facets can
	* broadcast to their own WebSocket clients reached via sub-agent
	* routing.
	*
	* The facet's logical name is persisted separately from its routing id.
	* Legacy facets used the logical name directly as `ctx.id.name`; newer
	* facets can use path-scoped routing ids while preserving `this.name`.
	*
	* @internal Called by {@link subAgent}.
	*/
	async _cf_initAsFacet(name, parentPath = [], identityName = name) {
		const routedName = this.lifecycle.name;
		if (routedName !== identityName) throw new Error(`Facet bootstrap mismatch: expected routed identity "${identityName}" but got "${routedName}". This usually means the parent passed the wrong id to ctx.facets.get(). See _cf_resolveSubAgent.`);
		this._isFacet = true;
		this._facetName = name;
		this._parentPath = parentPath;
		await Promise.all([
			this.ctx.storage.put("cf_agents_is_facet", true),
			this.ctx.storage.put("cf_agents_facet_name", name),
			this.ctx.storage.put("cf_agents_parent_path", parentPath)
		]);
		await this.__unsafe_ensureInitialized();
	}
	get name() {
		const routedName = this.lifecycle.name;
		return this._facetName ?? logicalNameFromPathV2Identity(routedName) ?? routedName;
	}
	/**
	* Ancestor chain for this agent, root-first. Empty for top-level
	* DOs. Populated at facet init time; survives hibernation.
	*
	* @example
	* ```ts
	* class Chat extends Agent {
	*   onStart() {
	*     console.log("chat started under:", this.parentPath);
	*     // → [{ className: "Tenant", name: "acme" }, { className: "Inbox", name: "alice" }]
	*   }
	* }
	* ```
	*
	* @experimental The API surface may change before stabilizing.
	*/
	get parentPath() {
		return this._parentPath;
	}
	/**
	* Ancestor chain + self, root-first. Convenient for logging.
	*
	* @experimental The API surface may change before stabilizing.
	*/
	get selfPath() {
		return [...this._parentPath, {
			className: this.constructor.name,
			name: this.name
		}];
	}
	/**
	* Resolve a typed parent stub for this facet's **immediate** parent
	* agent.
	*
	* Symmetric with `subAgent(Cls, name)`: while `subAgent` opens a
	* stub from parent to child, `parentAgent` opens one from child
	* to parent. Pass the direct parent's class reference — the
	* framework verifies it matches the last entry of
	* `this.parentPath` at runtime. If the parent is a top-level
	* Durable Object, the framework returns the normal namespace stub.
	* If the parent is itself a facet, the framework returns a bridge
	* proxy that routes method calls through the root/supervisor and
	* then down the recorded facet path.
	*
	* `this.parentPath` is root-first, so the direct parent is the
	* **last** entry: `this.parentPath.at(-1)`. For grandparents and
	* further ancestors, iterate `this.parentPath` and use
	* `getAgentByName(env.X, this.parentPath[i].name)` directly.
	*
	* For top-level parents, the framework first checks `env[Cls.name]`,
	* then falls back to the Worker `exports` object. This supports
	* custom binding names as long as the parent class is exported under
	* its class name.
	*
	* Facet-parent stubs route normal HTTP `.fetch()` calls through the
	* same root bridge as RPC methods. WebSocket upgrade requests are
	* not supported yet because WebSocket handles cannot be serialized
	* over RPC.
	*
	* @experimental The API surface may change before stabilizing.
	*
	* @throws If this agent is not a facet (no parent).
	* @throws If `Cls.name` doesn't match the recorded direct-parent
	*         class (guards against accidentally reaching the wrong
	*         DO, especially in nested Root → Mid → Leaf chains).
	* @throws If no namespace is found for a top-level parent, or no
	*         root namespace is available for a facet parent bridge.
	*
	* @example
	* ```ts
	* class Chat extends AIChatAgent<Env> {
	*   async onChatMessage(...) {
	*     const inbox = await this.parentAgent(Inbox);
	*     const memory = await inbox.getSharedMemory("facts");
	*     // ...
	*   }
	* }
	* ```
	*/
	async parentAgent(cls) {
		const parent = this._parentPath[this._parentPath.length - 1];
		if (!parent) throw new Error(`parentAgent(): ${this.constructor.name} is not a facet — only sub-agents (spawned via \`subAgent()\`) have a parent.`);
		if (cls.name !== parent.className) throw new Error(`parentAgent(${cls.name}): this facet's recorded parent class is "${parent.className}", not "${cls.name}". Pass the class whose constructor actually spawned this facet.`);
		if (this._parentPath.length > 1) return await this._cf_parentAgentFacetProxy(cls.name, this._parentPath);
		const binding = this._cf_getTopLevelNamespaceByClassName(cls.name);
		if (!binding) throw new Error(`parentAgent(${cls.name}): no top-level namespace for "${cls.name}" was found in env or worker exports. Make sure the parent class is exported under that class name and registered as a Durable Object binding.`);
		return await getAgentByName(binding, parent.name);
	}
	_cf_getTopLevelNamespaceByClassName(className) {
		return this._cf_asDurableObjectNamespace(this.env[className]) ?? this._cf_asDurableObjectNamespace(exports[className]);
	}
	_cf_asDurableObjectNamespace(candidate) {
		const binding = candidate;
		return binding?.idFromName ? binding : void 0;
	}
	async _cf_parentAgentFacetProxy(className, parentPath) {
		const [root] = parentPath;
		if (!root) throw new Error(`parentAgent(${className}): parent path is empty.`);
		const rootBinding = this._cf_getTopLevelNamespaceByClassName(root.className);
		if (!rootBinding) throw new Error(`parentAgent(${className}): direct parent is a facet, but no top-level root namespace "${root.className}" was found in env or worker exports to bridge the call.`);
		const rootStubPromise = getAgentByName(rootBinding, root.name);
		const targetPath = parentPath.map((step) => ({ ...step }));
		const invokeBridge = async (method, args) => {
			return await (await rootStubPromise)._cf_invokeSubAgentPath(targetPath, method, args);
		};
		const owner = this;
		return new Proxy({}, { get(_target, prop) {
			if (isInternalJsStubProp(prop)) return void 0;
			if (typeof prop !== "string") return void 0;
			if (prop === "fetch") return async (input, init) => {
				if (owner._cf_isWebSocketUpgradeRequest(input, init)) throw new Error(`parentAgent(${className}).fetch() does not support WebSocket upgrade requests yet. Use externally routed sub-agent URLs for WebSocket connections.`);
				return await invokeBridge(prop, [input, init]);
			};
			return async (...args) => {
				return await invokeBridge(prop, args);
			};
		} });
	}
	_cf_isWebSocketUpgradeRequest(input, init) {
		const initHeaders = init?.headers ? new Headers(init.headers) : void 0;
		const requestHeaders = input instanceof Request ? new Headers(input.headers) : void 0;
		return initHeaders?.get("Upgrade")?.toLowerCase() === "websocket" || requestHeaders?.get("Upgrade")?.toLowerCase() === "websocket";
	}
	/**
	* Get or create a named sub-agent — a child Durable Object (facet)
	* with its own isolated SQLite storage running on the same machine.
	*
	* The child class must extend `Agent` and be exported from the worker
	* entry point. The first call for a given name triggers the child's
	* `onStart()`. Subsequent calls return the existing instance.
	*
	* @experimental The API surface may change before stabilizing.
	*
	* @param cls The Agent subclass (must be exported from the worker)
	* @param name Unique name for this child instance
	* @returns A typed RPC stub for calling methods on the child
	*
	* @example
	* ```typescript
	* const searcher = await this.subAgent(SearchAgent, "main-search");
	* const results = await searcher.search("cloudflare agents");
	* ```
	*/
	async subAgent(cls, name) {
		return await this._cf_resolveSubAgent(cls.name, name);
	}
	async onAgentToolStart(_run) {}
	async onAgentToolFinish(_run, _result) {}
	/**
	* Parent hook fired (best-effort) whenever a child agent-tool run emits a
	* `reportProgress` signal that is forwarded through this parent's tail. Use it
	* to meter / steer / surface progress server-side. Fires for both awaited and
	* detached runs; it is NOT durable — after eviction a detached run's latest
	* snapshot is read from `inspectAgentToolRun().progress` on reconcile instead.
	*/
	async onProgress(_run, _progress) {}
	/**
	* Emit an ephemeral progress signal from a sub-agent that is currently running
	* as an agent tool. Rides the child's active turn stream as a transient
	* `data-agent-progress` part (re-broadcast to the parent's clients + surfaced
	* in `useAgentToolEvents`) and persists a latest-wins snapshot for recovery /
	* inspection. A no-op (with a dev warning) on the base `Agent`, which has no
	* streaming turn — overridden by chat hosts (`@cloudflare/think`,
	* `AIChatAgent`). See `design/rfc-detached-agent-tools.md`.
	*/
	async reportProgress(_progress, _options) {
		console.warn("[agents] reportProgress() is only supported on chat agents (@cloudflare/think, AIChatAgent) running as an agent tool; ignoring on base Agent.");
	}
	async runAgentTool(cls, options) {
		const runId = options.runId ?? nanoid(12);
		const agentType = cls.name;
		const detached = this._parseDetachedOption(options.detached);
		const existing = this._readAgentToolRun(runId);
		if (existing) {
			if (detached) {
				if (!this._isAgentToolRowHardTerminal(existing.status)) await this._armDetachedBackbone();
				return {
					runId,
					agentType,
					status: "running"
				};
			}
			if (existing.status === "completed" || existing.status === "error" || existing.status === "aborted") {
				if (existing.status === "completed" && existing.output_json == null) try {
					const child = await this.subAgent(cls, runId);
					const inspection = await this._asAgentToolChildAdapter(child).inspectAgentToolRun(runId);
					if (inspection?.status === "completed") {
						const result = this._terminalResultFromInspection(agentType, inspection);
						this._updateAgentToolTerminal(runId, result, inspection.completedAt);
						return result;
					}
				} catch {}
				return this._resultFromAgentToolRow(existing);
			}
			let reattachReason;
			let childTornDown = false;
			try {
				const child = await this.subAgent(cls, runId);
				const adapter = this._asAgentToolChildAdapter(child);
				const reattach = await this._reattachAgentToolRunToTerminal(adapter, existing, 1, this._resolvedOptions.agentToolReattachNoProgressTimeoutMs, this._resolvedOptions.agentToolReattachMaxWindowMs);
				if (reattach.result) {
					await this._finishAgentToolRun(this._agentToolRunInfoFromRow(existing), reattach.result, {
						sequence: reattach.sequence,
						completedAt: reattach.completedAt
					});
					return reattach.result;
				}
				reattachReason = reattach.reason;
				childTornDown = await this._teardownGivenUpAgentToolChild(adapter, runId, reattach.reason);
			} catch {}
			return await this._replayAndInterruptAgentToolRun(existing, this._interruptedMessageForReason(reattachReason), {
				reason: reattachReason,
				childStillRunning: !childTornDown
			});
		}
		const displayOrder = options.displayOrder ?? 0;
		const inputPreview = options.inputPreview ?? this._defaultAgentToolPreview(options.input);
		const displayJson = options.display !== void 0 ? JSON.stringify(options.display) : null;
		const inputPreviewJson = inputPreview !== void 0 ? JSON.stringify(inputPreview) : null;
		const startedAt = Date.now();
		if (this._activeAgentToolRunCount() >= this.maxConcurrentAgentTools) {
			const error = `maxConcurrentAgentTools (${this.maxConcurrentAgentTools}) exceeded`;
			this.sql`
        INSERT INTO cf_agent_tool_runs (
          run_id, parent_tool_call_id, agent_type, input_preview,
          input_redacted, status, error_message, display_metadata,
          display_order, started_at, completed_at
        ) VALUES (
          ${runId}, ${options.parentToolCallId ?? null}, ${agentType},
          ${inputPreviewJson}, 1, 'error', ${error}, ${displayJson},
          ${displayOrder}, ${startedAt}, ${Date.now()}
        )
      `;
			this._broadcastAgentToolEvent(options.parentToolCallId, 0, {
				kind: "started",
				runId,
				agentType,
				inputPreview,
				order: displayOrder,
				display: options.display
			});
			this._broadcastAgentToolEvent(options.parentToolCallId, 1, {
				kind: "error",
				runId,
				error
			});
			return {
				runId,
				agentType,
				status: "error",
				error
			};
		}
		const detachedMaxBudgetAt = detached ? startedAt + (detached.maxBudgetMs ?? this._resolvedOptions.detachedMaxBudgetMs) : null;
		const detachedNoProgressBudgetMs = detached ? detached.noProgressBudgetMs ?? this._resolvedOptions.detachedNoProgressBudgetMs : null;
		const detachedOnMilestonesJson = detached?.onMilestones ? JSON.stringify(detached.onMilestones) : null;
		this.sql`
      INSERT INTO cf_agent_tool_runs (
        run_id, parent_tool_call_id, agent_type, input_preview,
        input_redacted, status, display_metadata, display_order, started_at,
        detached, detached_on_finish, detached_notify_source,
        detached_max_budget_at, detached_no_progress_budget_ms,
        detached_on_milestones
      ) VALUES (
        ${runId}, ${options.parentToolCallId ?? null}, ${agentType},
        ${inputPreviewJson}, 1, 'starting', ${displayJson}, ${displayOrder},
        ${startedAt}, ${detached ? 1 : 0}, ${detached?.onFinishName ?? null},
        ${detached?.notifySource ?? null}, ${detachedMaxBudgetAt},
        ${detachedNoProgressBudgetMs}, ${detachedOnMilestonesJson}
      )
    `;
		const runInfo = {
			runId,
			parentToolCallId: options.parentToolCallId,
			agentType,
			inputPreview,
			status: "starting",
			display: options.display,
			...detached?.notifySource !== void 0 ? { notifySource: detached.notifySource } : {},
			displayOrder,
			startedAt
		};
		await this.onAgentToolStart(runInfo);
		this._broadcastAgentToolEvent(options.parentToolCallId, 0, {
			kind: "started",
			runId,
			agentType,
			inputPreview,
			order: displayOrder,
			display: options.display
		});
		const child = await this.subAgent(cls, runId);
		const adapter = this._asAgentToolChildAdapter(child);
		const childStart = await adapter.startAgentToolRun(options.input, { runId });
		this._markAgentToolRunning(runId);
		if (detached) {
			if (options.signal) console.warn(`[agents] runAgentTool: \`signal\` is ignored for a detached run (${runId}); a detached child must outlive the spawning turn. Use cancelAgentTool(runId) to cancel it.`);
			await this._armDetachedBackbone({ resetCadence: true });
			this._maybeWarnDetachedLiveCount();
			this.ctx.waitUntil(this._detachedFastPath(runInfo, cls, runId));
			return {
				runId,
				agentType,
				status: "running"
			};
		}
		let sequence = 1;
		let parentAbortListener;
		if (options.signal) if (options.signal.aborted) {
			await adapter.cancelAgentToolRun(runId, options.signal.reason);
			const result = {
				runId,
				agentType,
				status: "aborted",
				error: options.signal.reason instanceof Error ? options.signal.reason.message : String(options.signal.reason ?? "cancelled")
			};
			await this._finishAgentToolRun(runInfo, result, { sequence });
			return result;
		} else {
			parentAbortListener = () => {
				adapter.cancelAgentToolRun(runId, options.signal?.reason);
			};
			options.signal.addEventListener("abort", parentAbortListener, { once: true });
		}
		try {
			if (adapter.tailAgentToolRun) {
				const stream = await adapter.tailAgentToolRun(runId, { afterSequence: -1 });
				sequence = (await this._forwardAgentToolStream(stream, options.parentToolCallId, runId, sequence, options.signal)).next;
			} else {
				const chunks = await adapter.getAgentToolChunks(runId);
				sequence = this._broadcastAgentToolChunks(options.parentToolCallId, runId, chunks, sequence);
			}
			if (options.signal?.aborted) {
				await adapter.cancelAgentToolRun(runId, options.signal.reason);
				const result = {
					runId,
					agentType,
					status: "aborted",
					error: options.signal.reason instanceof Error ? options.signal.reason.message : String(options.signal.reason ?? "cancelled")
				};
				await this._finishAgentToolRun(runInfo, result, { sequence });
				return result;
			}
			const inspection = await adapter.inspectAgentToolRun(runId) ?? childStart;
			const result = this._terminalResultFromInspection(agentType, inspection);
			await this._finishAgentToolRun(runInfo, result, {
				sequence,
				completedAt: inspection.completedAt
			});
			return result;
		} catch (error) {
			if (options.signal?.aborted) {
				await adapter.cancelAgentToolRun(runId, options.signal.reason);
				const result = {
					runId,
					agentType,
					status: "aborted",
					error: options.signal.reason instanceof Error ? options.signal.reason.message : String(options.signal.reason ?? "cancelled")
				};
				await this._finishAgentToolRun(runInfo, result, { sequence });
				return result;
			}
			const result = {
				runId,
				agentType,
				status: "error",
				error: error instanceof Error ? error.message : String(error)
			};
			await this._finishAgentToolRun(runInfo, result, { sequence });
			return result;
		} finally {
			if (parentAbortListener && options.signal) options.signal.removeEventListener("abort", parentAbortListener);
		}
	}
	/**
	* Cancel an agent-tool run by id. Idempotent: cancelling an already-terminal
	* run is a no-op. Detached runs deliver through the guarded ledger so a wired
	* `onFinish` fires once with `status: "aborted"`; awaited runs leave terminal
	* observation to the awaiting/recovery path, avoiding duplicate finish hooks.
	*/
	async cancelAgentTool(runId, reason) {
		const row = this._readAgentToolRun(runId);
		if (!row) return;
		if (this._isAgentToolRowHardTerminal(row.status)) return;
		const isDetached = row.detached === 1;
		const message = reason instanceof Error ? reason.message : String(reason ?? "cancelled by parent");
		try {
			const child = await this._cf_resolveSubAgent(row.agent_type, runId);
			await this._asAgentToolChildAdapter(child).cancelAgentToolRun(runId, reason);
		} catch {}
		if (!isDetached) return;
		await this._deliverDetachedTerminal(runId, "finish", {
			runId,
			agentType: row.agent_type,
			status: "aborted",
			error: message
		});
	}
	/**
	* Parse + validate the `detached` option. Returns `null` for a non-detached
	* run, or the normalized config (with the validated `onFinish` method name)
	* for a detached one. Throws if `onFinish` does not name a method on this
	* agent — closures cannot survive Durable Object eviction, so the durable
	* hook is referenced by method name (the same contract as `schedule`).
	*/
	_parseDetachedOption(detached) {
		if (!detached) return null;
		if (detached === true) return {};
		let onFinishName = detached.onFinish;
		const notifySource = typeof detached.notify === "object" ? detached.notify.source : void 0;
		if (onFinishName !== void 0) {
			if (typeof this[onFinishName] !== "function") throw new Error(`runAgentTool: detached.onFinish "${onFinishName}" is not a method on ${this.constructor.name}. Pass the NAME of a method (e.g. "onImportDone"), not a closure — closures cannot be rehydrated after the Durable Object is evicted.`);
		} else if (detached.notify) {
			if (typeof this[DETACHED_NOTIFY_CALLBACK] === "function") onFinishName = DETACHED_NOTIFY_CALLBACK;
		}
		return {
			...onFinishName !== void 0 ? { onFinishName } : {},
			...notifySource !== void 0 ? { notifySource } : {},
			...detached.maxBudgetMs !== void 0 ? { maxBudgetMs: detached.maxBudgetMs } : {},
			...detached.noProgressBudgetMs !== void 0 ? { noProgressBudgetMs: detached.noProgressBudgetMs } : {},
			...(() => {
				const raw = detached.onMilestones;
				if (!raw) return {};
				const names = Array.isArray(raw) ? raw : raw.names;
				if (!Array.isArray(names) || names.length === 0) return {};
				return { onMilestones: {
					names,
					mode: Array.isArray(raw) ? "narrate" : raw.mode ?? "narrate"
				} };
			})()
		};
	}
	_isAgentToolRowHardTerminal(status) {
		return status === "completed" || status === "error" || status === "aborted";
	}
	_hasOutstandingDetachedRuns() {
		return (this.sql`
      SELECT COUNT(*) AS n FROM cf_agent_tool_runs
      WHERE detached = 1 AND finish_delivered_at IS NULL
    `[0]?.n ?? 0) > 0;
	}
	/** Detached runs still holding a concurrency slot (non-terminal). */
	_liveDetachedRunCount() {
		return this.sql`
      SELECT COUNT(*) AS n FROM cf_agent_tool_runs
      WHERE detached = 1 AND status IN ('starting', 'running')
    `[0]?.n ?? 0;
	}
	/**
	* Edge-triggered warning when live detached runs cross
	* `DETACHED_LIVE_COUNT_WARN_THRESHOLD`. Fires once on the up-crossing and
	* re-arms only after the count falls back below the threshold, so a parent
	* accumulating long-lived background runs surfaces a signal without spamming.
	*/
	_maybeWarnDetachedLiveCount() {
		const liveCount = this._liveDetachedRunCount();
		if (liveCount < DETACHED_LIVE_COUNT_WARN_THRESHOLD) {
			this._detachedLiveCountWarned = false;
			return;
		}
		if (this._detachedLiveCountWarned) return;
		this._detachedLiveCountWarned = true;
		this._emit("agent_tool:detached:live_count_warning", {
			liveCount,
			threshold: DETACHED_LIVE_COUNT_WARN_THRESHOLD
		});
		console.warn(`[agents] ${liveCount} detached agent-tool runs are live on this agent (threshold ${DETACHED_LIVE_COUNT_WARN_THRESHOLD}). Detached runs hold a concurrency slot until they finish — make sure they are completing or being cancelled, or lower \`maxConcurrentAgentTools\`.`);
	}
	/**
	* Warm fast path for a detached run: tail the child to terminal (so the
	* parent re-broadcasts its live stream to clients) and deliver the completion
	* with low latency while the isolate stays alive. Best-effort — the durable
	* `_cfDetachedReconcileTick` backbone is the guarantee; anything this misses
	* (eviction, a child that has not yet reached terminal) the backbone collects.
	*/
	async _detachedFastPath(runInfo, cls, runId) {
		try {
			const child = await this.subAgent(cls, runId);
			const adapter = this._asAgentToolChildAdapter(child);
			let sequence = 1;
			if (adapter.tailAgentToolRun) {
				const stream = await adapter.tailAgentToolRun(runId, { afterSequence: -1 });
				sequence = (await this._forwardAgentToolStream(stream, runInfo.parentToolCallId, runId, sequence, void 0)).next;
			}
			const inspection = await adapter.inspectAgentToolRun(runId);
			if (inspection && this._isAgentToolRowHardTerminal(inspection.status)) {
				const result = this._terminalResultFromInspection(runInfo.agentType, inspection);
				await this._deliverDetachedTerminal(runId, "finish", result, {
					sequence,
					serialize: true
				}, inspection.completedAt);
			}
		} catch {}
	}
	/**
	* Single delivery funnel for a detached terminal. Both the warm fast path and
	* the durable backbone route through here, with INDEPENDENT ledger slots for
	* `finish` (the real terminal) vs `give_up` (budget exhausted). Each slot is
	* delivered at-least-once via a claim + lease:
	*
	* - Concurrent double-fire is prevented by the guarded CAS claim (RETURNING
	*   yields the row only to the winner).
	* - A crash after the side effect but before `*_delivered_at` is written lets
	*   the lease expire so a later reconcile re-delivers — hence handlers must be
	*   idempotent.
	* - Two slots, not one, because `interrupted` is SOFT: a give-up followed by a
	*   real completion is legitimate, and a single shared "delivered" bit would
	*   dedupe the child's real late result away (the #1752 production incident).
	*/
	async _deliverDetachedTerminal(runId, kind, result, options, completedAt = Date.now()) {
		const now = Date.now();
		const leaseFloor = now - DETACHED_DELIVERY_LEASE_MS;
		const claimQuery = kind === "finish" ? `UPDATE cf_agent_tool_runs
             SET finish_claimed_at = ?
             WHERE run_id = ?
               AND finish_delivered_at IS NULL
               AND (finish_claimed_at IS NULL OR finish_claimed_at < ?)` : `UPDATE cf_agent_tool_runs
             SET give_up_claimed_at = ?
             WHERE run_id = ?
               AND give_up_delivered_at IS NULL
               AND (give_up_claimed_at IS NULL OR give_up_claimed_at < ?)`;
		if (this.ctx.storage.sql.exec(claimQuery, now, runId, leaseFloor).rowsWritten === 0) return;
		const row = this._readAgentToolRun(runId);
		if (!row) return;
		this._updateAgentToolTerminal(runId, result, completedAt);
		this._broadcastAgentToolTerminal(row.parent_tool_call_id ?? void 0, options?.sequence ?? Date.now(), result);
		const runInfo = this._agentToolRunInfoFromRow(row, result.status, completedAt);
		const lifecycle = {
			status: result.status,
			...result.summary !== void 0 ? { summary: result.summary } : {},
			...result.error !== void 0 ? { error: result.error } : {},
			...result.reason !== void 0 ? { reason: result.reason } : {},
			...result.childStillRunning !== void 0 ? { childStillRunning: result.childStillRunning } : {}
		};
		const invoke = async () => {
			try {
				await this.onAgentToolFinish(runInfo, lifecycle);
			} catch (error) {
				await this._safeRunOnError(error);
			}
			const callbackName = row.detached_on_finish;
			if (callbackName) {
				const callback = this[callbackName];
				if (typeof callback === "function") try {
					await callback.bind(this)(runInfo, lifecycle);
				} catch (error) {
					this._emit("agent_tool:detached:delivery_failed", {
						runId,
						kind,
						status: result.status,
						callback: callbackName,
						error: error instanceof Error ? error.message : String(error)
					});
					await this._safeRunOnError(error);
					throw error;
				}
			}
		};
		await this._runDetachedDelivery(invoke, { serialize: options?.serialize });
		if (kind === "finish") this.sql`
        UPDATE cf_agent_tool_runs
        SET finish_delivered_at = ${Date.now()}
        WHERE run_id = ${runId}
      `;
		else this.sql`
        UPDATE cf_agent_tool_runs
        SET give_up_delivered_at = ${Date.now()}
        WHERE run_id = ${runId}
      `;
	}
	async _safeRunOnError(error) {
		try {
			await this.onError(error);
		} catch {}
	}
	/**
	* Run a detached terminal delivery (the `onAgentToolFinish` + per-run
	* `onFinish` callbacks) in an appropriate execution context. The base `Agent`
	* has no turn queue, so it only establishes `agentContext` — a handler that
	* calls `runAgentTool` / `setState` therefore works regardless of where the
	* delivery fired from.
	*
	* Chat-layer subclasses (`@cloudflare/think`, `@cloudflare/ai-chat`) override
	* this to additionally serialize delivery against their turn queue when
	* `serialize` is set: a fast-path push or backbone tick can land mid-turn, and
	* a state-mutating `onFinish` running concurrently with an active LLM turn is a
	* data race. The fast path and backbone never run synchronously inside a turn
	* (they fire from `waitUntil` / a scheduled alarm), so enqueuing them on the
	* turn queue is deadlock-free. An explicit `cancelAgentTool` runs with
	* `serialize` unset because it may be called from inside the very turn that
	* triggers it, where enqueuing would self-deadlock.
	*/
	async _runDetachedDelivery(invoke, _options) {
		if (__DO_NOT_USE_WILL_BREAK__agentContext.getStore()?.agent) {
			await invoke();
			return;
		}
		await runInInvocation({
			agent: this,
			connection: void 0,
			request: void 0,
			email: void 0
		}, invoke, { detached: true });
	}
	/**
	* Arm the self-scheduling detached reconcile backbone. Existing schedules are
	* reused for recovery/startup calls, but a fresh detached dispatch resets the
	* pending cadence to the fast end so new work is noticed promptly.
	*/
	async _armDetachedBackbone(options) {
		const run = this._detachedBackboneArming.then(() => this._armDetachedBackboneInner(options));
		this._detachedBackboneArming = run.then(() => void 0, () => void 0);
		return run;
	}
	async _armDetachedBackboneInner(options) {
		const armed = (await this.listSchedules()).filter((schedule) => schedule.callback === DETACHED_RECONCILE_CALLBACK);
		if (armed.length > 0 && !options?.resetCadence) {
			for (const schedule of armed.slice(1)) await this.cancelSchedule(schedule.id);
			return;
		}
		for (const schedule of armed) await this.cancelSchedule(schedule.id);
		await this.schedule(DETACHED_BACKBONE_CADENCE_S[0], DETACHED_RECONCILE_CALLBACK, { cadenceIndex: 0 }, { idempotent: true });
	}
	/**
	* Durable backbone for detached runs. Runs on a self-rescheduling alarm:
	* collects any detached run that has reached terminal but was not yet
	* delivered (e.g. the parent was evicted before the fast path landed), gives
	* up on any run past its absolute budget (tearing the child down), and
	* reschedules itself while any detached run remains undelivered — cancelling
	* itself once everything has settled (zero steady-state cost).
	*/
	async _cfDetachedReconcileTick(payload) {
		const rows = this.sql`
      SELECT run_id, parent_tool_call_id, agent_type, input_preview, status,
             summary, output_json, error_message, interrupted_reason,
             child_still_running, display_metadata, display_order,
             started_at, completed_at, detached, detached_on_finish,
             detached_notify_source, detached_max_budget_at,
             detached_no_progress_budget_ms, last_progress_at,
             detached_on_milestones,
             finish_claimed_at, finish_delivered_at, give_up_claimed_at,
             give_up_delivered_at
      FROM cf_agent_tool_runs
      WHERE detached = 1 AND finish_delivered_at IS NULL
      ORDER BY started_at ASC
    `;
		for (const row of rows) {
			const runId = row.run_id;
			let inspection = null;
			try {
				const child = await this._cf_resolveSubAgent(row.agent_type, runId);
				inspection = await this._asAgentToolChildAdapter(child).inspectAgentToolRun(runId);
			} catch {}
			if (inspection?.milestones && row.detached_on_milestones) {
				const milestoneRunInfo = this._agentToolRunInfoFromRow(row);
				for (const milestone of inspection.milestones) this._maybeDeliverDetachedMilestone(row, milestoneRunInfo, milestone);
			}
			if (inspection && this._isAgentToolRowHardTerminal(inspection.status)) {
				const result = this._terminalResultFromInspection(row.agent_type, inspection);
				await this._deliverDetachedTerminal(runId, "finish", result, {
					sequence: Date.now(),
					serialize: true
				}, inspection.completedAt);
				continue;
			}
			const now = Date.now();
			const budgetAt = row.detached_max_budget_at;
			const latestMilestone = inspection?.milestones?.length ? inspection.milestones[inspection.milestones.length - 1].at : void 0;
			const signalTimes = [
				inspection?.progress?.at,
				latestMilestone,
				row.last_progress_at
			].filter((t) => typeof t === "number");
			const lastSignalAt = signalTimes.length > 0 ? Math.max(...signalTimes) : void 0;
			const noProgressBudgetMs = row.detached_no_progress_budget_ms;
			const overAbsolute = budgetAt !== null && now >= budgetAt;
			const overNoProgress = typeof noProgressBudgetMs === "number" && noProgressBudgetMs > 0 && Number.isFinite(noProgressBudgetMs) && typeof lastSignalAt === "number" && now - lastSignalAt >= noProgressBudgetMs;
			if ((overAbsolute || overNoProgress) && row.give_up_delivered_at === null) {
				let childTornDown = false;
				try {
					const child = await this._cf_resolveSubAgent(row.agent_type, runId);
					await this._asAgentToolChildAdapter(child).cancelAgentToolRun(runId, overAbsolute ? "detached budget exceeded" : "detached run went silent past its no-progress window");
					childTornDown = true;
				} catch {}
				await this._deliverDetachedTerminal(runId, "give_up", {
					runId,
					agentType: row.agent_type,
					status: "interrupted",
					error: overAbsolute ? "detached run exceeded its budget before completing" : "detached run went silent past its no-progress window",
					reason: overAbsolute ? "budget-exceeded" : "no-progress",
					childStillRunning: !childTornDown
				}, { serialize: true });
			}
		}
		if (this._hasOutstandingDetachedRuns()) {
			const currentIndex = typeof payload?.cadenceIndex === "number" ? payload.cadenceIndex : 0;
			const nextIndex = Math.min(currentIndex + 1, DETACHED_BACKBONE_CADENCE_S.length - 1);
			await this.schedule(DETACHED_BACKBONE_CADENCE_S[nextIndex], DETACHED_RECONCILE_CALLBACK, { cadenceIndex: nextIndex });
		}
	}
	hasAgentToolRun(classOrName, runId) {
		const agentType = typeof classOrName === "string" ? classOrName : classOrName.name;
		return (this.sql`
      SELECT COUNT(*) AS n FROM cf_agent_tool_runs
      WHERE run_id = ${runId} AND agent_type = ${agentType}
    `[0]?.n ?? 0) > 0;
	}
	async clearAgentToolRuns(options) {
		const rows = this.sql`
      SELECT run_id, agent_type, status FROM cf_agent_tool_runs
      ORDER BY started_at ASC
    `;
		const statusFilter = options?.status ? new Set(options.status) : null;
		const retained = rows.filter((row) => {
			if (statusFilter && !statusFilter.has(row.status)) return false;
			if (options?.olderThan !== void 0) {
				const full = this._readAgentToolRun(row.run_id);
				if (!full || full.started_at >= options.olderThan) return false;
			}
			return true;
		});
		for (const row of retained) {
			try {
				const cls = this._agentToolClassByName(row.agent_type);
				if (row.status === "starting" || row.status === "running") {
					const child = await this.subAgent(cls, row.run_id);
					await this._asAgentToolChildAdapter(child).cancelAgentToolRun(row.run_id, "clearing agent tool run");
				}
				await this.deleteSubAgent(cls, row.run_id);
			} catch {}
			this.sql`
        DELETE FROM cf_agent_tool_runs WHERE run_id = ${row.run_id}
      `;
		}
	}
	_isAgentToolTerminal(status) {
		return status === "completed" || status === "error" || status === "aborted" || status === "interrupted";
	}
	_activeAgentToolRunCount() {
		return this.sql`
      SELECT COUNT(*) AS n FROM cf_agent_tool_runs
      WHERE status IN ('starting', 'running')
    `[0]?.n ?? 0;
	}
	_defaultAgentToolPreview(input) {
		if (typeof input === "string") return input.slice(0, 500);
		if (input === null || input === void 0) return input;
		try {
			const json = JSON.stringify(input);
			return json.length > 500 ? `${json.slice(0, 497)}...` : json;
		} catch {
			return String(input).slice(0, 500);
		}
	}
	_readAgentToolRun(runId) {
		return this.sql`
      SELECT run_id, parent_tool_call_id, agent_type, input_preview, status,
             summary, output_json, error_message, interrupted_reason,
             child_still_running, display_metadata, display_order,
             started_at, completed_at, detached, detached_on_finish,
             detached_notify_source, detached_max_budget_at,
             finish_claimed_at, finish_delivered_at, give_up_claimed_at,
             give_up_delivered_at
      FROM cf_agent_tool_runs
      WHERE run_id = ${runId}
      LIMIT 1
    `[0] ?? null;
	}
	/**
	* Reconstruct the typed interrupted cause (`reason` / `childStillRunning`,
	* #1630 follow-up) from a stored row so a row→result/event rebuild — e.g. a
	* reconnect replay — carries the same fields a live client saw. Only
	* `interrupted` rows store a cause; everything else yields `{}` (the columns
	* are cleared whenever a row settles to a hard terminal).
	*/
	_agentToolInterruptedExtrasFromRow(row) {
		if (row.status !== "interrupted") return {};
		return {
			...row.interrupted_reason !== null ? { reason: row.interrupted_reason } : {},
			...row.child_still_running !== null ? { childStillRunning: row.child_still_running !== 0 } : {}
		};
	}
	_resultFromAgentToolRow(row) {
		const output = this._parseAgentToolJson(row.output_json);
		return {
			runId: row.run_id,
			agentType: row.agent_type,
			status: row.status,
			...output !== void 0 ? { output } : {},
			...row.summary !== null ? { summary: row.summary } : {},
			...row.error_message !== null ? { error: row.error_message } : {},
			...this._agentToolInterruptedExtrasFromRow(row)
		};
	}
	_agentToolRunInfoFromRow(row, status = row.status, completedAt = row.completed_at ?? void 0) {
		return {
			runId: row.run_id,
			parentToolCallId: row.parent_tool_call_id ?? void 0,
			agentType: row.agent_type,
			inputPreview: this._parseAgentToolJson(row.input_preview),
			status,
			display: this._parseAgentToolJson(row.display_metadata),
			...row.detached_notify_source != null ? { notifySource: row.detached_notify_source } : {},
			displayOrder: row.display_order,
			startedAt: row.started_at,
			completedAt
		};
	}
	_terminalResultFromInspection(agentType, inspection) {
		if (inspection.status === "completed") return {
			runId: inspection.runId,
			agentType,
			status: "completed",
			output: inspection.output,
			summary: inspection.summary
		};
		if (inspection.status === "aborted") return {
			runId: inspection.runId,
			agentType,
			status: "aborted",
			error: inspection.error
		};
		return {
			runId: inspection.runId,
			agentType,
			status: "error",
			error: inspection.error ?? "Agent tool run failed"
		};
	}
	async _finishAgentToolRun(run, result, options) {
		const completedAt = options?.completedAt ?? Date.now();
		this._updateAgentToolTerminal(run.runId, result, completedAt);
		if (options?.sequence !== void 0) this._broadcastAgentToolTerminal(run.parentToolCallId, options.sequence, result);
		const finish = () => this.onAgentToolFinish({
			...run,
			status: result.status,
			completedAt
		}, result);
		if (options?.deferFinishHook) return finish;
		await finish();
	}
	async _runDeferredAgentToolFinishHooks(hooks) {
		for (const hook of hooks) try {
			await hook();
		} catch (error) {
			try {
				await this.onError(error);
			} catch {}
		}
	}
	_updateAgentToolTerminal(runId, result, completedAt = Date.now()) {
		const childStillRunning = result.childStillRunning === void 0 ? null : result.childStillRunning ? 1 : 0;
		this.sql`
      UPDATE cf_agent_tool_runs
      SET status = ${result.status},
          summary = ${result.summary ?? null},
          output_json = ${this._stringifyAgentToolOutput(result.output)},
          error_message = ${result.error ?? null},
          interrupted_reason = ${result.reason ?? null},
          child_still_running = ${childStillRunning},
          completed_at = ${completedAt}
      WHERE run_id = ${runId}
        AND status NOT IN ('completed', 'error', 'aborted')
    `;
		if (result.status === "completed" && result.output !== void 0) this.sql`
        UPDATE cf_agent_tool_runs
        SET output_json = COALESCE(output_json, ${this._stringifyAgentToolOutput(result.output)}),
            summary = COALESCE(summary, ${result.summary ?? null})
        WHERE run_id = ${runId} AND status = 'completed'
      `;
	}
	_markAgentToolRunning(runId) {
		this.sql`
      UPDATE cf_agent_tool_runs
      SET status = 'running'
      WHERE run_id = ${runId} AND status = 'starting'
    `;
	}
	_parseAgentToolJson(value) {
		if (value === null) return void 0;
		try {
			return JSON.parse(value);
		} catch {
			return value;
		}
	}
	_stringifyAgentToolOutput(output) {
		if (output === void 0) return null;
		const json = JSON.stringify(output);
		return json === void 0 ? null : json;
	}
	_broadcastAgentToolEvent(parentToolCallId, sequence, event, replay, connection) {
		const message = {
			type: "agent-tool-event",
			parentToolCallId,
			sequence,
			event,
			...replay ? { replay } : {}
		};
		const body = JSON.stringify(message);
		if (connection) connection.send(body);
		else this.broadcast(body);
	}
	_broadcastAgentToolChunks(parentToolCallId, runId, chunks, sequence, replay, connection) {
		let next = sequence;
		for (const chunk of chunks) this._broadcastAgentToolEvent(parentToolCallId, next++, {
			kind: "chunk",
			runId,
			body: chunk.body
		}, replay, connection);
		return next;
	}
	async _broadcastAgentToolStoredChunks(row, sequence, replay, connection) {
		const child = await this._cf_resolveSubAgent(row.agent_type, row.run_id);
		const adapter = this._asAgentToolChildAdapter(child);
		return this._broadcastAgentToolStoredChunksFromAdapter(adapter, row, sequence, replay, connection);
	}
	async _broadcastAgentToolStoredChunksFromAdapter(adapter, row, sequence, replay, connection, timeoutMs) {
		const chunks = await this._getAgentToolChunksForRecovery(adapter, row.run_id, timeoutMs);
		if (!chunks) return sequence;
		return this._broadcastAgentToolChunks(row.parent_tool_call_id ?? void 0, row.run_id, chunks, sequence, replay, connection);
	}
	async _forwardAgentToolStream(stream, parentToolCallId, runId, sequence, signal, idleTimeoutMs) {
		let next = sequence;
		if (signal?.aborted) return {
			next,
			ended: "aborted"
		};
		let ended = "done";
		const reader = stream.getReader();
		const decoder = new TextDecoder();
		let bufferedBytes = "";
		let aborted = false;
		let resolveAbort;
		const abortPromise = new Promise((resolve) => {
			resolveAbort = resolve;
		});
		let abortListener;
		if (signal) {
			abortListener = () => resolveAbort?.();
			signal.addEventListener("abort", abortListener, { once: true });
		}
		const idleEnabled = typeof idleTimeoutMs === "number" && idleTimeoutMs > 0 && Number.isFinite(idleTimeoutMs);
		let resolveIdle;
		let idleTimer;
		const idlePromise = new Promise((resolve) => {
			resolveIdle = resolve;
		});
		const armIdle = () => {
			if (!idleEnabled) return;
			if (idleTimer !== void 0) clearTimeout(idleTimer);
			idleTimer = setTimeout(() => resolveIdle?.(), idleTimeoutMs);
		};
		let forwardedSinceProgress = false;
		try {
			const forwardChunk = (chunk) => {
				this._broadcastAgentToolEvent(parentToolCallId, next++, {
					kind: "chunk",
					runId,
					body: chunk.body
				});
				this._observeForwardedProgress(runId, chunk.body);
				forwardedSinceProgress = true;
				armIdle();
			};
			const forwardLine = (line) => {
				try {
					const chunk = JSON.parse(line);
					if (typeof chunk.body === "string") forwardChunk(chunk);
				} catch {}
			};
			const flushBufferedBytes = (final = false) => {
				while (true) {
					const newline = bufferedBytes.indexOf("\n");
					if (newline === -1) break;
					const line = bufferedBytes.slice(0, newline).trim();
					bufferedBytes = bufferedBytes.slice(newline + 1);
					if (line.length > 0) forwardLine(line);
				}
				if (final && bufferedBytes.trim().length > 0) {
					forwardLine(bufferedBytes);
					bufferedBytes = "";
				}
			};
			armIdle();
			while (true) {
				const readPromise = reader.read();
				readPromise.catch(() => {});
				const raced = await Promise.race([
					readPromise.then((result) => ({
						kind: "read",
						result
					})),
					abortPromise.then(() => ({ kind: "abort" })),
					idlePromise.then(() => ({ kind: "idle" }))
				]);
				if (raced.kind === "abort" || raced.kind === "idle") {
					aborted = true;
					ended = raced.kind === "idle" ? "idle" : "aborted";
					break;
				}
				const { done, value } = raced.result;
				if (done) {
					bufferedBytes += decoder.decode();
					flushBufferedBytes(true);
					break;
				}
				if (value instanceof Uint8Array) {
					bufferedBytes += decoder.decode(value, { stream: true });
					flushBufferedBytes();
				} else forwardChunk(value);
				if (forwardedSinceProgress) {
					forwardedSinceProgress = false;
					try {
						await this._onAgentToolStreamProgress();
					} catch {}
				}
			}
		} finally {
			if (idleTimer !== void 0) clearTimeout(idleTimer);
			if (abortListener && signal) signal.removeEventListener("abort", abortListener);
			if (!aborted) try {
				reader.releaseLock();
			} catch {}
		}
		return {
			next,
			ended
		};
	}
	/**
	* Hook invoked by `_forwardAgentToolStream` after a child produces output that
	* was forwarded to the parent's connections. Forwarding a sub-agent's stream
	* is genuine forward progress for the *parent* turn (the parent is
	* orchestrating the child), so chat-recovery subclasses (Think / AIChatAgent)
	* override this to advance their recovery progress marker.
	*
	* Without it, a parent whose turn merely `await`s a sub-agent banks zero
	* progress of its own, so under deploy churn the parent's no-progress recovery
	* window exhausts and abandons the turn as `interrupted` — even though the
	* child is healthily streaming and ultimately completes (observed in the
	* `deploy-churn --mode subagent` harness: `attempt 6/6, stable_timeout,
	* progress: 1`).
	*
	* Called ONLY after at least one chunk was actually forwarded — never merely
	* because a child is attached — so a silent / hung child still lets the parent
	* exhaust on its own timer. The base Agent has no recovery budget, so this is
	* a no-op; subclasses should throttle the (durable) bump since this can be
	* called repeatedly while a child streams.
	*/
	async _onAgentToolStreamProgress() {}
	/**
	* Best-effort observation of a forwarded child chunk: if it is a reserved
	* `data-agent-progress` frame, refresh the cached liveness timestamp on the
	* run row (a hint for a still-warm parent) and fire the public `onProgress`
	* hook. Never throws into the forward loop — the child's own persisted
	* snapshot (read via `inspectAgentToolRun`) remains authoritative for the
	* resetting no-progress budget after eviction.
	*/
	_observeForwardedProgress(runId, body) {
		let parsed;
		try {
			parsed = JSON.parse(body);
		} catch {
			return;
		}
		if (!parsed) return;
		const isMilestone = parsed.type === AGENT_TOOL_MILESTONE_PART;
		if (parsed.type !== "data-agent-progress" && !isMilestone) return;
		const data = parsed.data ?? {};
		const at = Date.now();
		const snapshot = {
			...typeof data.fraction === "number" ? { fraction: data.fraction } : {},
			...typeof data.message === "string" ? { message: data.message } : {},
			...typeof data.phase === "string" ? { phase: data.phase } : {},
			...isMilestone && typeof data.name === "string" ? { milestone: data.name } : {},
			...data.data !== void 0 ? { data: data.data } : {},
			at
		};
		const row = this._readAgentToolRun(runId);
		if (!row) return;
		if (row.detached) try {
			this.sql`
          UPDATE cf_agent_tool_runs SET last_progress_at = ${at}
          WHERE run_id = ${runId}
        `;
		} catch {}
		const runInfo = this._agentToolRunInfoFromRow(row);
		Promise.resolve(this.onProgress(runInfo, snapshot)).catch((error) => {
			console.error(`[agents] onProgress hook threw for run ${runId}:`, error instanceof Error ? error.message : String(error));
		});
		if (isMilestone && typeof data.name === "string") this._maybeDeliverDetachedMilestone(row, runInfo, {
			name: data.name,
			sequence: typeof data.sequence === "number" ? data.sequence : 0,
			at: typeof data.at === "number" ? data.at : at,
			...data.data !== void 0 ? { data: data.data } : {}
		});
	}
	/**
	* Deliver a milestone notification IF this run opted into it via
	* `detached: { onMilestones }` and the milestone name is in that set. Routes
	* to the overridable `_deliverDetachedMilestone` seam (a no-op on the base
	* `Agent`; chat hosts inject an idempotent synthetic chat message).
	*/
	_maybeDeliverDetachedMilestone(row, runInfo, milestone) {
		const configured = this._parseAgentToolJson(row.detached_on_milestones ?? null);
		const names = Array.isArray(configured) ? configured : configured?.names;
		const mode = Array.isArray(configured) ? "narrate" : configured?.mode ?? "narrate";
		if (!Array.isArray(names) || !names.includes(milestone.name)) return;
		Promise.resolve(this._deliverDetachedMilestone(runInfo, milestone, mode)).catch((error) => {
			console.error(`[agents] detached milestone delivery threw for run ${runInfo.runId} (${milestone.name}):`, error instanceof Error ? error.message : String(error));
		});
	}
	/**
	* Overridable seam for the `detached: { onMilestones }` convenience. The base
	* `Agent` has no chat surface, so this is a no-op; chat hosts
	* (`@cloudflare/think`, `AIChatAgent`) override it to submit an idempotent
	* synthetic message keyed on `(runId, milestone.name)`. Called from both the
	* warm tail and the backbone reconcile, so it MUST be idempotent.
	*/
	async _deliverDetachedMilestone(_run, _milestone, _mode) {}
	_broadcastAgentToolTerminal(parentToolCallId, sequence, result, replay, connection) {
		if (result.status === "completed") this._broadcastAgentToolEvent(parentToolCallId, sequence, {
			kind: "finished",
			runId: result.runId,
			summary: result.summary ?? ""
		}, replay, connection);
		else if (result.status === "aborted") this._broadcastAgentToolEvent(parentToolCallId, sequence, {
			kind: "aborted",
			runId: result.runId,
			reason: result.error
		}, replay, connection);
		else if (result.status === "interrupted") this._broadcastAgentToolEvent(parentToolCallId, sequence, {
			kind: "interrupted",
			runId: result.runId,
			error: result.error ?? "Agent tool run was interrupted",
			...result.reason !== void 0 ? { reason: result.reason } : {},
			...result.childStillRunning !== void 0 ? { childStillRunning: result.childStillRunning } : {}
		}, replay, connection);
		else this._broadcastAgentToolEvent(parentToolCallId, sequence, {
			kind: "error",
			runId: result.runId,
			error: result.error ?? "Agent tool run failed"
		}, replay, connection);
	}
	_asAgentToolChildAdapter(child) {
		const candidate = child;
		if (typeof candidate.startAgentToolRun !== "function" || typeof candidate.cancelAgentToolRun !== "function" || typeof candidate.inspectAgentToolRun !== "function" || typeof candidate.getAgentToolChunks !== "function") throw new Error("Agent tool child must implement the framework agent-tool adapter. Use a @cloudflare/think Think subclass or an AIChatAgent subclass.");
		return candidate;
	}
	_agentToolClassByName(className) {
		const cls = this.ctx.exports?.[className];
		if (!cls) throw new Error(`Agent tool class "${className}" is not exported.`);
		return cls;
	}
	async _replayAndInterruptAgentToolRun(row, message, extra) {
		let sequence = 1;
		try {
			sequence = await this._broadcastAgentToolStoredChunks(row, sequence);
		} catch {}
		const result = {
			runId: row.run_id,
			agentType: row.agent_type,
			status: "interrupted",
			error: message,
			...extra?.reason !== void 0 ? { reason: extra.reason } : {},
			...extra?.childStillRunning !== void 0 ? { childStillRunning: extra.childStillRunning } : {}
		};
		await this._finishAgentToolRun(this._agentToolRunInfoFromRow(row), result, { sequence });
		return result;
	}
	/**
	* Human-readable prose for an `interrupted` seal. Kept in sync with
	* {@link AgentToolInterruptedReason}; callers branch on the typed `reason`
	* field, not this string.
	*/
	_interruptedMessageForReason(reason) {
		switch (reason) {
			case "no-progress": return "Agent tool run was still running but made no forward progress within the re-attach no-progress budget; the parent gave up.";
			case "window-exceeded": return "Agent tool run did not reach a terminal result within the maximum re-attach window; the parent gave up.";
			case "not-tailable": return "Agent tool run was still running, but live-tail reattachment is not supported in this runtime.";
			case "inspect-timeout": return "Agent tool run inspection timed out during parent recovery.";
			case "inspect-failed": return "Agent tool run could not be inspected during parent recovery.";
			case "recovery-deadline": return "Agent tool run recovery deadline exceeded.";
			default: return "Agent tool run was still running and did not reach a terminal result.";
		}
	}
	/**
	* Tear down a child agent-tool run the parent has genuinely given up on
	* (#1630 follow-up). Teardown is scoped to `window-exceeded` ONLY — the hard
	* ceiling, where the child has had its full recovery window and is therefore
	* truly exhausted, so cancelling it reclaims its fiber / keep-alive. Every
	* other give-up is deliberately left repairable: `no-progress` seals stay
	* SOFT (`interrupted`, `childStillRunning: true`) so a re-issue can still
	* re-attach and collect the child if it self-heals — tearing those down would
	* defeat the repair-on-re-issue path and convert a retryable interrupt into a
	* non-retryable `aborted`. Reasons where the child's state is unknown
	* (`inspect-*`, `recovery-deadline`, `not-tailable`) are also left alone.
	* Returns whether the child was torn down (so the caller reports
	* `childStillRunning: false`).
	*/
	async _teardownGivenUpAgentToolChild(adapter, runId, reason) {
		if (reason !== "window-exceeded") return false;
		try {
			await adapter.cancelAgentToolRun(runId, `agent tool run given up by parent recovery: ${reason}`);
			return true;
		} catch {
			return false;
		}
	}
	/**
	* Re-attach to a still-running child agent-tool run and tail it to its real
	* terminal result, instead of abandoning it as `interrupted` (#1630). The
	* child is a separate facet with its own `chatRecovery`, so resolving it via
	* the adapter wakes it and lets it self-complete the interrupted turn; we tail
	* its live stream (forwarding chunks to the parent's connections) until it
	* reaches terminal, then inspect for the collected result.
	*
	* The wait is PROGRESS-KEYED, not a flat wall clock (which previously abandoned
	* healthy, still-advancing children whose recovery simply outran a fixed
	* budget). `noProgressTimeoutMs` bounds how long the parent waits with NO
	* forward progress; it is reset on every forwarded chunk. As long as the child
	* keeps streaming it is followed through to terminal. The loop also RE-ARMS
	* across stream-closes (a child re-evicted mid-recovery, or a tail that ends
	* before terminal) as long as the prior attempt made progress, so a child that
	* dies and recovers again during deploy churn is still collected. A genuinely
	* silent/hung child can never block recovery forever: it seals `interrupted`
	* after one `noProgressTimeoutMs` window. `maxWindowMs` is an OPTIONAL hard
	* wall-clock ceiling (default `Infinity` — uncapped, mirroring #1672's
	* `maxRecoveryWork`); set it finite to also bound a child that keeps
	* progressing, which seals `window-exceeded` and tears the child down.
	*
	* Returns the terminal `result` (and `completedAt`) when the child reaches a
	* terminal status, plus the advanced broadcast `sequence`. Returns
	* `{ result: undefined }` when there is no `tailAgentToolRun` adapter, the
	* child makes no progress within a full no-progress window, or the ceiling is
	* reached while the child is still non-terminal — the caller then seals
	* `interrupted`.
	*/
	async _reattachAgentToolRunToTerminal(adapter, row, sequence, noProgressTimeoutMs = DEFAULT_AGENT_TOOL_REATTACH_NO_PROGRESS_TIMEOUT_MS, maxWindowMs = DEFAULT_AGENT_TOOL_REATTACH_MAX_WINDOW_MS) {
		if (typeof adapter.tailAgentToolRun !== "function") return {
			sequence,
			reason: "not-tailable"
		};
		this._emit("agent_tool:recovery:reattach", {
			runId: row.run_id,
			agentType: row.agent_type,
			budgetMs: noProgressTimeoutMs
		});
		const collectTerminal = async (seq) => {
			let inspection = null;
			try {
				inspection = await adapter.inspectAgentToolRun(row.run_id);
			} catch {
				return null;
			}
			if (inspection && inspection.status !== "running" && inspection.status !== "starting") return {
				sequence: seq,
				result: this._terminalResultFromInspection(row.agent_type, inspection),
				completedAt: inspection.completedAt
			};
			return null;
		};
		let nextSequence = sequence;
		if (!(noProgressTimeoutMs > 0)) return await collectTerminal(nextSequence) ?? {
			sequence: nextSequence,
			reason: "no-progress"
		};
		const ceilingController = new AbortController();
		let ceilingTimer;
		if (maxWindowMs > 0 && Number.isFinite(maxWindowMs)) ceilingTimer = setTimeout(() => ceilingController.abort(), maxWindowMs);
		let reason = "no-progress";
		try {
			while (!ceilingController.signal.aborted) {
				let afterSequence = -1;
				try {
					const existing = await adapter.getAgentToolChunks(row.run_id);
					const last = existing[existing.length - 1];
					if (last) afterSequence = last.sequence;
				} catch {}
				const beforeSequence = nextSequence;
				let streamEnded = "idle";
				try {
					const stream = await adapter.tailAgentToolRun(row.run_id, { afterSequence });
					const forwarded = await this._forwardAgentToolStream(stream, row.parent_tool_call_id ?? void 0, row.run_id, nextSequence, ceilingController.signal, noProgressTimeoutMs);
					nextSequence = forwarded.next;
					streamEnded = forwarded.ended;
				} catch {}
				const terminal = await collectTerminal(nextSequence);
				if (terminal) return terminal;
				if (ceilingController.signal.aborted) {
					reason = "window-exceeded";
					break;
				}
				if (streamEnded !== "done") break;
				if (nextSequence <= beforeSequence) break;
			}
		} finally {
			if (ceilingTimer !== void 0) clearTimeout(ceilingTimer);
		}
		return {
			sequence: nextSequence,
			reason
		};
	}
	async _replayAgentToolRuns(connection) {
		const rows = this.sql`
      SELECT run_id, parent_tool_call_id, agent_type, input_preview, status,
             summary, output_json, error_message, interrupted_reason,
             child_still_running, display_metadata, display_order
      FROM cf_agent_tool_runs
      ORDER BY started_at ASC
    `;
		for (const row of rows) {
			const parentToolCallId = row.parent_tool_call_id ?? void 0;
			let sequence = 0;
			this._broadcastAgentToolEvent(parentToolCallId, sequence++, {
				kind: "started",
				runId: row.run_id,
				agentType: row.agent_type,
				inputPreview: this._parseAgentToolJson(row.input_preview),
				order: row.display_order,
				display: this._parseAgentToolJson(row.display_metadata)
			}, true, connection);
			try {
				sequence = await this._broadcastAgentToolStoredChunks(row, sequence, true, connection);
			} catch {}
			if (this._isAgentToolTerminal(row.status)) this._broadcastAgentToolTerminal(parentToolCallId, sequence, {
				runId: row.run_id,
				agentType: row.agent_type,
				status: row.status,
				output: this._parseAgentToolJson(row.output_json),
				summary: row.summary ?? void 0,
				error: row.error_message ?? void 0,
				...this._agentToolInterruptedExtrasFromRow(row)
			}, true, connection);
		}
	}
	async _reconcileAgentToolRuns(options) {
		const reattachTimeoutMs = options?.reattachTimeoutMs ?? this._resolvedOptions.agentToolReattachNoProgressTimeoutMs;
		const reattachMaxWindowMs = options?.reattachMaxWindowMs ?? this._resolvedOptions.agentToolReattachMaxWindowMs;
		const startedAt = Date.now();
		const totalTimeoutMs = options?.totalRecoveryTimeoutMs ?? DEFAULT_AGENT_TOOL_RECOVERY_TOTAL_TIMEOUT_MS;
		const deadlineAt = totalTimeoutMs > 0 ? startedAt + totalTimeoutMs : Number.POSITIVE_INFINITY;
		const deferredFinishes = [];
		const rows = this.sql`
      SELECT run_id, parent_tool_call_id, agent_type, input_preview, status,
             summary, output_json, error_message, interrupted_reason,
             child_still_running, display_metadata, display_order,
             started_at, completed_at
      FROM cf_agent_tool_runs
      WHERE status IN ('starting', 'running') AND detached = 0
      ORDER BY started_at ASC
    `;
		const runIds = options?.runIds !== void 0 ? new Set(options.runIds) : void 0;
		const recoveryRows = rows.filter((row) => !runIds || runIds.has(row.run_id));
		this._emit("agent_tool:recovery:begin", {
			runCount: recoveryRows.length,
			totalTimeoutMs
		});
		const finalizeRow = async (row, result, sequence, completedAt) => {
			this._emit("agent_tool:recovery:row", {
				runId: row.run_id,
				agentType: row.agent_type,
				status: result.status,
				reason: result.error,
				elapsedMs: Date.now() - startedAt
			});
			const deferredFinish = await this._finishAgentToolRun(this._agentToolRunInfoFromRow(row), result, {
				sequence,
				completedAt,
				deferFinishHook: options?.deferFinishHooks
			});
			if (deferredFinish) deferredFinishes.push(deferredFinish);
		};
		const reattachQueue = [];
		for (const row of recoveryRows) {
			const sequence = 1;
			const remainingMs = deadlineAt - Date.now();
			if (remainingMs <= 0) {
				this._emit("agent_tool:recovery:deadline", {
					runId: row.run_id,
					agentType: row.agent_type,
					elapsedMs: Date.now() - startedAt
				});
				await finalizeRow(row, {
					runId: row.run_id,
					agentType: row.agent_type,
					status: "interrupted",
					reason: "recovery-deadline",
					error: this._interruptedMessageForReason("recovery-deadline")
				}, sequence, void 0);
				continue;
			}
			const childTimeout = options?.childInspectionTimeoutMs ?? DEFAULT_AGENT_TOOL_RECOVERY_TIMEOUT_MS;
			const boundedChildTimeout = childTimeout > 0 ? Math.min(childTimeout, remainingMs) : remainingMs;
			const recovery = await this._inspectAgentToolRunForRecovery(row, sequence, boundedChildTimeout);
			if (recovery.status !== "inspected") {
				await finalizeRow(row, (() => {
					const reason = recovery.status === "timed-out" ? "inspect-timeout" : "inspect-failed";
					return {
						runId: row.run_id,
						agentType: row.agent_type,
						status: "interrupted",
						reason,
						error: this._interruptedMessageForReason(reason)
					};
				})(), sequence, void 0);
				continue;
			}
			const inspection = recovery.inspection;
			const stillRunning = !inspection || inspection.status === "running" || inspection.status === "starting";
			if (stillRunning && typeof recovery.adapter.tailAgentToolRun === "function") {
				reattachQueue.push({
					row,
					adapter: recovery.adapter
				});
				continue;
			}
			let sequenceAfterReplay = sequence;
			try {
				sequenceAfterReplay = await this._broadcastAgentToolStoredChunksFromAdapter(recovery.adapter, row, sequence, void 0, void 0, boundedChildTimeout);
			} catch {}
			if (stillRunning) await finalizeRow(row, {
				runId: row.run_id,
				agentType: row.agent_type,
				status: "interrupted",
				reason: "not-tailable",
				childStillRunning: true,
				error: this._interruptedMessageForReason("not-tailable")
			}, sequenceAfterReplay, void 0);
			else await finalizeRow(row, this._terminalResultFromInspection(row.agent_type, inspection), sequenceAfterReplay, inspection.completedAt);
		}
		await Promise.all(reattachQueue.map(async ({ row, adapter }) => {
			const reattach = await this._reattachAgentToolRunToTerminal(adapter, row, 1, reattachTimeoutMs, reattachMaxWindowMs);
			if (reattach.result) {
				await finalizeRow(row, reattach.result, reattach.sequence, reattach.completedAt);
				return;
			}
			const tornDown = await this._teardownGivenUpAgentToolChild(adapter, row.run_id, reattach.reason);
			await finalizeRow(row, {
				runId: row.run_id,
				agentType: row.agent_type,
				status: "interrupted",
				reason: reattach.reason,
				childStillRunning: !tornDown,
				error: this._interruptedMessageForReason(reattach.reason)
			}, reattach.sequence, reattach.completedAt);
		}));
		this._emit("agent_tool:recovery:complete", {
			runCount: recoveryRows.length,
			elapsedMs: Date.now() - startedAt
		});
		return deferredFinishes;
	}
	async _inspectAgentToolRunForRecovery(row, _sequence, timeoutMs = DEFAULT_AGENT_TOOL_RECOVERY_TIMEOUT_MS) {
		const inspect = (async () => {
			const child = await this._cf_resolveSubAgent(row.agent_type, row.run_id);
			const adapter = this._asAgentToolChildAdapter(child);
			return {
				status: "inspected",
				adapter,
				inspection: await adapter.inspectAgentToolRun(row.run_id)
			};
		})().catch(() => ({ status: "failed" }));
		if (timeoutMs <= 0) return inspect;
		let timeoutId;
		const timeout = new Promise((resolve) => {
			timeoutId = setTimeout(() => {
				resolve({ status: "timed-out" });
			}, timeoutMs);
		});
		const result = await Promise.race([inspect, timeout]);
		if (timeoutId !== void 0) clearTimeout(timeoutId);
		return result;
	}
	_scheduleAgentToolRunRecovery(options) {
		if (this._agentToolRunRecoveryPromise) return this._agentToolRunRecoveryPromise;
		if (options?.runIds && options.runIds.length === 0) return Promise.resolve();
		const recovery = (async () => {
			await new Promise((resolve) => setTimeout(resolve, 0));
			const recoveredAgentToolFinishes = await this._reconcileAgentToolRuns({
				deferFinishHooks: true,
				childInspectionTimeoutMs: options?.childInspectionTimeoutMs,
				totalRecoveryTimeoutMs: options?.totalRecoveryTimeoutMs,
				reattachTimeoutMs: options?.reattachTimeoutMs,
				reattachMaxWindowMs: options?.reattachMaxWindowMs,
				runIds: options?.runIds
			});
			await this._runDeferredAgentToolFinishHooks(recoveredAgentToolFinishes);
			if (this._hasOutstandingDetachedRuns()) await this._armDetachedBackbone();
		})().catch(async (error) => {
			this._emit("agent_tool:recovery:failed", { error: error instanceof Error ? error.message : String(error) });
			try {
				await this.onError(error);
			} catch {}
		}).finally(() => {
			this._agentToolRunRecoveryPromise = void 0;
		});
		this._agentToolRunRecoveryPromise = recovery;
		this.ctx.waitUntil(recovery);
		return recovery;
	}
	_agentToolRunRecoveryRunIds() {
		return this.sql`
      SELECT run_id
      FROM cf_agent_tool_runs
      WHERE status IN ('starting', 'running')
      ORDER BY started_at ASC
    `.map((row) => row.run_id);
	}
	async _getAgentToolChunksForRecovery(adapter, runId, timeoutMs) {
		const chunks = adapter.getAgentToolChunks(runId).catch(() => void 0);
		if (timeoutMs === void 0 || timeoutMs <= 0) return chunks;
		let timeoutId;
		const timeout = new Promise((resolve) => {
			timeoutId = setTimeout(() => resolve(void 0), timeoutMs);
		});
		const result = await Promise.race([chunks, timeout]);
		if (timeoutId !== void 0) clearTimeout(timeoutId);
		return result;
	}
	/**
	* Shared facet resolution — takes a CamelCase class name string
	* (matching `ctx.exports`) rather than a class reference. Both
	* `subAgent(cls, name)` and `_cf_invokeSubAgent(className, ...)`
	* funnel through here so registry bookkeeping and the
	* `_cf_initAsFacet` handshake are consistent.
	*
	* @internal
	*/
	async _cf_resolveSubAgent(className, name) {
		const ctx = this.ctx;
		if (!ctx.facets || !ctx.exports) throw new Error("subAgent() is not supported in this runtime — `ctx.facets` / `ctx.exports` are unavailable. Update to the latest `compatibility_date` in your wrangler.jsonc.");
		if (camelCaseToKebabCase(className) === "sub") throw new Error(`Sub-agent class name "${className}" kebab-cases to "sub", which collides with the reserved URL separator — rename the class (e.g. "SubThing" or "Subtask").`);
		const Cls = ctx.exports[className];
		if (!Cls) throw new Error(`Sub-agent class "${className}" not found in worker exports. Make sure the class is exported from your worker entry point and that the export name matches the class name.`);
		if (name.includes("\0")) throw new Error(`Sub-agent name contains null character (\\0), which is reserved.`);
		const facetKey = `${className}\0${name}`;
		const childParentPath = this.selfPath;
		const childPath = [...childParentPath, {
			className,
			name
		}];
		const rootClassName = this._parentPath[0]?.className ?? this.constructor.name;
		const rootNs = ctx.exports[rootClassName];
		if (!rootNs?.idFromName) {
			const minificationHint = /^_*[a-z][a-z0-9]{0,2}$/.test(rootClassName) ? ` The class name "${rootClassName}" looks minified — make sure your bundler preserves class names (e.g. esbuild's \`keepNames: true\`).` : "";
			throw new Error(`Sub-agent bootstrap requires the root agent class "${rootClassName}" to be available as a Durable Object namespace, but ctx.exports["${rootClassName}"] is missing or doesn't expose idFromName.${minificationHint} Make sure the root agent class is exported under that class name and registered in your wrangler.jsonc durable_objects.bindings.`);
		}
		const identity = await this._cf_subAgentIdentity(className, name, childPath);
		const facetId = rootNs.idFromName(identity.name);
		const stub = ctx.facets.get(facetKey, () => ({
			class: Cls,
			id: facetId
		}));
		this._recordSubAgent(className, name, identity);
		try {
			await runInInvocation({
				agent: this,
				connection: void 0,
				request: void 0,
				email: void 0
			}, async () => {
				await stub._cf_initAsFacet(name, childParentPath, identity.name);
			});
		} catch (error) {
			if (!identity.existing) this._forgetSubAgent(className, name);
			throw error;
		}
		return stub;
	}
	/**
	* Forcefully abort a running sub-agent. The child stops executing
	* immediately and will be restarted on next {@link subAgent} call.
	* Pending RPC calls receive the reason as an error.
	* Transitively aborts the child's own children.
	*
	* @experimental The API surface may change before stabilizing.
	*
	* @param cls The Agent subclass used when creating the child
	* @param name Name of the child to abort
	* @param reason Error thrown to pending/future RPC callers
	*/
	abortSubAgent(cls, name, reason) {
		const ctx = this.ctx;
		if (!ctx.facets) throw new Error("abortSubAgent() is not supported in this runtime — `ctx.facets` is unavailable. Update to the latest `compatibility_date` in your wrangler.jsonc.");
		const facetKey = `${cls.name}\0${name}`;
		ctx.facets.abort(facetKey, reason);
	}
	/**
	* Delete a sub-agent: abort it if running, then permanently wipe its
	* storage. Transitively deletes the child's own children.
	*
	* @experimental The API surface may change before stabilizing.
	*
	* @param cls The Agent subclass used when creating the child
	* @param name Name of the child to delete
	*/
	async deleteSubAgent(cls, name) {
		const ctx = this.ctx;
		if (!ctx.facets) throw new Error("deleteSubAgent() is not supported in this runtime — `ctx.facets` is unavailable. Update to the latest `compatibility_date` in your wrangler.jsonc.");
		const facetKey = `${cls.name}\0${name}`;
		const childPath = [...this.selfPath, {
			className: cls.name,
			name
		}];
		if (this._isFacet) await (await this._rootAlarmOwner())._cf_cleanupFacetPrefix(childPath);
		else await this._cf_cleanupFacetPrefix(childPath);
		try {
			ctx.facets.delete(facetKey);
		} catch {}
		this._forgetSubAgent(cls.name, name);
	}
	_addColumnIfNotExists(sql) {
		try {
			this.ctx.storage.sql.exec(sql);
		} catch (e) {
			if (!(e instanceof Error ? e.message : String(e)).toLowerCase().includes("duplicate column")) throw e;
		}
	}
	/** @internal */
	_ensureSubAgentRegistry() {
		if (this._subAgentRegistryReady) return;
		this.sql`
      CREATE TABLE IF NOT EXISTS cf_agents_sub_agents (
        class TEXT NOT NULL,
        name TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        identity_version TEXT,
        identity_name TEXT,
        PRIMARY KEY (class, name)
      )
    `;
		this._addColumnIfNotExists("ALTER TABLE cf_agents_sub_agents ADD COLUMN identity_version TEXT");
		this._addColumnIfNotExists("ALTER TABLE cf_agents_sub_agents ADD COLUMN identity_name TEXT");
		this._subAgentRegistryReady = true;
	}
	/** @internal */
	_recordSubAgent(className, name, identity) {
		this._ensureSubAgentRegistry();
		this.sql`
      INSERT OR IGNORE INTO cf_agents_sub_agents
        (class, name, created_at, identity_version, identity_name)
      VALUES
        (${className}, ${name}, ${Date.now()}, ${identity.version}, ${identity.name})
    `;
	}
	/** @internal */
	_subAgentRegistryRow(className, name) {
		this._ensureSubAgentRegistry();
		return this.sql`
      SELECT identity_version, identity_name
      FROM cf_agents_sub_agents
      WHERE class = ${className} AND name = ${name}
      LIMIT 1
    `[0] ?? null;
	}
	async _cf_subAgentIdentity(className, name, childPath) {
		const row = this._subAgentRegistryRow(className, name);
		if (row) {
			if (row.identity_version === SUB_AGENT_IDENTITY_VERSION_PATH_V2 && typeof row.identity_name === "string") return {
				version: SUB_AGENT_IDENTITY_VERSION_PATH_V2,
				name: row.identity_name,
				existing: true
			};
			return {
				version: SUB_AGENT_IDENTITY_VERSION_LEGACY,
				name,
				existing: true
			};
		}
		return {
			version: SUB_AGENT_IDENTITY_VERSION_PATH_V2,
			name: pathV2IdentityName(name, await sha256Hex(JSON.stringify(childPath))),
			existing: false
		};
	}
	/** @internal */
	_forgetSubAgent(className, name) {
		this._ensureSubAgentRegistry();
		this.sql`
      DELETE FROM cf_agents_sub_agents
      WHERE class = ${className} AND name = ${name}
    `;
	}
	hasSubAgent(classOrName, name) {
		const className = typeof classOrName === "string" ? classOrName : classOrName.name;
		this._ensureSubAgentRegistry();
		return (this.sql`
      SELECT COUNT(*) AS n FROM cf_agents_sub_agents
      WHERE class = ${className} AND name = ${name}
    `[0]?.n ?? 0) > 0;
	}
	listSubAgents(classOrName) {
		const className = typeof classOrName === "string" ? classOrName : classOrName?.name;
		this._ensureSubAgentRegistry();
		return (className ? this.sql`
          SELECT class, name, created_at FROM cf_agents_sub_agents
          WHERE class = ${className}
          ORDER BY created_at ASC
        ` : this.sql`
          SELECT class, name, created_at FROM cf_agents_sub_agents
          ORDER BY created_at ASC
        `).map((r) => ({
			className: r.class,
			name: r.name,
			createdAt: r.created_at
		}));
	}
	/**
	* Destroy the Agent, removing all state and scheduled tasks.
	*
	* On a top-level agent: drops every table, clears the alarm, and
	* aborts the isolate.
	*
	* On a sub-agent (facet): delegates teardown to the immediate
	* parent so the parent-owned schedule rows for this sub-agent
	* (and any of its descendants) are cancelled, the parent's
	* `cf_agents_sub_agents` registry entry is cleared, and
	* `ctx.facets.delete` wipes the facet's own storage. The
	* `ctx.facets.delete` call aborts this isolate, so this method
	* may not return cleanly when invoked from inside the facet —
	* callers should treat it as fire-and-forget.
	*/
	async destroy() {
		if (this._isFacet) {
			this._emit("destroy");
			await (await this._rootAlarmOwner())._cf_destroyDescendantFacet(this.selfPath);
			return;
		}
		await this.ctx.storage.put(DESTROY_PENDING_KEY, true);
		await this.lifecycle.disableAlarms();
		await this.lifecycle.dispose();
		this._disposables.dispose();
		await this.ctx.storage.deleteAll();
		this._destroyed = true;
		setTimeout(() => {
			this.ctx.abort("destroyed");
		}, 0);
		this._emit("destroy");
	}
	/**
	* @internal Defer this agent's destruction to its own alarm invocation
	* instead of running it inline (#1625).
	*
	* `destroy()` is a multi-step I/O sequence (drop tables, delete alarm,
	* delete all storage, dispose connections). Running it on the `waitUntil`
	* of a request whose client has already disconnected — the MCP
	* Streamable-HTTP session-DELETE path — gives it little to no
	* post-invocation grace, so the runtime routinely cancels it mid-flight.
	* This method instead performs two fast storage writes (a durable
	* "condemned" marker and an immediate alarm) that the caller can await
	* before responding; the alarm then fires as a fresh invocation with its
	* own full execution budget and runs `destroy()` there. If even that
	* invocation is interrupted, the marker survives and the next wake
	* finishes teardown — see the `alarm()` preamble.
	*
	* Unlike `destroy()`, this method does not abort the isolate, so RPC
	* callers don't need to swallow an abort error.
	*/
	async _cf_scheduleDestroy() {
		await this.__unsafe_ensureInitialized();
		if (this._isFacet) {
			await this.destroy();
			return;
		}
		const destroyAt = Date.now() + DESTROY_ALARM_DELAY_MS;
		await this.ctx.storage.put(DESTROY_PENDING_KEY, destroyAt);
		await this.lifecycle.rearmAlarm();
	}
	/**
	* Whether a (deferred or interrupted) destroy is pending. Reads the
	* durable marker directly — the in-memory `_isFacet` flag may not be
	* hydrated yet at the call sites, but facets never write the marker.
	*/
	async _pendingDestroyAlarm() {
		const pending = await this.ctx.storage.get(DESTROY_PENDING_KEY);
		if (typeof pending === "number") return pending;
		return pending === true ? Date.now() : null;
	}
	async _hasPendingDestroy() {
		return await this._pendingDestroyAlarm() !== null;
	}
	/**
	* Check if a method is callable
	* @param method The method name to check
	* @returns True if the method is marked as callable
	*/
	_isCallable(method) {
		return callableMetadata.has(this[method]);
	}
	/**
	* Get all methods marked as callable on this Agent
	* @returns A map of method names to their metadata
	*/
	getCallableMethods() {
		const result = /* @__PURE__ */ new Map();
		let prototype = Object.getPrototypeOf(this);
		while (prototype && prototype !== Object.prototype) {
			for (const name of Object.getOwnPropertyNames(prototype)) {
				if (name === "constructor") continue;
				if (result.has(name)) continue;
				try {
					const fn = prototype[name];
					if (typeof fn === "function") {
						const meta = callableMetadata.get(fn);
						if (meta) result.set(name, meta);
					}
				} catch (e) {
					if (!(e instanceof TypeError)) throw e;
				}
			}
			prototype = Object.getPrototypeOf(prototype);
		}
		return result;
	}
	/**
	* Start a workflow and track it in this Agent's database.
	* Automatically injects agent identity into the workflow params.
	*
	* The originating Agent identity is persisted in the workflow params so
	* callbacks (`this.agent` RPC, progress/completion/error, state updates)
	* route back to the exact Agent or sub-agent facet that started the run.
	* Note the following constraints:
	*
	* - **Resolution is by name.** Callbacks re-resolve the originating Agent via
	*   `getAgentByName(...)`. Agents addressed by a raw Durable Object id
	*   (`idFromString`/`get(id)`) rather than by name will not receive
	*   callbacks on the same instance.
	* - **Sub-agent runs are facet-local.** A workflow started from a sub-agent
	*   is tracked in that facet's own storage; the parent's `getWorkflows()` /
	*   `getWorkflowById()` do not see it. Aggregate across facets yourself if
	*   you need a combined view.
	* - **Class names must survive bundling.** The originating path is keyed by
	*   `constructor.name`. Ensure your bundler preserves class names
	*   (e.g. esbuild `keepNames: true`) so callbacks can be routed.
	*
	* @template P - Type of params to pass to the workflow
	* @param workflowName - Name of the workflow binding in env (e.g., 'MY_WORKFLOW')
	* @param params - Params to pass to the workflow
	* @param options - Optional workflow options. For sub-agents, pass
	*   `agentBinding` as the **root** Agent's Durable Object binding name, not a
	*   child binding.
	* @returns The workflow instance ID
	*
	* @example
	* ```typescript
	* const workflowId = await this.runWorkflow(
	*   'MY_WORKFLOW',
	*   { taskId: '123', data: 'process this' }
	* );
	* ```
	*/
	async runWorkflow(workflowName, params, options) {
		const workflow = this._findWorkflowBindingByName(workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowName}' not found in environment`);
		const agentOrigin = this._workflowOrigin(options);
		if (!agentOrigin) throw new Error("Could not detect Agent binding name from class name. Pass it explicitly via options.agentBinding");
		const workflowId = options?.id ?? `wf_${nanoid()}`;
		const augmentedParams = {
			...params,
			__agentName: this.name,
			__agentBinding: agentOrigin.kind === "agent" ? agentOrigin.binding : agentOrigin.rootBinding,
			__workflowName: workflowName,
			__agentOrigin: agentOrigin
		};
		const instance = await workflow.create({
			id: workflowId,
			params: augmentedParams,
			retention: options?.retention
		});
		const id = nanoid();
		const metadataJson = options?.metadata ? JSON.stringify(options.metadata) : null;
		try {
			this.sql`
        INSERT INTO cf_agents_workflows (id, workflow_id, workflow_name, status, metadata)
        VALUES (${id}, ${instance.id}, ${workflowName}, 'queued', ${metadataJson})
      `;
		} catch (e) {
			if (e instanceof Error && e.message.includes("UNIQUE constraint failed")) throw new Error(`Workflow with ID "${workflowId}" is already being tracked`);
			throw e;
		}
		this._emit("workflow:start", {
			workflowId: instance.id,
			workflowName
		});
		return instance.id;
	}
	/**
	* Send an event to a running workflow.
	* The workflow can wait for this event using step.waitForEvent().
	*
	* @param workflowName - Name of the workflow binding in env (e.g., 'MY_WORKFLOW')
	* @param workflowId - ID of the workflow instance
	* @param event - Event to send
	*
	* @example
	* ```typescript
	* await this.sendWorkflowEvent(
	*   'MY_WORKFLOW',
	*   workflowId,
	*   { type: 'approval', payload: { approved: true } }
	* );
	* ```
	*/
	async sendWorkflowEvent(workflowName, workflowId, event) {
		const workflow = this._findWorkflowBindingByName(workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowName}' not found in environment`);
		const instance = await workflow.get(workflowId);
		await tryN(3, async () => instance.sendEvent(event), {
			shouldRetry: isErrorRetryable,
			baseDelayMs: 200,
			maxDelayMs: 3e3
		});
		this._emit("workflow:event", {
			workflowId,
			eventType: event.type
		});
	}
	/**
	* Approve a waiting workflow.
	* Sends an approval event to the workflow that can be received by waitForApproval().
	*
	* @param workflowId - ID of the workflow to approve
	* @param data - Optional approval data (reason, metadata)
	*
	* @example
	* ```typescript
	* await this.approveWorkflow(workflowId, {
	*   reason: 'Approved by admin',
	*   metadata: { approvedBy: userId }
	* });
	* ```
	*/
	async approveWorkflow(workflowId, data) {
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		await this.sendWorkflowEvent(workflowInfo.workflowName, workflowId, {
			type: "approval",
			payload: {
				approved: true,
				reason: data?.reason,
				metadata: data?.metadata
			}
		});
		this._emit("workflow:approved", {
			workflowId,
			reason: data?.reason
		});
	}
	/**
	* Reject a waiting workflow.
	* Sends a rejection event to the workflow that will cause waitForApproval() to throw.
	*
	* @param workflowId - ID of the workflow to reject
	* @param data - Optional rejection data (reason)
	*
	* @example
	* ```typescript
	* await this.rejectWorkflow(workflowId, {
	*   reason: 'Request denied by admin'
	* });
	* ```
	*/
	async rejectWorkflow(workflowId, data) {
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		await this.sendWorkflowEvent(workflowInfo.workflowName, workflowId, {
			type: "approval",
			payload: {
				approved: false,
				reason: data?.reason
			}
		});
		this._emit("workflow:rejected", {
			workflowId,
			reason: data?.reason
		});
	}
	/**
	* Terminate a running workflow.
	* This immediately stops the workflow and sets its status to "terminated".
	*
	* @param workflowId - ID of the workflow to terminate (must be tracked via runWorkflow)
	* @throws Error if workflow not found in tracking table
	* @throws Error if workflow binding not found in environment
	* @throws Error if workflow is already completed/errored/terminated (from Cloudflare)
	*
	* @example
	* ```typescript
	* await this.terminateWorkflow(workflowId);
	* ```
	*/
	async terminateWorkflow(workflowId) {
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		const workflow = this._findWorkflowBindingByName(workflowInfo.workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowInfo.workflowName}' not found in environment`);
		const instance = await workflow.get(workflowId);
		await tryN(3, async () => instance.terminate(), {
			shouldRetry: isErrorRetryable,
			baseDelayMs: 200,
			maxDelayMs: 3e3
		});
		const status = await instance.status();
		this._updateWorkflowTracking(workflowId, status);
		this._emit("workflow:terminated", {
			workflowId,
			workflowName: workflowInfo.workflowName
		});
	}
	/**
	* Pause a running workflow.
	* The workflow can be resumed later with resumeWorkflow().
	*
	* @param workflowId - ID of the workflow to pause (must be tracked via runWorkflow)
	* @throws Error if workflow not found in tracking table
	* @throws Error if workflow binding not found in environment
	* @throws Error if workflow is not running (from Cloudflare)
	*
	* @example
	* ```typescript
	* await this.pauseWorkflow(workflowId);
	* ```
	*/
	async pauseWorkflow(workflowId) {
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		const workflow = this._findWorkflowBindingByName(workflowInfo.workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowInfo.workflowName}' not found in environment`);
		const instance = await workflow.get(workflowId);
		await tryN(3, async () => instance.pause(), {
			shouldRetry: isErrorRetryable,
			baseDelayMs: 200,
			maxDelayMs: 3e3
		});
		const status = await instance.status();
		this._updateWorkflowTracking(workflowId, status);
		this._emit("workflow:paused", {
			workflowId,
			workflowName: workflowInfo.workflowName
		});
	}
	/**
	* Resume a paused workflow.
	*
	* @param workflowId - ID of the workflow to resume (must be tracked via runWorkflow)
	* @throws Error if workflow not found in tracking table
	* @throws Error if workflow binding not found in environment
	* @throws Error if workflow is not paused (from Cloudflare)
	*
	* @example
	* ```typescript
	* await this.resumeWorkflow(workflowId);
	* ```
	*/
	async resumeWorkflow(workflowId) {
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		const workflow = this._findWorkflowBindingByName(workflowInfo.workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowInfo.workflowName}' not found in environment`);
		const instance = await workflow.get(workflowId);
		await tryN(3, async () => instance.resume(), {
			shouldRetry: isErrorRetryable,
			baseDelayMs: 200,
			maxDelayMs: 3e3
		});
		const status = await instance.status();
		this._updateWorkflowTracking(workflowId, status);
		this._emit("workflow:resumed", {
			workflowId,
			workflowName: workflowInfo.workflowName
		});
	}
	/**
	* Restart a workflow instance.
	* This re-runs the workflow from the beginning with the same ID.
	*
	* @param workflowId - ID of the workflow to restart (must be tracked via runWorkflow)
	* @param options - Optional settings
	* @param options.resetTracking - If true (default), resets created_at and clears error fields.
	*                                If false, preserves original timestamps.
	* @throws Error if workflow not found in tracking table
	* @throws Error if workflow binding not found in environment
	*
	* @example
	* ```typescript
	* // Reset tracking (default)
	* await this.restartWorkflow(workflowId);
	*
	* // Preserve original timestamps
	* await this.restartWorkflow(workflowId, { resetTracking: false });
	* ```
	*/
	async restartWorkflow(workflowId, options = {}) {
		const { resetTracking = true } = options;
		const workflowInfo = this.getWorkflow(workflowId);
		if (!workflowInfo) throw new Error(`Workflow ${workflowId} not found in tracking table`);
		const workflow = this._findWorkflowBindingByName(workflowInfo.workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowInfo.workflowName}' not found in environment`);
		const instance = await workflow.get(workflowId);
		await tryN(3, async () => instance.restart(), {
			shouldRetry: isErrorRetryable,
			baseDelayMs: 200,
			maxDelayMs: 3e3
		});
		if (resetTracking) {
			const now = Math.floor(Date.now() / 1e3);
			this.sql`
        UPDATE cf_agents_workflows
        SET status = 'queued',
            created_at = ${now},
            updated_at = ${now},
            completed_at = NULL,
            error_name = NULL,
            error_message = NULL
        WHERE workflow_id = ${workflowId}
      `;
		} else {
			const status = await instance.status();
			this._updateWorkflowTracking(workflowId, status);
		}
		this._emit("workflow:restarted", {
			workflowId,
			workflowName: workflowInfo.workflowName
		});
	}
	/**
	* Find a workflow binding by its name.
	*/
	_findWorkflowBindingByName(workflowName) {
		const binding = this.env[workflowName];
		if (binding && typeof binding === "object" && "create" in binding && "get" in binding) return binding;
	}
	/**
	* Get all workflow binding names from the environment.
	*/
	_getWorkflowBindingNames() {
		const names = [];
		for (const [key, value] of Object.entries(this.env)) if (value && typeof value === "object" && "create" in value && "get" in value) names.push(key);
		return names;
	}
	/**
	* Get the status of a workflow and update the tracking record.
	*
	* @param workflowName - Name of the workflow binding in env (e.g., 'MY_WORKFLOW')
	* @param workflowId - ID of the workflow instance
	* @returns The workflow status
	*/
	async getWorkflowStatus(workflowName, workflowId) {
		const workflow = this._findWorkflowBindingByName(workflowName);
		if (!workflow) throw new Error(`Workflow binding '${workflowName}' not found in environment`);
		const status = await (await workflow.get(workflowId)).status();
		this._updateWorkflowTracking(workflowId, status);
		return status;
	}
	/**
	* Get a tracked workflow by ID.
	*
	* @param workflowId - Workflow instance ID
	* @returns Workflow info or undefined if not found
	*/
	getWorkflow(workflowId) {
		const rows = this.sql`
      SELECT * FROM cf_agents_workflows WHERE workflow_id = ${workflowId}
    `;
		if (!rows || rows.length === 0) return;
		return this._rowToWorkflowInfo(rows[0]);
	}
	/**
	* Query tracked workflows with cursor-based pagination.
	*
	* @param criteria - Query criteria including optional cursor for pagination
	* @returns WorkflowPage with workflows, total count, and next cursor
	*
	* @example
	* ```typescript
	* // First page
	* const page1 = this.getWorkflows({ status: 'running', limit: 20 });
	*
	* // Next page
	* if (page1.nextCursor) {
	*   const page2 = this.getWorkflows({
	*     status: 'running',
	*     limit: 20,
	*     cursor: page1.nextCursor
	*   });
	* }
	* ```
	*/
	getWorkflows(criteria = {}) {
		const limit = Math.min(criteria.limit ?? 50, 100);
		const isAsc = criteria.orderBy === "asc";
		const total = this._countWorkflows(criteria);
		let query = "SELECT * FROM cf_agents_workflows WHERE 1=1";
		const params = [];
		if (criteria.status) {
			const statuses = Array.isArray(criteria.status) ? criteria.status : [criteria.status];
			const placeholders = statuses.map(() => "?").join(", ");
			query += ` AND status IN (${placeholders})`;
			params.push(...statuses);
		}
		if (criteria.workflowName) {
			query += " AND workflow_name = ?";
			params.push(criteria.workflowName);
		}
		if (criteria.metadata) for (const [key, value] of Object.entries(criteria.metadata)) {
			query += ` AND json_extract(metadata, '$.' || ?) = ?`;
			params.push(key, value);
		}
		if (criteria.cursor) {
			const cursor = this._decodeCursor(criteria.cursor);
			if (isAsc) query += " AND (created_at > ? OR (created_at = ? AND workflow_id > ?))";
			else query += " AND (created_at < ? OR (created_at = ? AND workflow_id < ?))";
			params.push(cursor.createdAt, cursor.createdAt, cursor.workflowId);
		}
		query += ` ORDER BY created_at ${isAsc ? "ASC" : "DESC"}, workflow_id ${isAsc ? "ASC" : "DESC"}`;
		query += " LIMIT ?";
		params.push(limit + 1);
		const rows = this.ctx.storage.sql.exec(query, ...params).toArray();
		const hasMore = rows.length > limit;
		const workflows = (hasMore ? rows.slice(0, limit) : rows).map((row) => this._rowToWorkflowInfo(row));
		return {
			workflows,
			total,
			nextCursor: hasMore && workflows.length > 0 ? this._encodeCursor(workflows[workflows.length - 1]) : null
		};
	}
	/**
	* Count workflows matching criteria (for pagination total).
	*/
	_countWorkflows(criteria) {
		let query = "SELECT COUNT(*) as count FROM cf_agents_workflows WHERE 1=1";
		const params = [];
		if (criteria.status) {
			const statuses = Array.isArray(criteria.status) ? criteria.status : [criteria.status];
			const placeholders = statuses.map(() => "?").join(", ");
			query += ` AND status IN (${placeholders})`;
			params.push(...statuses);
		}
		if (criteria.workflowName) {
			query += " AND workflow_name = ?";
			params.push(criteria.workflowName);
		}
		if (criteria.metadata) for (const [key, value] of Object.entries(criteria.metadata)) {
			query += ` AND json_extract(metadata, '$.' || ?) = ?`;
			params.push(key, value);
		}
		if (criteria.createdBefore) {
			query += " AND created_at < ?";
			params.push(Math.floor(criteria.createdBefore.getTime() / 1e3));
		}
		return this.ctx.storage.sql.exec(query, ...params).toArray()[0]?.count ?? 0;
	}
	/**
	* Encode a cursor from workflow info for pagination.
	* Stores createdAt as Unix timestamp in seconds (matching DB storage).
	*/
	_encodeCursor(workflow) {
		return btoa(JSON.stringify({
			c: Math.floor(workflow.createdAt.getTime() / 1e3),
			i: workflow.workflowId
		}));
	}
	/**
	* Decode a pagination cursor.
	* Returns createdAt as Unix timestamp in seconds (matching DB storage).
	*/
	_decodeCursor(cursor) {
		try {
			const data = JSON.parse(atob(cursor));
			if (typeof data.c !== "number" || typeof data.i !== "string") throw new Error("Invalid cursor structure");
			return {
				createdAt: data.c,
				workflowId: data.i
			};
		} catch {
			throw new Error("Invalid pagination cursor. The cursor may be malformed or corrupted.");
		}
	}
	/**
	* Delete a workflow tracking record.
	*
	* @param workflowId - ID of the workflow to delete
	* @returns true if a record was deleted, false if not found
	*/
	deleteWorkflow(workflowId) {
		const existing = this.sql`
      SELECT COUNT(*) as count FROM cf_agents_workflows WHERE workflow_id = ${workflowId}
    `;
		if (!existing[0] || existing[0].count === 0) return false;
		this.sql`DELETE FROM cf_agents_workflows WHERE workflow_id = ${workflowId}`;
		return true;
	}
	/**
	* Delete workflow tracking records matching criteria.
	* Useful for cleaning up old completed/errored workflows.
	*
	* @param criteria - Criteria for which workflows to delete
	* @returns Number of records matching criteria (expected deleted count)
	*
	* @example
	* ```typescript
	* // Delete all completed workflows created more than 7 days ago
	* const deleted = this.deleteWorkflows({
	*   status: 'complete',
	*   createdBefore: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
	* });
	*
	* // Delete all errored and terminated workflows
	* const deleted = this.deleteWorkflows({
	*   status: ['errored', 'terminated']
	* });
	* ```
	*/
	deleteWorkflows(criteria = {}) {
		let query = "DELETE FROM cf_agents_workflows WHERE 1=1";
		const params = [];
		if (criteria.status) {
			const statuses = Array.isArray(criteria.status) ? criteria.status : [criteria.status];
			const placeholders = statuses.map(() => "?").join(", ");
			query += ` AND status IN (${placeholders})`;
			params.push(...statuses);
		}
		if (criteria.workflowName) {
			query += " AND workflow_name = ?";
			params.push(criteria.workflowName);
		}
		if (criteria.metadata) for (const [key, value] of Object.entries(criteria.metadata)) {
			query += ` AND json_extract(metadata, '$.' || ?) = ?`;
			params.push(key, value);
		}
		if (criteria.createdBefore) {
			query += " AND created_at < ?";
			params.push(Math.floor(criteria.createdBefore.getTime() / 1e3));
		}
		return this.ctx.storage.sql.exec(query, ...params).rowsWritten;
	}
	/**
	* Migrate workflow tracking records from an old binding name to a new one.
	* Use this after renaming a workflow binding in wrangler.toml.
	*
	* @param oldName - Previous workflow binding name
	* @param newName - New workflow binding name
	* @returns Number of records migrated
	*
	* @example
	* ```typescript
	* // After renaming OLD_WORKFLOW to NEW_WORKFLOW in wrangler.toml
	* async onStart() {
	*   const migrated = this.migrateWorkflowBinding('OLD_WORKFLOW', 'NEW_WORKFLOW');
	* }
	* ```
	*/
	migrateWorkflowBinding(oldName, newName) {
		if (!this._findWorkflowBindingByName(newName)) throw new Error(`Workflow binding '${newName}' not found in environment`);
		const count = this.sql`
      SELECT COUNT(*) as count FROM cf_agents_workflows WHERE workflow_name = ${oldName}
    `[0]?.count ?? 0;
		if (count > 0) {
			this.sql`UPDATE cf_agents_workflows SET workflow_name = ${newName} WHERE workflow_name = ${oldName}`;
			console.log(`[Agent] Migrated ${count} workflow(s) from '${oldName}' to '${newName}'`);
		}
		return count;
	}
	/**
	* Update workflow tracking record from InstanceStatus
	*/
	_updateWorkflowTracking(workflowId, status) {
		const statusName = status.status;
		const now = Math.floor(Date.now() / 1e3);
		const completedAt = [
			"complete",
			"errored",
			"terminated"
		].includes(statusName) ? now : null;
		const errorName = status.error?.name ?? null;
		const errorMessage = status.error?.message ?? null;
		this.sql`
      UPDATE cf_agents_workflows
      SET status = ${statusName},
          error_name = ${errorName},
          error_message = ${errorMessage},
          updated_at = ${now},
          completed_at = ${completedAt}
      WHERE workflow_id = ${workflowId}
    `;
	}
	/**
	* Convert a database row to WorkflowInfo
	*/
	_rowToWorkflowInfo(row) {
		return {
			id: row.id,
			workflowId: row.workflow_id,
			workflowName: row.workflow_name,
			status: row.status,
			metadata: row.metadata ? JSON.parse(row.metadata) : null,
			error: row.error_name ? {
				name: row.error_name,
				message: row.error_message ?? ""
			} : null,
			createdAt: /* @__PURE__ */ new Date(row.created_at * 1e3),
			updatedAt: /* @__PURE__ */ new Date(row.updated_at * 1e3),
			completedAt: row.completed_at ? /* @__PURE__ */ new Date(row.completed_at * 1e3) : null
		};
	}
	_workflowOrigin(options) {
		if (this._isFacet) {
			const root = this._parentPath[0];
			const rootBindingName = options?.agentBinding ?? (root ? this._findAgentBindingNameForClass(root.className) : void 0);
			if (!rootBindingName) return void 0;
			return {
				kind: "facet",
				version: 1,
				rootBinding: rootBindingName,
				path: this.selfPath.map((step) => ({ ...step }))
			};
		}
		const agentBindingName = options?.agentBinding ?? this._findAgentBindingNameForClass(this._ParentClass.name);
		if (!agentBindingName) return void 0;
		return {
			kind: "agent",
			version: 1,
			binding: agentBindingName,
			name: this.name
		};
	}
	_findAgentBindingNameForClass(className) {
		for (const [key, value] of Object.entries(this.env)) if (value && typeof value === "object" && "idFromName" in value && typeof value.idFromName === "function") {
			if (key === className || camelCaseToKebabCase(key) === camelCaseToKebabCase(className)) return key;
		}
	}
	_findBindingNameForNamespace(namespace) {
		for (const [key, value] of Object.entries(this.env)) if (value === namespace) return key;
	}
	/**
	* Handle a callback from a workflow.
	* Invoked via the internal `_workflow_handleCallback` RPC whenever an
	* {@link AgentWorkflow} reports progress, completion, an error, or a custom
	* event back to its originating Agent (or sub-agent facet).
	* Override this to handle all callback types in one place.
	*
	* @param callback - The callback payload
	*/
	async onWorkflowCallback(callback) {
		const now = Math.floor(Date.now() / 1e3);
		switch (callback.type) {
			case "progress":
				this.sql`
          UPDATE cf_agents_workflows
          SET status = 'running', updated_at = ${now}
          WHERE workflow_id = ${callback.workflowId} AND status IN ('queued', 'waiting')
        `;
				await this.onWorkflowProgress(callback.workflowName, callback.workflowId, callback.progress);
				break;
			case "complete":
				this.sql`
          UPDATE cf_agents_workflows
          SET status = 'complete', updated_at = ${now}, completed_at = ${now}
          WHERE workflow_id = ${callback.workflowId}
            AND status NOT IN ('terminated', 'paused')
        `;
				await this.onWorkflowComplete(callback.workflowName, callback.workflowId, callback.result);
				break;
			case "error":
				this.sql`
          UPDATE cf_agents_workflows
          SET status = 'errored', updated_at = ${now}, completed_at = ${now},
              error_name = 'WorkflowError', error_message = ${callback.error}
          WHERE workflow_id = ${callback.workflowId}
            AND status NOT IN ('terminated', 'paused')
        `;
				await this.onWorkflowError(callback.workflowName, callback.workflowId, callback.error);
				break;
			case "event":
				await this.onWorkflowEvent(callback.workflowName, callback.workflowId, callback.event);
				break;
		}
	}
	/**
	* Called when a workflow reports progress.
	* Override to handle progress updates.
	*
	* @param workflowName - Workflow binding name
	* @param workflowId - ID of the workflow
	* @param progress - Typed progress data (default: DefaultProgress)
	*/
	async onWorkflowProgress(workflowName, workflowId, progress) {}
	/**
	* Called when a workflow completes successfully.
	* Override to handle completion.
	*
	* @param workflowName - Workflow binding name
	* @param workflowId - ID of the workflow
	* @param result - Optional result data
	*/
	async onWorkflowComplete(workflowName, workflowId, result) {}
	/**
	* Called when a workflow encounters an error.
	* Override to handle errors.
	*
	* @param workflowName - Workflow binding name
	* @param workflowId - ID of the workflow
	* @param error - Error message
	*/
	async onWorkflowError(workflowName, workflowId, error) {
		console.error(`Workflow error [${workflowName}/${workflowId}]: ${error}\nOverride onWorkflowError() in your Agent to handle workflow errors.`);
	}
	/**
	* Called when a workflow sends a custom event.
	* Override to handle custom events.
	*
	* @param workflowName - Workflow binding name
	* @param workflowId - ID of the workflow
	* @param event - Custom event payload
	*/
	async onWorkflowEvent(workflowName, workflowId, event) {}
	/**
	* Handle a workflow callback via RPC.
	* @internal - Called by AgentWorkflow, do not call directly
	*/
	async _workflow_handleCallback(callback) {
		await this.__unsafe_ensureInitialized();
		await this.onWorkflowCallback(callback);
	}
	/**
	* Broadcast a message to all connected clients via RPC.
	* @internal - Called by AgentWorkflow, do not call directly
	*/
	async _workflow_broadcast(message) {
		await this.__unsafe_ensureInitialized();
		this.broadcast(JSON.stringify(message));
	}
	/**
	* Update agent state via RPC.
	* @internal - Called by AgentWorkflow, do not call directly
	*/
	async _workflow_updateState(action, state) {
		await this.__unsafe_ensureInitialized();
		if (action === "set") this.setState(state);
		else if (action === "merge") {
			const currentState = this.state ?? {};
			this.setState({
				...currentState,
				...state
			});
		} else if (action === "reset") this.setState(this.initialState);
	}
	async addMcpServer(serverName, urlOrBinding, callbackHostOrOptions, agentsPrefix, options) {
		const isHttpTransport = typeof urlOrBinding === "string";
		const normalizedUrl = isHttpTransport ? new URL(urlOrBinding).href : void 0;
		let requestedId;
		if (typeof callbackHostOrOptions === "object" && callbackHostOrOptions !== null && typeof callbackHostOrOptions.id === "string") {
			const rawId = callbackHostOrOptions.id;
			requestedId = normalizeServerId(rawId);
		}
		const allServers = this.mcp.listServers();
		const existingServer = allServers.find((s) => s.name === serverName && (!isHttpTransport || new URL(s.server_url).href === normalizedUrl));
		if (requestedId) {
			const idConflict = allServers.find((s) => {
				if (s.id !== requestedId) return false;
				if (s.name !== serverName) return true;
				if (isHttpTransport) return new URL(s.server_url).href !== normalizedUrl;
				return false;
			});
			if (idConflict) throw new Error(`MCP server id "${requestedId}" is already in use by server "${idConflict.name}" (${idConflict.server_url}). Stable ids must be unique per (name, url).`);
			if (existingServer && existingServer.id !== requestedId) {
				await this.mcp.migrateServerId(existingServer.id, requestedId, this.name);
				existingServer.id = requestedId;
			}
		}
		if (existingServer && this.mcp.mcpConnections[existingServer.id]) {
			const conn = this.mcp.mcpConnections[existingServer.id];
			if (conn.connectionState === MCPConnectionState.AUTHENTICATING) {
				const authProvider = conn.options.transport.authProvider;
				const authUrl = await this._redeemableAuthUrl(existingServer.id, authProvider?.authUrl, authProvider) ?? await this._redeemableAuthUrl(existingServer.id, existingServer.auth_url, authProvider);
				if (authUrl) return {
					id: existingServer.id,
					state: MCPConnectionState.AUTHENTICATING,
					authUrl
				};
				const reconnectResult = await this.mcp.connectToServer(existingServer.id);
				if (reconnectResult.state === MCPConnectionState.AUTHENTICATING) {
					if (!reconnectResult.authUrl) throw new Error("OAuth configuration incomplete: missing authUrl");
					return {
						id: existingServer.id,
						state: reconnectResult.state,
						authUrl: reconnectResult.authUrl
					};
				}
				if (reconnectResult.state === MCPConnectionState.CONNECTED) {
					const discoverResult = await this.mcp.discoverIfConnected(existingServer.id);
					if (!discoverResult?.success) throw new Error(`Failed to discover MCP server capabilities: ${discoverResult?.error ?? "connection not found"}`);
					return {
						id: existingServer.id,
						state: MCPConnectionState.READY
					};
				}
				throw new Error(`Failed to connect to MCP server at ${normalizedUrl}: ${reconnectResult.error}`);
			}
			if (conn.connectionState === MCPConnectionState.FAILED) throw new Error(`MCP server "${serverName}" is in failed state: ${conn.connectionError}`);
			return {
				id: existingServer.id,
				state: MCPConnectionState.READY
			};
		}
		if (typeof urlOrBinding !== "string") {
			const rpcOpts = callbackHostOrOptions;
			const normalizedName = serverName.toLowerCase().replace(/\s+/g, "-");
			const reconnectId = requestedId ?? existingServer?.id;
			const { id } = await this.mcp.connect(`${RPC_DO_PREFIX}${normalizedName}`, {
				reconnect: reconnectId ? { id: reconnectId } : void 0,
				transport: {
					type: "rpc",
					namespace: urlOrBinding,
					name: normalizedName,
					props: rpcOpts?.props
				}
			});
			const conn = this.mcp.mcpConnections[id];
			if (conn && conn.connectionState === MCPConnectionState.CONNECTED) {
				const discoverResult = await this.mcp.discoverIfConnected(id);
				if (discoverResult && !discoverResult.success) throw new Error(`Failed to discover MCP server capabilities: ${discoverResult.error}`);
			} else if (conn && conn.connectionState === MCPConnectionState.FAILED) throw new Error(`Failed to connect to MCP server "${serverName}" via RPC: ${conn.connectionError}`);
			const bindingName = this._findBindingNameForNamespace(urlOrBinding);
			if (bindingName) this.mcp.saveRpcServerToStorage(id, serverName, normalizedName, bindingName, rpcOpts?.props);
			return {
				id,
				state: MCPConnectionState.READY
			};
		}
		const httpOptions = callbackHostOrOptions;
		let resolvedCallbackHost;
		let resolvedAgentsPrefix;
		let resolvedOptions;
		let resolvedCallbackPath;
		if (typeof httpOptions === "object" && httpOptions !== null) {
			resolvedCallbackHost = httpOptions.callbackHost;
			resolvedCallbackPath = httpOptions.callbackPath;
			resolvedAgentsPrefix = httpOptions.agentsPrefix ?? "agents";
			resolvedOptions = {
				client: httpOptions.client,
				transport: httpOptions.transport,
				retry: httpOptions.retry
			};
		} else {
			resolvedCallbackHost = httpOptions;
			resolvedAgentsPrefix = agentsPrefix ?? "agents";
			resolvedOptions = options;
		}
		if (!this._resolvedOptions.sendIdentityOnConnect && resolvedCallbackHost && !resolvedCallbackPath) throw new Error("callbackPath is required in addMcpServer options when sendIdentityOnConnect is false — the default callback URL would expose the instance name. Provide a callbackPath and route the callback request to this agent via getAgentByName.");
		if (!resolvedCallbackHost) {
			const { request, connection } = getCurrentAgent();
			if (request) {
				const requestUrl = new URL(request.url);
				resolvedCallbackHost = `${requestUrl.protocol}//${requestUrl.host}`;
			} else if (connection?.uri) {
				const connectionUrl = new URL(connection.uri);
				resolvedCallbackHost = `${connectionUrl.protocol}//${connectionUrl.host}`;
			}
		}
		let callbackUrl;
		if (resolvedCallbackHost) {
			const normalizedHost = resolvedCallbackHost.replace(/\/$/, "");
			callbackUrl = resolvedCallbackPath ? `${normalizedHost}/${resolvedCallbackPath.replace(/^\//, "")}` : `${normalizedHost}/${resolvedAgentsPrefix}/${camelCaseToKebabCase(this._ParentClass.name)}/${this.name}/callback`;
		}
		const id = requestedId ?? existingServer?.id ?? nanoid(8);
		let authProvider;
		if (callbackUrl) {
			authProvider = this.createMcpOAuthProvider(callbackUrl);
			authProvider.serverId = id;
		}
		const transportType = resolvedOptions?.transport?.type ?? "auto";
		let headerTransportOpts = {};
		if (resolvedOptions?.transport?.headers) headerTransportOpts = {
			eventSourceInit: { fetch: (url, init) => fetch(url, {
				...init,
				headers: resolvedOptions?.transport?.headers
			}) },
			requestInit: { headers: resolvedOptions?.transport?.headers }
		};
		await this.mcp.registerServer(id, {
			url: normalizedUrl,
			name: serverName,
			callbackUrl,
			client: resolvedOptions?.client,
			transport: {
				...headerTransportOpts,
				authProvider,
				type: transportType,
				skipIssuerMetadataValidation: resolvedOptions?.transport?.skipIssuerMetadataValidation
			},
			retry: resolvedOptions?.retry
		});
		const result = await this.mcp.connectToServer(id);
		if (result.state === MCPConnectionState.FAILED) throw new Error(`Failed to connect to MCP server at ${normalizedUrl}: ${result.error}`);
		if (result.state === MCPConnectionState.AUTHENTICATING) {
			if (!callbackUrl) throw new Error("This MCP server requires OAuth authentication. Provide callbackHost in addMcpServer options to enable the OAuth flow.");
			return {
				id,
				state: result.state,
				authUrl: result.authUrl
			};
		}
		const discoverResult = await this.mcp.discoverIfConnected(id);
		if (discoverResult && !discoverResult.success) throw new Error(`Failed to discover MCP server capabilities: ${discoverResult.error}`);
		return {
			id,
			state: MCPConnectionState.READY
		};
	}
	async _redeemableAuthUrl(serverId, authUrl, authProvider) {
		if (!this._isAbsoluteHttpUrl(authUrl) || !authProvider) return;
		const state = new URL(authUrl).searchParams.get("state");
		if (!state) return authUrl;
		authProvider.serverId = serverId;
		try {
			return (await authProvider.checkState(state)).valid ? authUrl : void 0;
		} catch {
			return;
		}
	}
	_isAbsoluteHttpUrl(value) {
		if (!value) return false;
		try {
			const url = new URL(value);
			return url.protocol === "http:" || url.protocol === "https:";
		} catch {
			return false;
		}
	}
	async removeMcpServer(id) {
		await this.mcp.removeServer(id);
	}
	getMcpServers() {
		const mcpState = {
			prompts: this.mcp.listPrompts(),
			resources: this.mcp.listResources(),
			servers: {},
			tools: this.mcp.listTools()
		};
		const servers = this.mcp.listServers();
		if (servers && Array.isArray(servers) && servers.length > 0) for (const server of servers) {
			const serverConn = this.mcp.mcpConnections[server.id];
			let defaultState = "not-connected";
			if (!serverConn && server.auth_url) defaultState = "authenticating";
			mcpState.servers[server.id] = {
				auth_url: server.auth_url,
				capabilities: serverConn?.serverCapabilities ?? null,
				error: sanitizeErrorString(serverConn?.connectionError ?? null),
				instructions: serverConn?.instructions ?? null,
				name: server.name,
				server_url: server.server_url,
				state: serverConn?.connectionState ?? defaultState
			};
		}
		return mcpState;
	}
	/**
	* Create the OAuth provider used when connecting to MCP servers that require authentication.
	*
	* Override this method in a subclass to supply a custom OAuth provider implementation,
	* for example to use pre-registered client credentials, mTLS-based authentication,
	* or any other OAuth flow beyond dynamic client registration.
	*
	* @example
	* // Custom OAuth provider
	* class MyAgent extends Agent {
	*   createMcpOAuthProvider(callbackUrl: string): AgentMcpOAuthProvider {
	*     return new MyCustomOAuthProvider(
	*       this.ctx.storage,
	*       this.name,
	*       callbackUrl
	*     );
	*   }
	* }
	*
	* @param callbackUrl The OAuth callback URL for the authorization flow
	* @returns An {@link AgentMcpOAuthProvider} instance used by {@link addMcpServer}
	*/
	createMcpOAuthProvider(callbackUrl) {
		return new DurableObjectOAuthClientProvider(this.ctx.storage, this.name, callbackUrl);
	}
	broadcastMcpServers() {
		this._broadcastProtocol(JSON.stringify({
			mcp: this.getMcpServers(),
			type: "cf_agent_mcp_servers"
		}));
	}
};
Agent.options = {};
Agent._CF_OOM_ALARM_STRIKES_KEY = "cf_agents:oom_alarm_strikes";
const wrappedClasses = /* @__PURE__ */ new Set();
var _email = /* @__PURE__ */ new WeakMap();
_Symbol$dispose = Symbol.dispose;
var EmailBridge = class extends RpcTarget {
	constructor(email) {
		super();
		_classPrivateFieldInitSpec(this, _email, void 0);
		_classPrivateFieldSet2(_email, this, email);
	}
	async getRaw() {
		const reader = _classPrivateFieldGet2(_email, this).raw.getReader();
		const chunks = [];
		let done = false;
		while (!done) {
			const { value, done: readerDone } = await reader.read();
			done = readerDone;
			if (value) chunks.push(value);
		}
		const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
		const combined = new Uint8Array(totalLength);
		let offset = 0;
		for (const chunk of chunks) {
			combined.set(chunk, offset);
			offset += chunk.length;
		}
		return combined;
	}
	setReject(reason) {
		_classPrivateFieldGet2(_email, this).setReject(reason);
	}
	forward(rcptTo, headers) {
		return _classPrivateFieldGet2(_email, this).forward(rcptTo, headers);
	}
	reply(options) {
		return _classPrivateFieldGet2(_email, this).reply(new EmailMessage(options.from, options.to, options.raw));
	}
	[_Symbol$dispose]() {}
};
const agentMapCache = /* @__PURE__ */ new WeakMap();
/**
* Route an email to the appropriate Agent
* @param email The email to route
* @param env The environment containing the Agent bindings
* @param options The options for routing the email
* @returns A promise that resolves when the email has been routed
*/
async function routeAgentEmail(email, env, options) {
	const routingInfo = await options.resolver(email, env);
	if (!routingInfo) {
		if (options.onNoRoute) await options.onNoRoute(email);
		else console.warn("No routing information found for email, dropping message");
		return;
	}
	if (!agentMapCache.has(env)) {
		const map = {};
		const originalNames = [];
		for (const [key, value] of Object.entries(env)) if (value && typeof value === "object" && "idFromName" in value && typeof value.idFromName === "function") {
			map[key] = value;
			map[camelCaseToKebabCase(key)] = value;
			map[key.toLowerCase()] = value;
			originalNames.push(key);
		}
		agentMapCache.set(env, {
			map,
			originalNames
		});
	}
	const cached = agentMapCache.get(env);
	const namespace = cached.map[routingInfo.agentName];
	if (!namespace) {
		const availableAgents = cached.originalNames.join(", ");
		throw new Error(`Agent namespace '${routingInfo.agentName}' not found in environment. Available agents: ${availableAgents}`);
	}
	const agent = await getAgentByName(namespace, routingInfo.agentId);
	const bridge = new EmailBridge(email);
	await agent._onEmail({
		from: email.from,
		to: email.to,
		headers: email.headers,
		rawSize: email.rawSize,
		_secureRouted: routingInfo._secureRouted,
		_bridge: bridge
	});
}
/**
* A wrapper for streaming responses in callable methods
*/
var StreamingResponse = class {
	constructor(connection, id) {
		this._closed = false;
		this._connection = connection;
		this._id = id;
	}
	_send(response) {
		const state = facetStreamingResponseDeliveryStates.get(this);
		if (!state) return sendRpcResponseIfOpen(this._connection, response);
		const delivery = sendFacetRpcResponseIfOpen(state.replyTarget, response);
		trackFacetStreamingResponseDelivery(this, delivery.completion);
		return delivery.sent;
	}
	/**
	* Whether the stream has been closed (via end() or error())
	*/
	get isClosed() {
		return this._closed;
	}
	/**
	* Send a chunk of data to the client
	* @param chunk The data to send
	* @returns false if stream is already closed (no-op), true if sent
	*/
	send(chunk) {
		if (this._closed) {
			console.warn("StreamingResponse.send() called after stream was closed - data not sent");
			return false;
		}
		const response = {
			done: false,
			id: this._id,
			result: chunk,
			success: true,
			type: "rpc"
		};
		return this._send(response);
	}
	/**
	* End the stream and send the final chunk (if any)
	* @param finalChunk Optional final chunk of data to send
	* @returns false if stream is already closed (no-op), true if sent
	*/
	end(finalChunk) {
		if (this._closed) return false;
		this._closed = true;
		const response = {
			done: true,
			id: this._id,
			result: finalChunk,
			success: true,
			type: "rpc"
		};
		return this._send(response);
	}
	/**
	* Send an error to the client and close the stream
	* @param message Error message to send
	* @returns false if stream is already closed (no-op), true if sent
	*/
	error(message) {
		if (this._closed) return false;
		this._closed = true;
		const response = {
			error: message,
			id: this._id,
			success: false,
			type: "rpc"
		};
		return this._send(response);
	}
};
//#endregion
export { getCurrentAgent as a, callable as i, DEFAULT_AGENT_STATIC_OPTIONS as n, routeAgentEmail as o, StreamingResponse as r, unstable_callable as s, Agent as t };

//# sourceMappingURL=src-5W6JNKVb.js.map