import { i as _classPrivateFieldInitSpec, n as _classPrivateFieldSet2, r as _assertClassBrand, t as _classPrivateFieldGet2 } from "./classPrivateFieldGet2-DZBYAB34.js";
import { t as LifecycleCapability } from "./capability-BjSKYpzg.js";
import { t as _classPrivateMethodInitSpec } from "./classPrivateMethodInitSpec-qMjJ6sHQ.js";
import { SqlError } from "./sql-error.js";
import { isDurableObjectCodeUpdateReset, isDurableObjectMemoryLimitReset, isPlatformTransientError, tryN, validateRetryOptions } from "./retries.js";
import { nanoid } from "nanoid";
import { parseCronExpression } from "cron-schedule";
//#region src/schedules/schedule-timing.ts
/**
* Pure timing rules for persistent schedules.
*
* A `ScheduleTiming` is the parsed "when and how it runs" half of a
* `Schedule` — everything except identity, callback, and payload. Parsing
* user-facing inputs here keeps Scheduler's storage code to one insert path.
*/
/** Longest allowed gap between interval executions: 30 days. */
const MAX_INTERVAL_SECONDS = 720 * 60 * 60;
/** True when this timing repeats and therefore deduplicates by default. */
function isRecurring(timing) {
	return timing.type === "cron" || timing.type === "interval";
}
/**
* Next wall-clock execution time for a cron expression.
*
* @param cron - A standard cron expression.
* @param nowMs - Current epoch time in milliseconds.
* @returns The next execution epoch time in milliseconds.
* @throws For an unparseable cron expression.
*/
function nextCronTimeMs(cron, nowMs) {
	return parseCronExpression(cron).getNextDate(new Date(nowMs)).getTime();
}
/**
* Parse a user-facing `when` into schedule timing.
*
* A `Date` runs once at that date, a number runs once after that many
* seconds, and a string is a recurring cron expression.
*
* @param when - The requested execution time or recurrence.
* @param nowMs - Current epoch time in milliseconds.
* @param callback - Callback name, used only in error messages.
* @returns The parsed timing.
* @throws For a `when` value that is not a `Date`, number, or string.
*/
function parseWhen(when, nowMs, callback) {
	if (when instanceof Date) return {
		type: "scheduled",
		time: Math.floor(when.getTime() / 1e3)
	};
	if (typeof when === "number") return {
		type: "delayed",
		time: Math.floor((nowMs + when * 1e3) / 1e3),
		delayInSeconds: when
	};
	if (typeof when === "string") return {
		type: "cron",
		time: Math.floor(nextCronTimeMs(when, nowMs) / 1e3),
		cron: when
	};
	throw new Error(`Invalid schedule type: ${JSON.stringify(when)}(${typeof when}) trying to schedule ${callback}`);
}
/**
* Reject an interval outside the supported range.
*
* @param intervalSeconds - The requested gap between executions.
* @throws For a non-positive interval or one longer than 30 days.
*/
function validateIntervalSeconds(intervalSeconds) {
	if (typeof intervalSeconds !== "number" || intervalSeconds <= 0) throw new Error("intervalSeconds must be a positive number");
	if (intervalSeconds > 2592e3) throw new Error(`intervalSeconds cannot exceed ${MAX_INTERVAL_SECONDS} seconds (30 days)`);
}
/**
* Parse a fixed interval into schedule timing.
*
* @param intervalSeconds - Seconds between executions.
* @param nowMs - Current epoch time in milliseconds.
* @returns The parsed timing with the first execution one interval from now.
* @throws For an interval outside the supported range.
*/
function parseInterval(intervalSeconds, nowMs) {
	validateIntervalSeconds(intervalSeconds);
	return {
		type: "interval",
		time: Math.floor((nowMs + intervalSeconds * 1e3) / 1e3),
		intervalSeconds
	};
}
//#endregion
//#region src/schedules/scheduler.ts
/**
* Lifecycle scheduling primitive. Owns the `cf_agents_schedules` table,
* callback retry policy, and due-row processing.
*
* Scheduler consumes only the standard capability services: storage, alarm
* coordination, the host invocation boundary, events, and routing. Lifecycle combines
* Scheduler's next wake-up candidate with contributions from other
* capabilities and the host before arming the Durable Object.
*/
const schedulerCallbackResolvers = /* @__PURE__ */ new WeakMap();
/**
* @internal Supply a composition-root fallback for callback names outside the
* registered map. Agent uses this to keep its historical name-based
* scheduling API (`this.schedule(60, "methodName")`) working: the resolver
* looks the method up on the Agent, and the resolved handler still runs
* inside the Lifecycle host boundary.
*/
function setSchedulerCallbackResolver(scheduler, resolver) {
	schedulerCallbackResolvers.set(scheduler, resolver);
}
const SCHEDULE_SCHEMA_VERSION_KEY = "cf_agents:schedules_schema_version";
const CURRENT_SCHEDULE_SCHEMA_VERSION = 1;
const DEFAULT_RETRY = {
	maxAttempts: 3,
	baseDelayMs: 100,
	maxDelayMs: 3e3
};
function parseRetryOptions(row) {
	return typeof row.retry_options === "string" ? JSON.parse(row.retry_options) : void 0;
}
function resolveRetryConfig(retry, defaults) {
	return {
		maxAttempts: retry?.maxAttempts ?? defaults.maxAttempts,
		baseDelayMs: retry?.baseDelayMs ?? defaults.baseDelayMs,
		maxDelayMs: retry?.maxDelayMs ?? defaults.maxDelayMs
	};
}
/**
* @internal Create or migrate the schedule table. Idempotent. Shared with
* Agent's constructor-time schema initialization so a brand-new Agent
* exposes the table before Lifecycle startup completes, exactly as it did
* before Scheduler was extracted; {@link Scheduler.onStart} applies the
* same migration for plain Lifecycle Objects.
*/
function ensureScheduleTable(storage) {
	const rawSql = (query, ...params) => storage.sql.exec(query, ...params);
	rawSql(`
        CREATE TABLE IF NOT EXISTS cf_agents_schedules (
          id TEXT PRIMARY KEY NOT NULL DEFAULT (randomblob(9)),
          callback TEXT,
          payload TEXT,
          type TEXT NOT NULL CHECK(type IN ('scheduled', 'delayed', 'cron', 'interval')),
          time INTEGER,
          delayInSeconds INTEGER,
          cron TEXT,
          intervalSeconds INTEGER,
          running INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (unixepoch()),
          execution_started_at INTEGER,
          retry_options TEXT,
          owner_path TEXT,
          owner_path_key TEXT
        )
      `);
	const addColumnIfMissing = (statement) => {
		try {
			rawSql(statement);
		} catch (error) {
			if (!(error instanceof Error ? error.message : String(error)).toLowerCase().includes("duplicate column")) throw error;
		}
	};
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN intervalSeconds INTEGER");
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN running INTEGER DEFAULT 0");
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN execution_started_at INTEGER");
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN retry_options TEXT");
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN owner_path TEXT");
	addColumnIfMissing("ALTER TABLE cf_agents_schedules ADD COLUMN owner_path_key TEXT");
	const rows = rawSql("SELECT sql FROM sqlite_master WHERE type='table' AND name='cf_agents_schedules'").toArray();
	const ddl = typeof rows[0]?.sql === "string" ? rows[0].sql : "";
	if (ddl && !ddl.includes("'interval'")) {
		rawSql("DROP TABLE IF EXISTS cf_agents_schedules_new");
		rawSql(`
        CREATE TABLE cf_agents_schedules_new (
          id TEXT PRIMARY KEY NOT NULL DEFAULT (randomblob(9)),
          callback TEXT,
          payload TEXT,
          type TEXT NOT NULL CHECK(type IN ('scheduled', 'delayed', 'cron', 'interval')),
          time INTEGER,
          delayInSeconds INTEGER,
          cron TEXT,
          intervalSeconds INTEGER,
          running INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (unixepoch()),
          execution_started_at INTEGER,
          retry_options TEXT,
          owner_path TEXT,
          owner_path_key TEXT
        )
      `);
		rawSql(`
      INSERT INTO cf_agents_schedules_new
        (id, callback, payload, type, time, delayInSeconds, cron,
         intervalSeconds, running, created_at, execution_started_at,
         retry_options, owner_path, owner_path_key)
      SELECT id, callback, payload, type, time, delayInSeconds, cron,
             intervalSeconds, running, created_at, execution_started_at,
             retry_options, owner_path, owner_path_key
      FROM cf_agents_schedules
    `);
		rawSql("DROP TABLE cf_agents_schedules");
		rawSql("ALTER TABLE cf_agents_schedules_new RENAME TO cf_agents_schedules");
	}
	rawSql("DELETE FROM cf_agents_schedules WHERE callback = '_cf_keepAliveHeartbeat'");
}
var _handlers = /* @__PURE__ */ new WeakMap();
var _retryDefaults = /* @__PURE__ */ new WeakMap();
var _hungScheduleTimeoutSeconds = /* @__PURE__ */ new WeakMap();
var _onError = /* @__PURE__ */ new WeakMap();
var _warnedStartupCallbacks = /* @__PURE__ */ new WeakMap();
var _executingRowId = /* @__PURE__ */ new WeakMap();
var _Scheduler_brand = /* @__PURE__ */ new WeakSet();
/**
* Persistent task scheduling for a Lifecycle Object.
*
* Register callbacks in the constructor and install the instance with
* `Lifecycle.use()`. Scheduler owns its SQL schema, task CRUD, callback
* retries, and due-row processing. It contributes its next wake time while
* Lifecycle owns the physical alarm, and it runs registered callbacks
* through Lifecycle's host invocation boundary.
*
* @experimental The API surface may change before stabilizing.
*/
var Scheduler = class extends LifecycleCapability {
	/**
	* Create a persistent Scheduler.
	*
	* @param options - Registered callbacks plus optional retry,
	* hung-interval, and error policy. Registering `callbacks` types
	* {@link set} and {@link every} against the map — names and
	* payloads are checked where the handlers are declared and where they are
	* scheduled. Names outside the map are rejected unless a composition-root
	* resolver supplies them — the internal aperture behind `Agent`'s
	* name-based scheduling API.
	*/
	constructor(options = {}) {
		super("scheduler");
		_classPrivateMethodInitSpec(this, _Scheduler_brand);
		_classPrivateFieldInitSpec(this, _handlers, void 0);
		_classPrivateFieldInitSpec(this, _retryDefaults, void 0);
		_classPrivateFieldInitSpec(this, _hungScheduleTimeoutSeconds, void 0);
		_classPrivateFieldInitSpec(this, _onError, void 0);
		_classPrivateFieldInitSpec(this, _warnedStartupCallbacks, /* @__PURE__ */ new Set());
		_classPrivateFieldInitSpec(this, _executingRowId, void 0);
		_classPrivateFieldSet2(_handlers, this, options.callbacks ?? {});
		_classPrivateFieldSet2(_retryDefaults, this, resolveRetryConfig(options.retry, DEFAULT_RETRY));
		_classPrivateFieldSet2(_hungScheduleTimeoutSeconds, this, options.hungScheduleTimeoutSeconds ?? 30);
		_classPrivateFieldSet2(_onError, this, options.onError);
	}
	/** Initialize and migrate schedule storage during Lifecycle startup. */
	async onStart() {
		_classPrivateFieldGet2(_warnedStartupCallbacks, this).clear();
		const storage = this.lifecycle.storage;
		if ((await storage.get(SCHEDULE_SCHEMA_VERSION_KEY) ?? 0) >= CURRENT_SCHEDULE_SCHEMA_VERSION) return;
		ensureScheduleTable(this.lifecycle.storage);
		await storage.put(SCHEDULE_SCHEMA_VERSION_KEY, CURRENT_SCHEDULE_SCHEMA_VERSION);
	}
	/** Execute due schedule rows during the Lifecycle alarm phase. */
	async onAlarm() {
		await _assertClassBrand(_Scheduler_brand, this, _fireDueSchedules).call(this);
	}
	/** Handle Scheduler protocol messages routed by another Lifecycle. */
	async onRoute(context) {
		const message = context.payload;
		const owner = context.source ?? null;
		switch (message.type) {
			case "schedule": return _assertClassBrand(_Scheduler_brand, this, _insert).call(this, owner, parseWhen(message.when, Date.now(), message.callback), message.callback, message.payload, message.options);
			case "every": return _assertClassBrand(_Scheduler_brand, this, _insert).call(this, owner, parseInterval(message.intervalSeconds, Date.now()), message.callback, message.payload, message.options);
			case "get": return _assertClassBrand(_Scheduler_brand, this, _getForOwner).call(this, owner, message.id);
			case "list": return _assertClassBrand(_Scheduler_brand, this, _listForOwner).call(this, owner, message.criteria);
			case "cancel": return _assertClassBrand(_Scheduler_brand, this, _cancelForOwner).call(this, owner, message.id);
			case "dispatch":
				await _assertClassBrand(_Scheduler_brand, this, _executeCallback).call(this, message.row);
				return true;
			default: throw new Error("Unknown routed Scheduler message");
		}
	}
	/** Contribute the earliest pending schedule to Lifecycle alarm selection. */
	getNextAlarm() {
		const nowMs = Date.now();
		const hungCutoffSeconds = Math.floor(nowMs / 1e3) - _classPrivateFieldGet2(_hungScheduleTimeoutSeconds, this);
		const nextSchedule = _assertClassBrand(_Scheduler_brand, this, _nextScheduleTimeMs).call(this, nowMs, hungCutoffSeconds);
		const hungIntervalRecheck = _assertClassBrand(_Scheduler_brand, this, _nextHungIntervalRecheckMs).call(this, hungCutoffSeconds);
		if (nextSchedule === null) return hungIntervalRecheck;
		if (hungIntervalRecheck === null) return nextSchedule;
		return Math.min(nextSchedule, hungIntervalRecheck);
	}
	/** Set a delayed, dated, or cron schedule for a registered callback. */
	async set(when, callback, payload, options) {
		await this.lifecycle.ready();
		_assertClassBrand(_Scheduler_brand, this, _validateSchedule).call(this, when, callback, options);
		const result = this.lifecycle.routes.source ? await this.lifecycle.routes.toRoot({
			type: "schedule",
			when,
			callback,
			payload,
			options
		}) : await _assertClassBrand(_Scheduler_brand, this, _insert).call(this, null, parseWhen(when, Date.now(), callback), callback, payload, options);
		_assertClassBrand(_Scheduler_brand, this, _emitCreated).call(this, result);
		return result.schedule;
	}
	/** Set a fixed-interval schedule for a registered callback. */
	async every(intervalSeconds, callback, payload, options) {
		await this.lifecycle.ready();
		_assertClassBrand(_Scheduler_brand, this, _validateInterval).call(this, intervalSeconds, callback, options?.retry);
		const result = this.lifecycle.routes.source ? await this.lifecycle.routes.toRoot({
			type: "every",
			intervalSeconds,
			callback,
			payload,
			options
		}) : await _assertClassBrand(_Scheduler_brand, this, _insert).call(this, null, parseInterval(intervalSeconds, Date.now()), callback, payload, options);
		_assertClassBrand(_Scheduler_brand, this, _emitCreated).call(this, result);
		return result.schedule;
	}
	/** Get a schedule by ID. Works inside routed sub-agents. */
	async get(id) {
		await this.lifecycle.ready();
		return this.lifecycle.routes.source ? await this.lifecycle.routes.toRoot({
			type: "get",
			id
		}) : _assertClassBrand(_Scheduler_brand, this, _getForOwner).call(this, null, id);
	}
	/** List schedules matching criteria. Works inside routed sub-agents. */
	async list(criteria = {}) {
		await this.lifecycle.ready();
		return this.lifecycle.routes.source ? await this.lifecycle.routes.toRoot({
			type: "list",
			criteria
		}) : _assertClassBrand(_Scheduler_brand, this, _listForOwner).call(this, null, criteria);
	}
	/**
	* Cancel one schedule owned by this Scheduler.
	*
	* @param id - ID of the schedule to cancel.
	* @returns True when a schedule was cancelled, false when none matched.
	*/
	async cancel(id) {
		await this.lifecycle.ready();
		const result = this.lifecycle.routes.source ? await this.lifecycle.routes.toRoot({
			type: "cancel",
			id
		}) : await _assertClassBrand(_Scheduler_brand, this, _cancelForOwner).call(this, null, id);
		if (result.ok && result.callback) _assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:cancel", {
			callback: result.callback,
			id
		});
		return result.ok;
	}
	/**
	* @internal Synchronous read backing Agent's deprecated `getSchedule()`.
	* Not part of the primitive's contract — use {@link get}. Cannot cross
	* Durable Object boundaries and throws inside routed sub-agents.
	*/
	__DO_NOT_USE_WILL_REMOVE__getSchedule(id) {
		if (this.lifecycle.routes.source) throw new Error("getSchedule() is synchronous and cannot read routed schedule storage. Use await getScheduleById(id) on Agent, or await scheduler.get(id) on a standalone Scheduler.");
		return _assertClassBrand(_Scheduler_brand, this, _getForOwner).call(this, null, id);
	}
	/**
	* @internal Synchronous read backing Agent's deprecated `getSchedules()`.
	* Not part of the primitive's contract — use {@link list}. Cannot cross
	* Durable Object boundaries and throws inside routed sub-agents.
	*/
	__DO_NOT_USE_WILL_REMOVE__getSchedules(criteria = {}) {
		if (this.lifecycle.routes.source) throw new Error("getSchedules() is synchronous and cannot read routed schedule storage. Use await listSchedules(criteria) on Agent, or await scheduler.list(criteria) on a standalone Scheduler.");
		return _assertClassBrand(_Scheduler_brand, this, _listForOwner).call(this, null, criteria);
	}
	/** @internal Remove rows owned by one routed Lifecycle subtree. */
	__DO_NOT_USE_WILL_BREAK__cleanupRoutePrefix(prefix) {
		_assertClassBrand(_Scheduler_brand, this, _cancelOwners).call(this, (owner) => owner.key === prefix || owner.key.startsWith(`${prefix}/`));
	}
	/** @internal Apply Agent's outer alarm memory-limit policy to Scheduler rows. */
	__DO_NOT_USE_WILL_BREAK__handleAlarmMemoryLimit(options) {
		const executingRowId = _classPrivateFieldGet2(_executingRowId, this);
		_classPrivateFieldSet2(_executingRowId, this, void 0);
		if (options.sealed) {
			_assertClassBrand(_Scheduler_brand, this, _deleteRows).call(this, options.callbacks, executingRowId);
			return;
		}
		if (options.nextTime === void 0) throw new Error("Scheduler memory-limit backoff requires nextTime");
		_assertClassBrand(_Scheduler_brand, this, _moveRows).call(this, options.callbacks, executingRowId, options.nextTime);
	}
};
function _validateSchedule(when, callback, options) {
	if (typeof callback !== "string") throw new Error("Callback must be a string");
	if (!_assertClassBrand(_Scheduler_brand, this, _hasCallback).call(this, callback)) throw new Error(`Unknown scheduled callback "${callback}": not registered on this Scheduler`);
	if (options?.retry) validateRetryOptions(options.retry, _classPrivateFieldGet2(_retryDefaults, this));
	if (!(when instanceof Date) && typeof when !== "number" && typeof when !== "string") throw new Error(`Invalid schedule type: ${JSON.stringify(when)}(${typeof when}) trying to schedule ${callback}`);
	_assertClassBrand(_Scheduler_brand, this, _warnWhenScheduledDuringStartup).call(this, when, callback, options);
}
function _validateInterval(intervalSeconds, callback, retry) {
	validateIntervalSeconds(intervalSeconds);
	if (typeof callback !== "string") throw new Error("Callback must be a string");
	if (!_assertClassBrand(_Scheduler_brand, this, _hasCallback).call(this, callback)) throw new Error(`Unknown scheduled callback "${callback}": not registered on this Scheduler`);
	if (retry) validateRetryOptions(retry, _classPrivateFieldGet2(_retryDefaults, this));
}
/**
* A non-idempotent one-shot created during startup accumulates one row per
* Durable Object wake, whether it came from the host's onStart or another
* startup hook. Warn once per callback; an explicit `idempotent` choice
* (either value) opts out.
*/
function _warnWhenScheduledDuringStartup(when, callback, options) {
	if (!this.lifecycle.starting()) return;
	if (options?.idempotent !== void 0) return;
	if (typeof when === "string") return;
	if (_classPrivateFieldGet2(_warnedStartupCallbacks, this).has(callback)) return;
	_classPrivateFieldGet2(_warnedStartupCallbacks, this).add(callback);
	console.warn(`Scheduling "${callback}" during startup (e.g. onStart()) without { idempotent: true } creates a new row on every Durable Object restart, which can cause duplicate executions. Pass { idempotent: true } to deduplicate, or use an interval schedule for recurring tasks.`);
}
/** Resolve a name to its registered or composition-root-supplied handler. */
function _resolveCallback(name) {
	const handler = _classPrivateFieldGet2(_handlers, this)[name];
	if (handler) return handler;
	return schedulerCallbackResolvers.get(this)?.(name);
}
/** True when a name resolves to a runnable callback. */
function _hasCallback(name) {
	return _assertClassBrand(_Scheduler_brand, this, _resolveCallback).call(this, name) !== void 0;
}
/** Run one resolved callback inside the host invocation boundary. */
async function _invokeCallback(name, payload, schedule) {
	const handler = _assertClassBrand(_Scheduler_brand, this, _resolveCallback).call(this, name);
	if (!handler) throw new Error(`Unknown scheduled callback "${name}"`);
	await this.lifecycle.runInHostContext(() => handler(payload, schedule));
}
function _sql(strings, ...values) {
	const query = strings.reduce((result, part, index) => result + part + (index < values.length ? "?" : ""), "");
	try {
		return [...this.lifecycle.storage.sql.exec(query, ...values)];
	} catch (cause) {
		throw new SqlError(query, cause);
	}
}
/**
* Insert a schedule row for the given owner, or return the existing row
* when an idempotent request matches one. One-shot timings deduplicate only
* when `idempotent: true` is passed; recurring timings deduplicate unless
* `idempotent: false` opts out. `created: false` marks a dedup hit so
* callers suppress the `schedule:create` event.
*/
async function _insert(owner, timing, callback, payload, options) {
	const payloadJson = JSON.stringify(payload);
	if (isRecurring(timing) ? options?.idempotent !== false : Boolean(options?.idempotent)) {
		const existing = _assertClassBrand(_Scheduler_brand, this, _findMatchingRow).call(this, owner?.key ?? null, timing, callback, payloadJson);
		if (existing) {
			await this.lifecycle.alarms.rearm();
			return {
				schedule: _assertClassBrand(_Scheduler_brand, this, _rowToSchedule).call(this, existing),
				created: false
			};
		}
	}
	const id = nanoid(9);
	_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      INSERT OR REPLACE INTO cf_agents_schedules
        (id, callback, payload, type, time, delayInSeconds, cron,
         intervalSeconds, running, retry_options, owner_path, owner_path_key)
      VALUES
        (${id}, ${callback}, ${payloadJson}, ${timing.type}, ${timing.time},
         ${timing.type === "delayed" ? timing.delayInSeconds : null},
         ${timing.type === "cron" ? timing.cron : null},
         ${timing.type === "interval" ? timing.intervalSeconds : null},
         0,
         ${options?.retry ? JSON.stringify(options.retry) : null},
         ${owner?.data ?? null}, ${owner?.key ?? null})
    `;
	await this.lifecycle.alarms.rearm();
	return {
		schedule: {
			id,
			callback,
			payload,
			retry: options?.retry,
			...timing
		},
		created: true
	};
}
/** Find the row an idempotent insert deduplicates onto, if any. */
function _findMatchingRow(ownerKey, timing, callback, payloadJson) {
	let query = "SELECT * FROM cf_agents_schedules WHERE type = ? AND callback = ? AND payload IS ? AND owner_path_key IS ?";
	const params = [
		timing.type,
		callback,
		payloadJson,
		ownerKey
	];
	if (timing.type === "cron") {
		query += " AND cron = ?";
		params.push(timing.cron);
	}
	if (timing.type === "interval") {
		query += " AND intervalSeconds = ?";
		params.push(timing.intervalSeconds);
	}
	let rows;
	try {
		rows = this.lifecycle.storage.sql.exec(query, ...params).toArray();
	} catch (cause) {
		throw new SqlError(query, cause);
	}
	return rows[0];
}
function _rowToSchedule(row) {
	const base = {
		callback: row.callback,
		id: row.id,
		payload: JSON.parse(row.payload),
		retry: parseRetryOptions(row)
	};
	switch (row.type) {
		case "scheduled": return {
			...base,
			time: row.time,
			type: "scheduled"
		};
		case "delayed": return {
			...base,
			delayInSeconds: row.delayInSeconds ?? 0,
			time: row.time,
			type: "delayed"
		};
		case "cron": return {
			...base,
			cron: row.cron ?? "",
			time: row.time,
			type: "cron"
		};
		case "interval": return {
			...base,
			intervalSeconds: row.intervalSeconds ?? 0,
			time: row.time,
			type: "interval"
		};
	}
}
function _getForOwner(owner, id) {
	const ownerPathKey = owner?.key ?? null;
	const result = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT * FROM cf_agents_schedules
      WHERE id = ${id} AND owner_path_key IS ${ownerPathKey}
    `;
	if (result.length === 0) return void 0;
	return _assertClassBrand(_Scheduler_brand, this, _rowToSchedule).call(this, result[0]);
}
function _listForOwner(owner, criteria = {}) {
	let query = "SELECT * FROM cf_agents_schedules WHERE owner_path_key IS ?";
	const params = [owner?.key ?? null];
	if (criteria.id) {
		query += " AND id = ?";
		params.push(criteria.id);
	}
	if (criteria.type) {
		query += " AND type = ?";
		params.push(criteria.type);
	}
	if (criteria.timeRange) {
		query += " AND time >= ? AND time <= ?";
		const start = criteria.timeRange.start || /* @__PURE__ */ new Date(0);
		const end = criteria.timeRange.end || /* @__PURE__ */ new Date(999999999999999);
		params.push(Math.floor(start.getTime() / 1e3), Math.floor(end.getTime() / 1e3));
	}
	return this.lifecycle.storage.sql.exec(query, ...params).toArray().map((row) => _assertClassBrand(_Scheduler_brand, this, _rowToSchedule).call(this, row));
}
async function _cancelForOwner(owner, id) {
	const ownerPathKey = owner?.key ?? null;
	const result = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT * FROM cf_agents_schedules
      WHERE id = ${id} AND owner_path_key IS ${ownerPathKey}
    `;
	if (result.length === 0) return { ok: false };
	const callback = result[0].callback;
	_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      DELETE FROM cf_agents_schedules
      WHERE id = ${id} AND owner_path_key IS ${ownerPathKey}
    `;
	await this.lifecycle.alarms.rearm();
	return {
		ok: true,
		callback
	};
}
/** Cancel every owned row matched by a route-address predicate. */
function _cancelOwners(matches) {
	const rows = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT * FROM cf_agents_schedules
      WHERE owner_path IS NOT NULL
    `;
	for (const row of rows) {
		if (!row.owner_path || !matches({
			key: row.owner_path_key ?? row.owner_path,
			data: row.owner_path
		})) continue;
		_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:cancel", {
			callback: row.callback,
			id: row.id
		});
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`DELETE FROM cf_agents_schedules WHERE id = ${row.id}`;
	}
}
/** Delete rows selected by a host-owned failure policy. Best-effort. */
function _deleteRows(callbacks, executingRowId) {
	for (const callback of callbacks) try {
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
          DELETE FROM cf_agents_schedules WHERE callback = ${callback}
        `;
	} catch {}
	if (executingRowId) try {
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
          DELETE FROM cf_agents_schedules WHERE id = ${executingRowId}
        `;
	} catch {}
}
/** Delay rows selected by a host-owned failure policy. Best-effort. */
function _moveRows(callbacks, executingRowId, nextTime) {
	for (const callback of callbacks) try {
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
          UPDATE cf_agents_schedules
          SET time = ${nextTime}
          WHERE callback = ${callback} AND time <= ${nextTime}
        `;
	} catch {}
	if (executingRowId) try {
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
          UPDATE cf_agents_schedules
          SET time = ${nextTime}
          WHERE id = ${executingRowId} AND time <= ${nextTime}
        `;
	} catch {}
}
function _emit(type, payload) {
	this.lifecycle.events.emit(type, payload);
}
function _emitCreated(result) {
	if (!result.created) return;
	_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:create", {
		callback: result.schedule.callback,
		id: result.schedule.id
	});
}
/**
* Execute a local schedule row with retry handling.
*
* The capability remains outside ambient context. Lifecycle's host
* invocation boundary establishes host context only around the user
* callback itself.
*/
async function _executeCallback(row) {
	if (!_assertClassBrand(_Scheduler_brand, this, _hasCallback).call(this, row.callback)) {
		console.error(`callback ${row.callback} not found`);
		return;
	}
	const { maxAttempts, baseDelayMs, maxDelayMs } = resolveRetryConfig(parseRetryOptions(row), _classPrivateFieldGet2(_retryDefaults, this));
	let parsedPayload;
	try {
		parsedPayload = JSON.parse(row.payload);
	} catch (error) {
		console.error(`Failed to parse payload for schedule "${row.id}" (callback "${row.callback}")`, error);
		_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:error", {
			callback: row.callback,
			id: row.id,
			error: error instanceof Error ? error.message : String(error),
			attempts: 0
		});
		return;
	}
	const isOneShotSchedule = row.type === "delayed" || row.type === "scheduled";
	const shouldDeferReset = (error) => isOneShotSchedule && isDurableObjectCodeUpdateReset(error);
	const shouldDeferOnExhaustion = (error) => isOneShotSchedule && isPlatformTransientError(error);
	const shouldDeferMemoryLimit = (error) => isOneShotSchedule && isDurableObjectMemoryLimitReset(error);
	const schedule = _assertClassBrand(_Scheduler_brand, this, _rowToSchedule).call(this, row);
	try {
		_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:execute", {
			callback: row.callback,
			id: row.id
		});
		await tryN(maxAttempts, async (attempt) => {
			if (attempt > 1) _assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:retry", {
				callback: row.callback,
				id: row.id,
				attempt,
				maxAttempts
			});
			await _assertClassBrand(_Scheduler_brand, this, _invokeCallback).call(this, row.callback, parsedPayload, schedule);
		}, {
			baseDelayMs,
			maxDelayMs,
			shouldRetry: (error) => !shouldDeferReset(error)
		});
	} catch (error) {
		if (shouldDeferReset(error)) {
			console.warn(`Deferring scheduled callback "${row.callback}" to a fresh invocation after a Durable Object code-update reset; the one-shot row is preserved and the alarm will re-run on new code.`);
			throw error;
		}
		if (shouldDeferOnExhaustion(error)) {
			console.warn(`Deferring scheduled callback "${row.callback}" after exhausting in-process retries on a transient platform error; the one-shot row is preserved and the alarm will re-run once the platform recovers.`);
			throw error;
		}
		if (shouldDeferMemoryLimit(error)) {
			console.warn(`Deferring scheduled callback "${row.callback}" to the alarm memory-limit circuit breaker after a Durable Object memory-limit reset; the one-shot row is preserved so the breaker can bound the retry loop and seal it (#1825).`);
			throw error;
		}
		console.error(`error executing callback "${row.callback}" after ${maxAttempts} attempts`, error);
		_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:error", {
			callback: row.callback,
			id: row.id,
			error: error instanceof Error ? error.message : String(error),
			attempts: maxAttempts
		});
		try {
			await _classPrivateFieldGet2(_onError, this)?.call(this, error);
		} catch {}
	}
}
/** Execute every schedule row due in the current alarm phase. */
async function _fireDueSchedules() {
	const now = Math.floor(Date.now() / 1e3);
	const dueRows = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT * FROM cf_agents_schedules WHERE time <= ${now}
    `;
	_assertClassBrand(_Scheduler_brand, this, _warnStaleOneShots).call(this, dueRows);
	for (const row of dueRows) {
		const outcome = await _assertClassBrand(_Scheduler_brand, this, _runDueRow).call(this, row, now);
		if (outcome === "stop") return;
		if (outcome === "executed") _assertClassBrand(_Scheduler_brand, this, _advanceRow).call(this, row);
	}
}
/**
* Warn when many stale one-shot rows share the same callback — this
* usually means one-shot schedules were created repeatedly (e.g. in
* onStart) without idempotent:true and rows accumulated across restarts.
*/
function _warnStaleOneShots(dueRows) {
	const DUPLICATE_SCHEDULE_THRESHOLD = 10;
	const oneShotCounts = /* @__PURE__ */ new Map();
	for (const row of dueRows) if (row.type === "delayed" || row.type === "scheduled") oneShotCounts.set(row.callback, (oneShotCounts.get(row.callback) ?? 0) + 1);
	for (const [callback, count] of oneShotCounts) {
		if (count < DUPLICATE_SCHEDULE_THRESHOLD) continue;
		try {
			console.warn(`Processing ${count} stale "${callback}" schedules in a single alarm cycle. This usually means one-shot schedules are created repeatedly without the idempotent option. Consider an interval schedule for recurring tasks, or pass { idempotent: true } when creating one-shot schedules.`);
			_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:duplicate_warning", {
				callback,
				count,
				type: "one-shot"
			});
		} catch {}
	}
}
/**
* Run one due row.
*
* @returns "executed" when the row ran and should advance, "skipped" when
* it should be left for a later alarm cycle, and "stop" when host teardown
* disabled further durable work mid-phase.
*/
async function _runDueRow(row, now) {
	if (row.type === "interval" && row.running === 1) {
		const elapsedSeconds = now - (row.execution_started_at ?? 0);
		if (elapsedSeconds < _classPrivateFieldGet2(_hungScheduleTimeoutSeconds, this)) {
			console.warn(`Skipping interval schedule ${row.id}: previous execution still running`);
			return "skipped";
		}
		console.warn(`Forcing reset of hung interval schedule ${row.id} (started ${elapsedSeconds}s ago)`);
	}
	if (row.type === "interval") _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
        UPDATE cf_agents_schedules
        SET running = 1, execution_started_at = ${now}
        WHERE id = ${row.id}
      `;
	let executed = false;
	if (row.owner_path) try {
		executed = await this.lifecycle.routes.to({
			key: row.owner_path_key ?? row.owner_path,
			data: row.owner_path
		}, {
			type: "dispatch",
			row
		});
	} catch (error) {
		console.error(`error dispatching scheduled callback "${row.callback}"`, error);
		_assertClassBrand(_Scheduler_brand, this, _emit).call(this, "schedule:error", {
			callback: row.callback,
			id: row.id,
			error: error instanceof Error ? error.message : String(error),
			attempts: 0
		});
		try {
			await _classPrivateFieldGet2(_onError, this)?.call(this, error);
		} catch {}
		if (row.type === "interval") _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
            UPDATE cf_agents_schedules SET running = 0 WHERE id = ${row.id}
          `;
		return "skipped";
	}
	else {
		_classPrivateFieldSet2(_executingRowId, this, row.id);
		await _assertClassBrand(_Scheduler_brand, this, _executeCallback).call(this, row);
		_classPrivateFieldSet2(_executingRowId, this, void 0);
		executed = true;
	}
	if (this.lifecycle.alarms.disabled()) return "stop";
	return executed ? "executed" : "skipped";
}
/** Reschedule a recurring row or delete an executed one-shot row. */
function _advanceRow(row) {
	if (row.type === "cron") {
		const nextTimestamp = Math.floor(nextCronTimeMs(row.cron ?? "", Date.now()) / 1e3);
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
        UPDATE cf_agents_schedules SET time = ${nextTimestamp} WHERE id = ${row.id}
      `;
	} else if (row.type === "interval") {
		const nextTimestamp = Math.floor(Date.now() / 1e3) + (row.intervalSeconds ?? 0);
		_assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
        UPDATE cf_agents_schedules
        SET running = 0, time = ${nextTimestamp}
        WHERE id = ${row.id}
      `;
	} else _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
        DELETE FROM cf_agents_schedules WHERE id = ${row.id}
      `;
}
/**
* Earliest wall-clock time (ms) a schedule row is ready to execute,
* clamped to the future, or `null` when no row qualifies.
*/
function _nextScheduleTimeMs(nowMs, hungCutoffSeconds) {
	const readySchedules = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT time FROM cf_agents_schedules
      WHERE type != 'interval'
        OR running = 0
        OR coalesce(execution_started_at, 0) <= ${hungCutoffSeconds}
      ORDER BY time ASC
      LIMIT 1
    `;
	if (readySchedules.length > 0 && "time" in readySchedules[0]) return Math.max(readySchedules[0].time * 1e3, nowMs + 1);
	return null;
}
/**
* Wall-clock time (ms) at which the earliest still-running (not yet hung)
* interval schedule crosses the hung timeout and must be re-checked, or
* `null` when none is running.
*/
function _nextHungIntervalRecheckMs(hungCutoffSeconds) {
	const startedAt = _assertClassBrand(_Scheduler_brand, this, _sql).bind(this)`
      SELECT execution_started_at FROM cf_agents_schedules
      WHERE type = 'interval'
        AND running = 1
        AND coalesce(execution_started_at, 0) > ${hungCutoffSeconds}
      ORDER BY execution_started_at ASC
      LIMIT 1
    `[0]?.execution_started_at;
	if (startedAt !== null && startedAt !== void 0) return (startedAt + _classPrivateFieldGet2(_hungScheduleTimeoutSeconds, this)) * 1e3;
	return null;
}
//#endregion
export { ensureScheduleTable as n, setSchedulerCallbackResolver as r, Scheduler as t };

//# sourceMappingURL=scheduler-CR9RHGos.js.map