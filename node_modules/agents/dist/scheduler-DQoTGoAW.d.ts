import {
  s as LifecycleCapability,
  u as LifecycleRouteContext
} from "./capability-runner-CvHGZqUu.js";
import { t as RetryOptions } from "./retries-CAvxtG9d.js";

//#region src/schedules/types.d.ts
/**
 * A persisted task scheduled by an Agent.
 *
 * @template T Type of the callback payload.
 */
type Schedule<T = string> = {
  /** Unique schedule identifier. */ id: string /** Name of the Agent method invoked by the schedule. */;
  callback: string /** Data passed to the callback. */;
  payload: T /** Retry policy for callback execution. */;
  retry?: RetryOptions;
} & (
  | {
      /** One-time execution at a specific date. */ type: "scheduled" /** Unix timestamp in seconds. */;
      time: number;
    }
  | {
      /** One-time execution after a relative delay. */ type: "delayed" /** Unix timestamp in seconds. */;
      time: number /** Delay from creation in seconds. */;
      delayInSeconds: number;
    }
  | {
      /** Recurring execution from a cron expression. */ type: "cron" /** Unix timestamp in seconds for the next execution. */;
      time: number /** Cron expression defining the recurrence. */;
      cron: string;
    }
  | {
      /** Recurring execution at a fixed interval. */ type: "interval" /** Unix timestamp in seconds for the next execution. */;
      time: number /** Number of seconds between executions. */;
      intervalSeconds: number;
    }
);
/**
 * Constraint for a Scheduler's registered callback map: named handlers
 * receiving the parsed payload and the schedule that fired.
 *
 * @experimental The API surface may change before stabilizing.
 */
type SchedulerHandlers = Record<
  string,
  (payload: never, schedule: never) => unknown
>;
/**
 * Default callback surface for a Scheduler constructed without registered
 * callbacks: any name compiles with an untyped payload. At runtime a name
 * must be registered or supplied by a composition-root resolver (the
 * aperture behind Agent's name-based scheduling API); a bare Scheduler
 * rejects it otherwise.
 *
 * @experimental The API surface may change before stabilizing.
 */
type SchedulerCallbacks = Record<
  string,
  (payload: unknown, schedule: Schedule<unknown>) => unknown
>;
/**
 * The payload type a registered scheduler callback accepts.
 *
 * @experimental The API surface may change before stabilizing.
 */
type SchedulerPayload<Handler> = Handler extends (
  payload: infer Payload,
  ...rest: never[]
) => unknown
  ? Payload
  : never;
/** Options accepted when creating one schedule. */
type ScheduleOptions = {
  /** Retry policy for callback execution, overriding the Scheduler default. */ retry?: RetryOptions;
  /**
   * Deduplicate onto an existing matching schedule instead of creating a new
   * row. Defaults to `true` for cron and interval schedules, `false` for
   * one-shot schedules.
   */
  idempotent?: boolean;
};
/** Filters accepted by `getSchedules()` and `listSchedules()`. */
type ScheduleCriteria = {
  id?: string;
  type?: "scheduled" | "delayed" | "cron" | "interval";
  timeRange?: {
    start?: Date;
    end?: Date;
  };
};
//#endregion
//#region src/schedules/options.d.ts
/** Events emitted while a Scheduler creates, executes, retries, or cancels work. */
type SchedulerEventType =
  | "schedule:create"
  | "schedule:cancel"
  | "schedule:execute"
  | "schedule:retry"
  | "schedule:error"
  | "schedule:duplicate_warning";
/**
 * Optional callbacks and policy for a Scheduler capability.
 *
 * @experimental The API surface may change before stabilizing.
 */
interface SchedulerOptions<
  Handlers extends SchedulerHandlers = SchedulerCallbacks
> {
  /**
   * Named callbacks this Scheduler can run. Each schedule row persists a
   * callback name; registration in a field initializer re-binds the names on
   * every Durable Object wake, so register unconditionally. Names outside
   * this map are rejected unless a composition-root resolver supplies them
   * — the internal aperture behind `Agent`'s name-based scheduling API.
   */
  readonly callbacks?: Handlers;
  /** Default callback retry policy. */
  readonly retry?: RetryOptions;
  /** Seconds before an in-flight interval is treated as abandoned. Default: 30. */
  readonly hungScheduleTimeoutSeconds?: number;
  /** Observe terminal callback errors. Runs as capability code without host context. */
  readonly onError?: (error: unknown) => void | Promise<void>;
}
//#endregion
//#region src/schedules/scheduler.d.ts
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
declare class Scheduler<
  Handlers extends SchedulerHandlers = SchedulerCallbacks
> extends LifecycleCapability {
  #private;
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
  constructor(options?: SchedulerOptions<Handlers>);
  /** Initialize and migrate schedule storage during Lifecycle startup. */
  onStart(): Promise<void>;
  /** Execute due schedule rows during the Lifecycle alarm phase. */
  onAlarm(): Promise<void>;
  /** Handle Scheduler protocol messages routed by another Lifecycle. */
  onRoute(context: LifecycleRouteContext): Promise<unknown>;
  /** Contribute the earliest pending schedule to Lifecycle alarm selection. */
  getNextAlarm(): number | null;
  /** Set a delayed, dated, or cron schedule for a registered callback. */
  set<Name extends keyof Handlers & string>(
    when: Date | string | number,
    callback: Name,
    payload?: SchedulerPayload<Handlers[Name]>,
    options?: ScheduleOptions
  ): Promise<Schedule<SchedulerPayload<Handlers[Name]>>>;
  /** Set a fixed-interval schedule for a registered callback. */
  every<Name extends keyof Handlers & string>(
    intervalSeconds: number,
    callback: Name,
    payload?: SchedulerPayload<Handlers[Name]>,
    options?: ScheduleOptions
  ): Promise<Schedule<SchedulerPayload<Handlers[Name]>>>;
  /** Get a schedule by ID. Works inside routed sub-agents. */
  get(id: string): Promise<Schedule<unknown> | undefined>;
  /** List schedules matching criteria. Works inside routed sub-agents. */
  list(criteria?: ScheduleCriteria): Promise<Schedule<unknown>[]>;
  /**
   * Cancel one schedule owned by this Scheduler.
   *
   * @param id - ID of the schedule to cancel.
   * @returns True when a schedule was cancelled, false when none matched.
   */
  cancel(id: string): Promise<boolean>;
  /**
   * @internal Synchronous read backing Agent's deprecated `getSchedule()`.
   * Not part of the primitive's contract — use {@link get}. Cannot cross
   * Durable Object boundaries and throws inside routed sub-agents.
   */
  __DO_NOT_USE_WILL_REMOVE__getSchedule<T = string>(
    id: string
  ): Schedule<T> | undefined;
  /**
   * @internal Synchronous read backing Agent's deprecated `getSchedules()`.
   * Not part of the primitive's contract — use {@link list}. Cannot cross
   * Durable Object boundaries and throws inside routed sub-agents.
   */
  __DO_NOT_USE_WILL_REMOVE__getSchedules<T = string>(
    criteria?: ScheduleCriteria
  ): Schedule<T>[];
  /** @internal Remove rows owned by one routed Lifecycle subtree. */
  __DO_NOT_USE_WILL_BREAK__cleanupRoutePrefix(prefix: string): void;
  /** @internal Apply Agent's outer alarm memory-limit policy to Scheduler rows. */
  __DO_NOT_USE_WILL_BREAK__handleAlarmMemoryLimit(options: {
    readonly callbacks: ReadonlyArray<string>;
    readonly sealed: boolean;
    readonly nextTime?: number;
  }): void;
}
//#endregion
export {
  ScheduleCriteria as a,
  SchedulerHandlers as c,
  Schedule as i,
  SchedulerPayload as l,
  SchedulerEventType as n,
  ScheduleOptions as o,
  SchedulerOptions as r,
  SchedulerCallbacks as s,
  Scheduler as t
};
//# sourceMappingURL=scheduler-DQoTGoAW.d.ts.map
