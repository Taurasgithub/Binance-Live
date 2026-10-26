//#region src/lifecycle/capability.d.ts
/** Opaque address understood by a Lifecycle routing transport. */
type LifecycleRouteAddress = {
  /** Stable equality and storage key. */ readonly key: string /** Transport-owned serialized address. */;
  readonly data: string;
};
/** Context supplied with a routed capability message. */
type LifecycleRouteContext = {
  /** Address of the sending Lifecycle, or undefined for an unrouted root. */ readonly source:
    | LifecycleRouteAddress
    | undefined /** Capability-owned message payload. */;
  readonly payload: unknown;
};
/** Alarm coordination available to every Lifecycle capability. */
type LifecycleAlarms = {
  /** Recompute the physical alarm from installed capability state. */ readonly rearm: () => Promise<void>;
  /**
   * True once explicit host teardown permanently disabled alarm arming.
   * Capabilities stop dispatching durable work when this reports true.
   */
  readonly disabled: () => boolean;
};
/** Best-effort telemetry available to every Lifecycle capability. */
type LifecycleEvents = {
  /** Publish an event under this capability's stable identity. */ readonly emit: (
    type: string,
    payload: unknown
  ) => void;
};
/** Routing available to every Lifecycle capability. */
type LifecycleRoutes = {
  /** This Lifecycle's transport address, or undefined at the route root. */ readonly source:
    | LifecycleRouteAddress
    | undefined /** Route a capability-owned message to the root Lifecycle. */;
  readonly toRoot: (
    payload: unknown
  ) => Promise<unknown> /** Route a capability-owned message to another Lifecycle. */;
  readonly to: (
    target: LifecycleRouteAddress,
    payload: unknown
  ) => Promise<unknown>;
};
/**
 * Standard services granted to every installed Lifecycle capability.
 *
 * @experimental The API surface may change before stabilizing.
 */
type LifecycleServices = {
  readonly storage: DurableObjectStorage;
  readonly ready: () => Promise<void> /** True while capability and host startup hooks are still running. */;
  readonly starting: () => boolean;
  readonly alarms: LifecycleAlarms;
  /**
   * Run a capability-held user callback inside the host invocation context.
   * Capability hooks run outside host context; this is the one boundary for
   * entering it, and a host composition root may substitute its own wrapper
   * (Agent adds tracing span scope).
   */
  readonly runInHostContext: (fn: () => unknown) => Promise<unknown>;
  readonly events: LifecycleEvents;
  readonly routes: LifecycleRoutes;
};
/**
 * Base class for capabilities that consume standard Lifecycle services.
 *
 * @experimental The API surface may change before stabilizing.
 */
declare abstract class LifecycleCapability<Props extends object = object> {
  readonly capabilityId: string;
  protected constructor(capabilityId: string);
  /** Default startup hook; capabilities override when they own startup work. */
  onStart(_context: CapabilityStartContext<Props>): void;
  /** Standard services when installed, or undefined in isolated unit tests. */
  protected get lifecycleServices(): LifecycleServices | undefined;
  /** Standard services supplied when Lifecycle installs this capability. */
  protected get lifecycle(): LifecycleServices;
}
//#endregion
//#region src/lifecycle/capability-runner.d.ts
type MaybePromise<T> = T | Promise<T>;
/** One capability's requested physical alarm. */
type AlarmContribution =
  | number
  | {
      /** Epoch time in milliseconds. */ readonly time: number /** Ignore ordinary wake-time candidates while this request exists. */;
      readonly exclusive: true;
    }
  | null;
/** One best-effort event published by a Lifecycle capability. */
type LifecycleEvent = {
  /** Stable capability or subsystem name. */ readonly source: string /** Stable event name within that source. */;
  readonly type: string /** Event-specific data. */;
  readonly payload: unknown;
};
/** Context supplied when durable capabilities start. */
type CapabilityStartContext<Props extends object = object> = {
  /** Properties supplied while resolving the Durable Object. */ readonly props:
    | Props
    | undefined;
};
/** Context supplied when durable capabilities inspect an HTTP request. */
type CapabilityRequestContext = {
  /** The request entering the Durable Object. */ readonly request: Request;
};
/**
 * A capability installed into a Durable Object lifecycle.
 *
 * Capabilities extending `LifecycleCapability` receive the standard storage,
 * readiness, alarm, event, and routing surface. Host-specific bindings and
 * protocol adapters remain explicit constructor dependencies. Hook parameters
 * carry only phase data; hooks do not run in ambient host context.
 *
 * @experimental The API surface may change before stabilizing.
 */
interface DurableObjectCapability<Props extends object = object> {
  /** Initialize or recover the capability before the host handles work. */
  onStart?(context: CapabilityStartContext<Props>): MaybePromise<void>;
  /**
   * Inspect an HTTP request before the host's request handler.
   *
   * Return a response to handle the request, or `undefined` to continue.
   */
  onRequest?(
    context: CapabilityRequestContext
  ): MaybePromise<Response | undefined | void>;
  /** Run work assigned to the capability when the host's alarm fires. */
  onAlarm?(): MaybePromise<void>;
  /** Handle one message routed to this capability identity. */
  onRoute?(context: LifecycleRouteContext): MaybePromise<unknown>;
  /** Return this capability's next requested physical alarm. */
  getNextAlarm?(): MaybePromise<AlarmContribution>;
  /** Release live or in-memory resources during explicit host destruction. */
  dispose?(): MaybePromise<void>;
}
//#endregion
export {
  LifecycleEvent as a,
  LifecycleEvents as c,
  LifecycleRoutes as d,
  LifecycleServices as f,
  DurableObjectCapability as i,
  LifecycleRouteAddress as l,
  CapabilityRequestContext as n,
  LifecycleAlarms as o,
  CapabilityStartContext as r,
  LifecycleCapability as s,
  AlarmContribution as t,
  LifecycleRouteContext as u
};
//# sourceMappingURL=capability-runner-CvHGZqUu.d.ts.map
