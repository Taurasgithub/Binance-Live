import {
  i as DurableObjectCapability,
  l as LifecycleRouteAddress,
  t as AlarmContribution
} from "./capability-runner-CvHGZqUu.js";
import { AsyncLocalStorage } from "node:async_hooks";
import { DurableObject } from "cloudflare:workers";

//#region src/lifecycle/types.d.ts
type ImmutablePrimitive = undefined | null | boolean | string | number;
type Immutable<T> = T extends ImmutablePrimitive
  ? T
  : T extends Array<infer U>
    ? ImmutableArray<U>
    : T extends Map<infer K, infer V>
      ? ImmutableMap<K, V>
      : T extends Set<infer M>
        ? ImmutableSet<M>
        : ImmutableObject<T>;
type ImmutableArray<T> = ReadonlyArray<Immutable<T>>;
type ImmutableMap<K, V> = ReadonlyMap<Immutable<K>, Immutable<V>>;
type ImmutableSet<T> = ReadonlySet<Immutable<T>>;
type ImmutableObject<T> = { readonly [K in keyof T]: Immutable<T[K]> };
/** Immutable state persisted in a hibernating WebSocket attachment. */
type ConnectionState<T> = ImmutableObject<T> | null;
/** Functional update applied to a connection's current state. */
type ConnectionSetStateFn<T> = (prevState: ConnectionState<T>) => T;
/** Context supplied when a lifecycle accepts a WebSocket connection. */
type ConnectionContext = {
  /** Original WebSocket upgrade request. */ request: Request;
};
/** A WebSocket managed by a Durable Object lifecycle. */
type Connection<TState = unknown> = WebSocket & {
  /** Connection identifier */ id: string;
  /**
   * The URL of the original WebSocket upgrade request.
   * Persisted in the WebSocket attachment so it survives hibernation.
   */
  uri: string | null;
  /**
   * Arbitrary state associated with this connection.
   * Read-only — use {@link Connection.setState} to update.
   *
   * This property is configurable, meaning it can be redefined via
   * `Object.defineProperty` by downstream consumers (e.g. the Cloudflare
   * Agents SDK) to namespace or wrap internal state storage.
   */
  state: ConnectionState<TState>;
  /**
   * Update the state associated with this connection.
   *
   * Accepts either a new state value or an updater function that receives
   * the previous state and returns the next state.
   *
   * This property is configurable, meaning it can be redefined via
   * `Object.defineProperty` by downstream consumers that provide their own
   * state projection.
   */
  setState(
    state: TState | ConnectionSetStateFn<TState> | null
  ): ConnectionState<TState>;
  /**
   * Tags returned by the owning Durable Object's `getConnectionTags` callback.
   * Always includes the connection id as the first tag.
   */
  tags: readonly string[];
};
//#endregion
//#region src/lifecycle/durable-object-lifecycle.d.ts
/** Payload delivered to a lifecycle-managed WebSocket callback. */
type WSMessage = ArrayBuffer | ArrayBufferView | string;
/** Internal envelope transported between routed Lifecycle instances. */
type LifecycleRouteEnvelope = {
  readonly capability: string;
  readonly source: LifecycleRouteAddress | undefined;
  readonly payload: unknown;
};
/**
 * Installs and coordinates the runtime lifecycle for a Durable Object.
 *
 * Construct this as an instance field on a class that directly extends
 * `DurableObject`, then call {@link Lifecycle.installHandlers}
 * from that class's constructor.
 *
 * @experimental The API surface may change before stabilizing.
 */
declare class Lifecycle<
  Env extends object = Cloudflare.Env,
  Props extends Record<string, unknown> = Record<string, unknown>
> {
  #private;
  /**
   * Construct and install a lifecycle in one explicit operation.
   *
   * @param host - The Durable Object whose runtime handlers the lifecycle owns.
   * @returns The installed lifecycle.
   */
  static install<
    Env extends object,
    Props extends Record<string, unknown> = Record<string, unknown>
  >(host: DurableObject<Env>): Lifecycle<Env, Props>;
  /**
   * Bind a lifecycle to a Durable Object instance without mutating its handlers.
   *
   * @param host - The Durable Object whose runtime lifecycle this object owns.
   */
  constructor(host: DurableObject<Env>);
  /**
   * Install platform fetch, alarm, and hibernating WebSocket handlers.
   *
   * Existing handlers are preserved for framework-owned dispatch such as the
   * Agent's sub-agent router and alarm circuit breaker. Calling this method
   * more than once is an error.
   */
  installHandlers(): void;
  /**
   * Add a reusable capability before this lifecycle starts.
   *
   * @param capability - The capability to add in dispatch order.
   * @returns This lifecycle.
   */
  use(capability: DurableObjectCapability<Props>): this;
  /** @internal Deliver a generic capability envelope to this Lifecycle. */
  route(envelope: LifecycleRouteEnvelope): Promise<unknown>;
  /**
   * Execute SQL queries against the Durable Object's database
   * @template T Type of the returned rows
   * @param strings SQL query template strings
   * @param values Values to be inserted into the query
   * @returns Array of query results
   */
  sql<T = Record<string, string | number | boolean | null>>(
    strings: TemplateStringsArray,
    ...values: (string | number | boolean | null)[]
  ): T[];
  /**
   * Handle an incoming request for the owning Durable Object.
   */
  fetch(request: Request): Promise<Response>;
  /** @internal Dispatch a hibernating WebSocket message. */
  webSocketMessage(ws: WebSocket, message: WSMessage): Promise<void>;
  /** @internal Dispatch and reciprocate a hibernating WebSocket close. */
  webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean
  ): Promise<void>;
  /** @internal Dispatch a hibernating WebSocket error. */
  webSocketError(ws: WebSocket, error: unknown): Promise<void>;
  /**
   * Start lifecycle capabilities and the owning Durable Object.
   *
   * Runtime fetch, alarm, and WebSocket entry points call this automatically.
   * RPC methods may call it explicitly because native RPC bypasses fetch.
   *
   * @param props - Optional properties supplied to capability and host startup.
   */
  start(props?: Props): Promise<void>;
  /**
   * The name used to address this Durable Object.
   *
   * Native `ctx.id.name` is authoritative. A read-only legacy storage fallback
   * lets objects created by older PartyServer releases migrate without new
   * name writes.
   */
  get name(): string;
  /** Send a message to all connected clients, except connection ids listed in `without` */
  broadcast(
    msg: string | ArrayBuffer | ArrayBufferView,
    without?: string[] | undefined
  ): void;
  /** Get a connection by connection id */
  getConnection<TState = unknown>(id: string): Connection<TState> | undefined;
  /** Get all managed connections, optionally filtered by tag. */
  getConnections<TState = unknown>(tag?: string): Iterable<Connection<TState>>;
  /**
   * Recompute the physical Durable Object alarm from every capability.
   *
   * Concurrent requests are serialized so a later durable-state change cannot
   * be overwritten by an earlier alarm calculation.
   */
  rearmAlarm(): Promise<void>;
  /** Dispose installed capabilities in reverse registration order. */
  dispose(): Promise<void>;
  /** Permanently disable and clear alarms during explicit object teardown. */
  disableAlarms(): Promise<void>;
  /** Dispatch lifecycle and host alarm callbacks after startup. */
  alarm(): Promise<void>;
}
//#endregion
//#region src/lifecycle/current-agent.d.ts
/**
 * A Durable Object that has installed the Agents SDK Lifecycle.
 *
 * Pass a more specific Lifecycle Object type to {@link getCurrentAgent} when
 * shared host code needs APIs implemented by a particular object.
 *
 * @experimental The API surface may change before stabilizing.
 */
interface LifecycleObject<
  Env extends object = Cloudflare.Env,
  Props extends Record<string, unknown> = Record<string, unknown>
> extends DurableObject<Env> {
  readonly lifecycle: Lifecycle<Env, Props>;
  onStart?(props?: Props): void | Promise<void>;
  onRequest?(request: Request): Response | Promise<Response>;
  onAlarm?(): void | Promise<void>;
  getNextAlarm?(): AlarmContribution | Promise<AlarmContribution>;
  onConnect?(
    connection: Connection,
    context: ConnectionContext
  ): void | Promise<void>;
  onMessage?(connection: Connection, message: WSMessage): void | Promise<void>;
  onClose?(
    connection: Connection,
    code: number,
    reason: string,
    wasClean: boolean
  ): void | Promise<void>;
  onError?(connection: Connection, error: unknown): void | Promise<void>;
  getConnectionTags?(
    connection: Connection,
    context: ConnectionContext
  ): string[] | Promise<string[]>;
}
/** Values associated with the currently executing Lifecycle host. */
type AgentContextStore = {
  /** Lifecycle host selected for this invocation. */ agent: unknown /** WebSocket connection selected for this invocation, when applicable. */;
  connection:
    | Connection
    | undefined /** HTTP request selected for this invocation, when applicable. */;
  request:
    | Request
    | undefined /** Extension-owned value selected for this invocation, when applicable. */;
  email: unknown;
};
/** Values returned by {@link getCurrentAgent}. */
type CurrentAgentContext<
  Host extends DurableObject = LifecycleObject,
  Email = unknown
> = {
  agent: Host | undefined;
  connection: Connection | undefined;
  request: Request | undefined;
  email: Email | undefined;
};
/**
 * Shared invocation context for Lifecycle, Agent, AIChatAgent, and Think.
 *
 * @internal Importing or relying on this symbol will break your code in a
 * future release. Use {@link getCurrentAgent} to read the public context.
 */
declare const __DO_NOT_USE_WILL_BREAK__agentContext: AsyncLocalStorage<AgentContextStore>;
/**
 * Return the current Agent or Lifecycle Object and invocation-specific values.
 *
 * Lifecycle host startup and alarm hooks receive the current object with no
 * request. Request hooks additionally receive the request being handled.
 * Lifecycle-managed WebSocket hooks receive their connection and, during
 * connect, its upgrade request. Agent extensions may also establish context
 * for email, chat turns, callable methods, and detached work. Capability hooks
 * do not run in this ambient context.
 */
declare function getCurrentAgent<
  Host extends DurableObject = LifecycleObject
>(): CurrentAgentContext<Host>;
//#endregion
export {
  getCurrentAgent as a,
  WSMessage as c,
  ConnectionSetStateFn as d,
  ConnectionState as f,
  __DO_NOT_USE_WILL_BREAK__agentContext as i,
  Connection as l,
  CurrentAgentContext as n,
  Lifecycle as o,
  LifecycleObject as r,
  LifecycleRouteEnvelope as s,
  AgentContextStore as t,
  ConnectionContext as u
};
//# sourceMappingURL=current-agent-CuMErtly.d.ts.map
