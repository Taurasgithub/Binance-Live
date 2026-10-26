import { n as publishDiagnosticsEvent, t as channels } from "../diagnostics-CaBjfz4J.js";
import { subscribe as subscribe$1, unsubscribe } from "node:diagnostics_channel";
//#region src/observability/index.ts
/**
* Channel keys whose diagnostics channel name differs from `agents:${key}`.
* Keep this in sync with {@link channels} for any camelCase key that maps to a
* snake_case diagnostics channel.
*/
const CHANNEL_DIAGNOSTIC_NAME_OVERRIDES = { agentTool: "agents:agent_tool" };
/**
* The default observability implementation.
*
* Publishes events to diagnostics_channel. Events are silent unless
* a subscriber is registered or a Tail Worker is attached.
*/
const genericObservability = { emit(event) {
	publishDiagnosticsEvent(event);
} };
/**
* Subscribe to a typed observability channel.
*
* ```ts
* import { subscribe } from "agents/observability";
*
* const unsub = subscribe("rpc", (event) => {
*   console.log(event.payload.method); // fully typed
* });
* ```
*
* @returns A function that unsubscribes the callback.
*/
function subscribe(channelKey, callback) {
	const name = CHANNEL_DIAGNOSTIC_NAME_OVERRIDES[channelKey] ?? `agents:${channelKey}`;
	const handler = (message, _name) => callback(message);
	subscribe$1(name, handler);
	return () => unsubscribe(name, handler);
}
//#endregion
export { channels, genericObservability, subscribe };

//# sourceMappingURL=index.js.map