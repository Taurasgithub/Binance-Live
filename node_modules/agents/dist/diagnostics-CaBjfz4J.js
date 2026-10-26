import { channel } from "node:diagnostics_channel";
//#region src/observability/diagnostics.ts
/** Diagnostics channels for Agent and Lifecycle telemetry. */
const channels = {
	state: channel("agents:state"),
	rpc: channel("agents:rpc"),
	message: channel("agents:message"),
	chat: channel("agents:chat"),
	transcript: channel("agents:transcript"),
	fiber: channel("agents:fiber"),
	agentTool: channel("agents:agent_tool"),
	schedule: channel("agents:schedule"),
	lifecycle: channel("agents:lifecycle"),
	workflow: channel("agents:workflow"),
	mcp: channel("agents:mcp"),
	email: channel("agents:email"),
	channel: channel("agents:channel")
};
function getChannel(type) {
	if (type.startsWith("mcp:")) return channels.mcp;
	if (type.startsWith("workflow:")) return channels.workflow;
	if (type.startsWith("fiber:")) return channels.fiber;
	if (type.startsWith("transcript:") || type.startsWith("chat:transcript:")) return channels.transcript;
	if (type.startsWith("chat:")) return channels.chat;
	if (type.startsWith("agent_tool:")) return channels.agentTool;
	if (type.startsWith("schedule:") || type.startsWith("queue:")) return channels.schedule;
	if (type.startsWith("message:") || type.startsWith("tool:") || type.startsWith("submission:") || type.startsWith("action:")) return channels.message;
	if (type === "rpc" || type.startsWith("rpc:")) return channels.rpc;
	if (type.startsWith("state:")) return channels.state;
	if (type.startsWith("email:")) return channels.email;
	if (type.startsWith("channel:") || type.startsWith("notice:")) return channels.channel;
	return channels.lifecycle;
}
/** Publish one event to its existing diagnostics channel. */
function publishDiagnosticsEvent(event) {
	getChannel(event.type).publish(event);
}
//#endregion
export { publishDiagnosticsEvent as n, channels as t };

//# sourceMappingURL=diagnostics-CaBjfz4J.js.map