import { AsyncLocalStorage } from "node:async_hooks";
//#region src/lifecycle/current-agent.ts
/**
* Shared invocation context for Lifecycle, Agent, AIChatAgent, and Think.
*
* @internal Importing or relying on this symbol will break your code in a
* future release. Use {@link getCurrentAgent} to read the public context.
*/
const __DO_NOT_USE_WILL_BREAK__agentContext = new AsyncLocalStorage();
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
function getCurrentAgent() {
	const store = __DO_NOT_USE_WILL_BREAK__agentContext.getStore();
	if (!store) return {
		agent: void 0,
		connection: void 0,
		request: void 0,
		email: void 0
	};
	return {
		agent: store.agent,
		connection: store.connection,
		request: store.request,
		email: store.email
	};
}
/** Run one host hook in the context of its Lifecycle Object. */
function runInLifecycleHostContext(context, operation) {
	return __DO_NOT_USE_WILL_BREAK__agentContext.run({
		agent: context.host,
		connection: context.connection,
		request: context.request,
		email: void 0
	}, operation);
}
/** Run a capability hook without inheriting a current Agent or host context. */
function runWithoutCurrentAgent(operation) {
	return __DO_NOT_USE_WILL_BREAK__agentContext.exit(operation);
}
//#endregion
export { runWithoutCurrentAgent as i, getCurrentAgent as n, runInLifecycleHostContext as r, __DO_NOT_USE_WILL_BREAK__agentContext as t };

//# sourceMappingURL=current-agent-DhoDkSnH.js.map