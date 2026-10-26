import { camelCaseToKebabCase } from "./utils.js";
//#region src/agent-routing.ts
const namespaceMapCache = /* @__PURE__ */ new WeakMap();
const bindingNameCache = /* @__PURE__ */ new WeakMap();
const DEFAULT_ROUTING_RETRY_OPTIONS = {
	maxAttempts: 3,
	baseDelayMs: 100,
	maxDelayMs: 800
};
function durableObjectGetOptions(options) {
	return options?.locationHint ? { locationHint: options.locationHint } : void 0;
}
function validatePositiveInteger(value, name) {
	if (!Number.isFinite(value) || value < 1) throw new Error(`${name} must be >= 1`);
	if (!Number.isInteger(value)) throw new Error(`${name} must be an integer`);
}
function validatePositiveNumber(value, name) {
	if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be > 0`);
}
function resolveRoutingRetryOptions(options) {
	if (options === false) return null;
	const resolved = {
		maxAttempts: options?.maxAttempts ?? DEFAULT_ROUTING_RETRY_OPTIONS.maxAttempts,
		baseDelayMs: options?.baseDelayMs ?? DEFAULT_ROUTING_RETRY_OPTIONS.baseDelayMs,
		maxDelayMs: options?.maxDelayMs ?? DEFAULT_ROUTING_RETRY_OPTIONS.maxDelayMs,
		onRetry: options?.onRetry
	};
	validatePositiveInteger(resolved.maxAttempts, "routingRetry.maxAttempts");
	validatePositiveNumber(resolved.baseDelayMs, "routingRetry.baseDelayMs");
	validatePositiveNumber(resolved.maxDelayMs, "routingRetry.maxDelayMs");
	if (resolved.baseDelayMs > resolved.maxDelayMs) throw new Error("routingRetry.baseDelayMs must be <= maxDelayMs");
	return resolved;
}
function isRetryableDurableObjectError(error) {
	if (typeof error !== "object" || error === null) return false;
	const typed = error;
	return typed.retryable === true && typed.overloaded !== true;
}
function routingRetryDelayMs(attempt, options) {
	const upperBoundMs = Math.min(options.maxDelayMs, options.baseDelayMs * 2 ** (attempt - 1));
	return Math.floor(Math.random() * upperBoundMs);
}
async function retryDurableObjectOperation(operation, context, retryOptions) {
	const resolved = resolveRoutingRetryOptions(retryOptions);
	if (!resolved) return await operation();
	let attempt = 1;
	while (true) try {
		return await operation();
	} catch (error) {
		const nextAttempt = attempt + 1;
		if (nextAttempt > resolved.maxAttempts || !isRetryableDurableObjectError(error)) throw error;
		const delayMs = routingRetryDelayMs(attempt, resolved);
		try {
			await resolved.onRetry?.({
				error,
				attempt,
				maxAttempts: resolved.maxAttempts,
				delayMs,
				name: context.name,
				className: context.className
			});
		} catch (callbackError) {
			console.warn("Durable Object routing retry callback failed:", callbackError);
		}
		await new Promise((resolve) => setTimeout(resolve, delayMs));
		attempt = nextAttempt;
	}
}
function encodeLifecycleProps(props) {
	const bytes = new TextEncoder().encode(JSON.stringify(props));
	let binary = "";
	for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
	return btoa(binary);
}
function mutableRequest(request) {
	return new Request(request);
}
function cloneRequestForFetch(request) {
	return request.clone();
}
function resolveCorsHeaders(cors) {
	if (cors === true) return {
		"Access-Control-Allow-Origin": "*",
		"Access-Control-Allow-Methods": "GET, POST, HEAD, OPTIONS",
		"Access-Control-Allow-Headers": "*",
		"Access-Control-Max-Age": "86400"
	};
	if (cors && typeof cors === "object") {
		const headers = new Headers(cors);
		const record = {};
		headers.forEach((value, key) => {
			record[key] = value;
		});
		return record;
	}
	return null;
}
/**
* Route `/agents/:binding/:name` HTTP and WebSocket requests to a named
* Durable Object. The target may extend `Agent` or compose `Lifecycle`
* directly into a plain `DurableObject`.
*
* @param request - Incoming Worker request.
* @param env - Worker environment containing Durable Object bindings.
* @param options - Routing options.
* @returns The matched response, or `null` when the path does not match.
*/
async function routeAgentRequest(request, env, options) {
	const environment = env;
	if (!namespaceMapCache.has(environment)) {
		const namespaceMap = {};
		const bindingNames = {};
		for (const [key, value] of Object.entries(environment)) if (value && typeof value === "object" && "idFromName" in value && typeof value.idFromName === "function") {
			const kebab = camelCaseToKebabCase(key);
			namespaceMap[kebab] = value;
			bindingNames[kebab] = key;
		}
		namespaceMapCache.set(environment, namespaceMap);
		bindingNameCache.set(environment, bindingNames);
	}
	const map = namespaceMapCache.get(environment);
	const bindingNames = bindingNameCache.get(environment);
	const prefixParts = (options?.prefix || "agents").split("/");
	const parts = new URL(request.url).pathname.split("/").filter(Boolean);
	if (!prefixParts.every((part, index) => parts[index] === part) || parts.length < prefixParts.length + 2) return null;
	const namespaceName = parts[prefixParts.length];
	const name = parts[prefixParts.length + 1];
	if (!name || !namespaceName) return null;
	const boundNamespace = map[namespaceName];
	if (!boundNamespace) {
		console.error(`The URL ${request.url} with namespace "${namespaceName}" and name "${name}" does not match any Durable Object binding.`);
		return new Response("Invalid request", { status: 400 });
	}
	const corsHeaders = resolveCorsHeaders(options?.cors);
	const isWebSocket = request.headers.get("Upgrade")?.toLowerCase() === "websocket";
	const withCorsHeaders = (response) => {
		if (!corsHeaders || isWebSocket) return response;
		const nextResponse = new Response(response.body, response);
		for (const [key, value] of Object.entries(corsHeaders)) nextResponse.headers.set(key, value);
		return nextResponse;
	};
	if (request.method === "OPTIONS" && corsHeaders) return new Response(null, { headers: corsHeaders });
	const namespace = options?.jurisdiction ? boundNamespace.jurisdiction(options.jurisdiction) : boundNamespace;
	const id = namespace.idFromName(name);
	const getOptions = durableObjectGetOptions(options);
	request = mutableRequest(request);
	const className = bindingNames[namespaceName];
	const route = {
		className,
		name
	};
	if (isWebSocket) {
		const requestOrResponse = await options?.onBeforeConnect?.(request, route);
		if (requestOrResponse instanceof Request) request = mutableRequest(requestOrResponse);
		else if (requestOrResponse instanceof Response) return requestOrResponse;
	} else {
		const requestOrResponse = await options?.onBeforeRequest?.(request, route);
		if (requestOrResponse instanceof Request) request = mutableRequest(requestOrResponse);
		else if (requestOrResponse instanceof Response) return withCorsHeaders(requestOrResponse);
	}
	if (options?.props !== void 0) request.headers.set("x-agents-lifecycle-props", encodeLifecycleProps(options.props));
	const response = await retryDurableObjectOperation(() => namespace.get(id, getOptions).fetch(cloneRequestForFetch(request)), {
		name,
		className
	}, options?.routingRetry);
	return isWebSocket ? response : withCorsHeaders(response);
}
/**
* Get a named Agent stub after its lifecycle startup has completed.
*
* @param namespace - Agent Durable Object namespace.
* @param name - Agent instance name.
* @param options - Placement, startup properties, and retry options.
* @returns The initialized Agent stub.
*/
async function getAgentByName(namespace, name, options) {
	const target = options?.jurisdiction ? namespace.jurisdiction(options.jurisdiction) : namespace;
	const id = target.idFromName(name);
	const stub = target.get(id, durableObjectGetOptions(options));
	const lifecycleStub = stub;
	await retryDurableObjectOperation(() => lifecycleStub.__unsafe_ensureInitialized(options?.props), { name }, options?.routingRetry);
	return stub;
}
//#endregion
export { getAgentByName, routeAgentRequest };

//# sourceMappingURL=agent-routing.js.map