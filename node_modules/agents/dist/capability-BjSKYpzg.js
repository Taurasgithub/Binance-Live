//#region src/lifecycle/capability.ts
const installedServices = /* @__PURE__ */ new WeakMap();
/**
* Base class for capabilities that consume standard Lifecycle services.
*
* @experimental The API surface may change before stabilizing.
*/
var LifecycleCapability = class {
	constructor(capabilityId) {
		if (capabilityId.trim() === "") throw new Error("Lifecycle capability IDs must be non-empty");
		this.capabilityId = capabilityId;
	}
	/** Default startup hook; capabilities override when they own startup work. */
	onStart(_context) {}
	/** Standard services when installed, or undefined in isolated unit tests. */
	get lifecycleServices() {
		return installedServices.get(this);
	}
	/** Standard services supplied when Lifecycle installs this capability. */
	get lifecycle() {
		const services = this.lifecycleServices;
		if (!services) throw new Error(`${this.constructor.name} must be installed with Lifecycle.use() before use`);
		return services;
	}
};
/**
* @internal Bind the standard service surface to one capability instance.
* `Lifecycle.use()` calls this during installation. Test a capability by
* installing it on a minimal Durable Object with a real Lifecycle rather
* than binding fake services.
*/
function bindLifecycleCapability(capability, services) {
	installedServices.set(capability, services);
}
/** @internal Read a capability ID without exposing installation internals. */
function lifecycleCapabilityId(capability) {
	return capability instanceof LifecycleCapability ? capability.capabilityId : void 0;
}
//#endregion
export { bindLifecycleCapability as n, lifecycleCapabilityId as r, LifecycleCapability as t };

//# sourceMappingURL=capability-BjSKYpzg.js.map