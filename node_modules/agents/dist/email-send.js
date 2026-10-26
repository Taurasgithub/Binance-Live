import { signAgentHeaders } from "./email.js";
//#region src/email-send.ts
/** Send with the routing and signing behavior used by `Agent.sendEmail()`. */
async function sendAgentEmail(options, identity) {
	if (!options.binding) throw new Error("binding is required. Pass your send_email binding, e.g. this.sendEmail({ binding: this.env.EMAIL, ... }).");
	const headers = {
		...options.headers,
		"X-Agent-Name": identity.agentName,
		"X-Agent-ID": identity.agentId
	};
	if (options.inReplyTo) headers["In-Reply-To"] = options.inReplyTo;
	if (typeof options.secret === "string") {
		const signedHeaders = await signAgentHeaders(options.secret, identity.agentName, identity.agentId);
		headers["X-Agent-Sig"] = signedHeaders["X-Agent-Sig"];
		headers["X-Agent-Sig-Ts"] = signedHeaders["X-Agent-Sig-Ts"];
	}
	return options.binding.send({
		from: options.from,
		to: options.to,
		subject: options.subject,
		text: options.text,
		html: options.html,
		replyTo: options.replyTo,
		cc: options.cc,
		bcc: options.bcc,
		headers
	});
}
//#endregion
export { sendAgentEmail };

//# sourceMappingURL=email-send.js.map