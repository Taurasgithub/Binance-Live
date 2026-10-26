import { s as SendEmailOptions } from "./email-7TatiTnl.js";

//#region src/email-send.d.ts
type AgentEmailIdentity = {
  agentName: string;
  agentId: string;
};
/** Send with the routing and signing behavior used by `Agent.sendEmail()`. */
declare function sendAgentEmail(
  options: SendEmailOptions,
  identity: AgentEmailIdentity
): Promise<EmailSendResult>;
//#endregion
export { sendAgentEmail };
//# sourceMappingURL=email-send.d.ts.map
