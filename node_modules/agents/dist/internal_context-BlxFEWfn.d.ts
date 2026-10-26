//#region src/internal_context.d.ts
type AgentEmail = {
  from: string;
  to: string;
  getRaw: () => Promise<Uint8Array>;
  headers: Headers;
  rawSize: number;
  setReject: (reason: string) => void;
  forward: (rcptTo: string, headers?: Headers) => Promise<EmailSendResult>;
  reply: (options: {
    from: string;
    to: string;
    raw: string;
  }) => Promise<EmailSendResult> /** @internal Indicates email was routed via createSecureReplyEmailResolver */;
  _secureRouted?: boolean;
};
//#endregion
export { AgentEmail as t };
//# sourceMappingURL=internal_context-BlxFEWfn.d.ts.map
