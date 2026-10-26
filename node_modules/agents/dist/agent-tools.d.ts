import {
  $ as AgentToolRunPart,
  B as AgentToolChildAdapter,
  G as AgentToolFailure,
  H as AgentToolEvent,
  K as AgentToolInterruptedReason,
  Q as AgentToolRunInspection,
  U as AgentToolEventMessage,
  V as AgentToolDisplayMetadata,
  W as AgentToolEventState,
  Z as AgentToolRunInfo,
  at as DetachedAgentToolConfig,
  ct as RunAgentToolResult,
  et as AgentToolRunState,
  it as ChatCapableAgentClass,
  nt as AgentToolStoredChunk,
  ot as DetachedRunAgentToolResult,
  q as AgentToolLifecycleResult,
  rt as AgentToolTerminalStatus,
  st as RunAgentToolOptions,
  tt as AgentToolRunStatus
} from "./agent-routing-CnkaHb-v.js";
import { FlexibleSchema, InferSchema, Tool } from "ai";

//#region src/agent-tools.d.ts
type ParseSchema<Output = unknown> = {
  parse(value: unknown): Output;
};
type AgentToolOutputSchema<Output> =
  | FlexibleSchema<Output>
  | ParseSchema<Output>;
type AgentToolFactoryOptions<Output = unknown, Input = unknown> = {
  description: string;
  inputSchema: FlexibleSchema<Input>;
  outputSchema?: AgentToolOutputSchema<Output>;
  displayName?: string;
  icon?: string;
  display?: AgentToolDisplayMetadata;
};
type InferredAgentToolFactoryOptions<
  InputSchema extends FlexibleSchema,
  Output
> = Omit<AgentToolFactoryOptions<Output>, "inputSchema"> & {
  inputSchema: InputSchema;
};
/**
 * Create an AI SDK tool that dispatches a chat-capable sub-agent through
 * `Agent.runAgentTool`.
 */
declare function agentTool<
  InputSchema extends FlexibleSchema,
  Output = unknown
>(
  cls: ChatCapableAgentClass,
  options: InferredAgentToolFactoryOptions<InputSchema, Output>
): Tool<InferSchema<InputSchema>, string | Output | AgentToolFailure>;
declare function agentTool<Input = unknown, Output = unknown>(
  cls: ChatCapableAgentClass,
  options: AgentToolFactoryOptions<Output, Input>
): Tool<Input, string | Output | AgentToolFailure>;
//#endregion
export {
  type AgentToolChildAdapter,
  type AgentToolDisplayMetadata,
  type AgentToolEvent,
  type AgentToolEventMessage,
  type AgentToolEventState,
  type AgentToolFactoryOptions,
  type AgentToolFailure,
  type AgentToolInterruptedReason,
  type AgentToolLifecycleResult,
  type AgentToolRunInfo,
  type AgentToolRunInspection,
  type AgentToolRunPart,
  type AgentToolRunState,
  type AgentToolRunStatus,
  type AgentToolStoredChunk,
  type AgentToolTerminalStatus,
  type ChatCapableAgentClass,
  type DetachedAgentToolConfig,
  type DetachedRunAgentToolResult,
  type RunAgentToolOptions,
  type RunAgentToolResult,
  agentTool
};
//# sourceMappingURL=agent-tools.d.ts.map
