import {
  CallToolRequest,
  CallToolRequestOptions,
  Client
} from "@modelcontextprotocol/client";
import {
  CallToolResult,
  CallToolResultSchema,
  CompatibilityCallToolResultSchema
} from "@modelcontextprotocol/sdk/types.js";
import { Client as Client$1 } from "@modelcontextprotocol/sdk/client/index.js";

//#region src/mcp/client/invoker.d.ts
type LegacyCallToolResultSchema =
  | typeof CallToolResultSchema
  | typeof CompatibilityCallToolResultSchema;
type CompatibleMcpClient = Client | Client$1;
//#endregion
export { LegacyCallToolResultSchema as n, CompatibleMcpClient as t };
//# sourceMappingURL=invoker-CG0_p_Wq.d.ts.map
