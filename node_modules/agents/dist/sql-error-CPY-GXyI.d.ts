//#region src/sql-error.d.ts
/**
 * Error class for SQL execution failures, containing the query that failed.
 */
declare class SqlError extends Error {
  /** The SQL query that failed */
  readonly query: string;
  constructor(query: string, cause: unknown);
}
//#endregion
export { SqlError as t };
//# sourceMappingURL=sql-error-CPY-GXyI.d.ts.map
