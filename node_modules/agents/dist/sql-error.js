//#region src/sql-error.ts
/**
* Error class for SQL execution failures, containing the query that failed.
*/
var SqlError = class extends Error {
	constructor(query, cause) {
		const message = cause instanceof Error ? cause.message : String(cause);
		super(`SQL query failed: ${message}`, { cause });
		this.name = "SqlError";
		this.query = query;
	}
};
//#endregion
export { SqlError };

//# sourceMappingURL=sql-error.js.map