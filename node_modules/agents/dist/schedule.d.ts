import {
  ParsedSchedule,
  getSchedulePrompt,
  scheduleSchema
} from "./schedules/parser.js";

//#region src/schedule.d.ts
/**
 * @deprecated this has been renamed to getSchedulePrompt, and unstable_getSchedulePrompt will be removed in the next major version
 * @param event - The event to get the schedule prompt for
 * @returns The schedule prompt
 */
declare function unstable_getSchedulePrompt(event: { date: Date }): string;
/**
 * @deprecated this has been renamed to scheduleSchema, and unstable_scheduleSchema will be removed in the next major version
 * @returns The schedule schema
 */
declare const unstable_scheduleSchema: import("zod").ZodObject<
  {
    description: import("zod").ZodString;
    when: import("zod").ZodDiscriminatedUnion<
      [
        import("zod").ZodObject<
          {
            type: import("zod").ZodLiteral<"scheduled">;
            date: import("zod").ZodString;
          },
          import("zod/v4/core").$strip
        >,
        import("zod").ZodObject<
          {
            type: import("zod").ZodLiteral<"delayed">;
            delayInSeconds: import("zod").ZodNumber;
          },
          import("zod/v4/core").$strip
        >,
        import("zod").ZodObject<
          {
            type: import("zod").ZodLiteral<"cron">;
            cron: import("zod").ZodString;
          },
          import("zod/v4/core").$strip
        >,
        import("zod").ZodObject<
          {
            type: import("zod").ZodLiteral<"no-schedule">;
          },
          import("zod/v4/core").$strip
        >
      ],
      "type"
    >;
  },
  import("zod/v4/core").$strip
>;
//#endregion
export {
  type ParsedSchedule as Schedule,
  getSchedulePrompt,
  scheduleSchema,
  unstable_getSchedulePrompt,
  unstable_scheduleSchema
};
//# sourceMappingURL=schedule.d.ts.map
