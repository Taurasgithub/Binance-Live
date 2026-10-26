import { z } from "zod";

//#region src/schedules/parser.d.ts
/**
 * Get the schedule prompt for a given event
 * @param event - The event to get the schedule prompt for
 * @returns The schedule prompt
 */
declare function getSchedulePrompt(event: { date: Date }): string;
/**
 * The schema for parsing natural language scheduling requests.
 *
 * @example
 * ```typescript
 * import { generateObject } from "ai";
 * import { scheduleSchema, getSchedulePrompt } from "agents/schedules/parser";
 *
 * const result = await generateObject({
 *   model,
 *   prompt: `${getSchedulePrompt({ date: new Date() })} Input: "${userInput}"`,
 *   schema: scheduleSchema,
 *   // Required for OpenAI to avoid strict JSON schema validation errors
 *   providerOptions: {
 *     openai: { strictJsonSchema: false }
 *   }
 * });
 * ```
 *
 * @remarks
 * When using this schema with OpenAI models via the AI SDK, you must pass
 * `providerOptions: { openai: { strictJsonSchema: false } }` to `generateObject`.
 * This is because the schema uses a discriminated union which is not compatible
 * with OpenAI's strict structured outputs mode.
 */
declare const scheduleSchema: z.ZodObject<
  {
    description: z.ZodString;
    when: z.ZodDiscriminatedUnion<
      [
        z.ZodObject<
          {
            type: z.ZodLiteral<"scheduled">;
            date: z.ZodString;
          },
          z.core.$strip
        >,
        z.ZodObject<
          {
            type: z.ZodLiteral<"delayed">;
            delayInSeconds: z.ZodNumber;
          },
          z.core.$strip
        >,
        z.ZodObject<
          {
            type: z.ZodLiteral<"cron">;
            cron: z.ZodString;
          },
          z.core.$strip
        >,
        z.ZodObject<
          {
            type: z.ZodLiteral<"no-schedule">;
          },
          z.core.$strip
        >
      ],
      "type"
    >;
  },
  z.core.$strip
>;
/**
 * The type for the schedule prompt
 */
type ParsedSchedule = z.infer<typeof scheduleSchema>;
//#endregion
export { ParsedSchedule, getSchedulePrompt, scheduleSchema };
//# sourceMappingURL=parser.d.ts.map
