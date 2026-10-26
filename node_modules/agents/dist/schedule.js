import { getSchedulePrompt, scheduleSchema } from "./schedules/parser.js";
//#region src/schedule.ts
let didWarnAboutUnstableGetSchedulePrompt = false;
/**
* @deprecated this has been renamed to getSchedulePrompt, and unstable_getSchedulePrompt will be removed in the next major version
* @param event - The event to get the schedule prompt for
* @returns The schedule prompt
*/
function unstable_getSchedulePrompt(event) {
	if (!didWarnAboutUnstableGetSchedulePrompt) {
		didWarnAboutUnstableGetSchedulePrompt = true;
		console.warn("unstable_getSchedulePrompt is deprecated, use getSchedulePrompt instead. unstable_getSchedulePrompt will be removed in the next major version.");
	}
	return getSchedulePrompt(event);
}
/**
* @deprecated this has been renamed to scheduleSchema, and unstable_scheduleSchema will be removed in the next major version
* @returns The schedule schema
*/
const unstable_scheduleSchema = scheduleSchema;
//#endregion
export { getSchedulePrompt, scheduleSchema, unstable_getSchedulePrompt, unstable_scheduleSchema };

//# sourceMappingURL=schedule.js.map