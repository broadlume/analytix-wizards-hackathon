import { EventEmitter } from "node:events";
import OpenAI from "openai";
import { AssistantStreamEvent } from "openai/resources/beta/assistants";
import { RequiredActionFunctionToolCall } from "openai/resources/beta/threads/index";
import {inspect} from "util";

export class SQLQueryToolEventHandler extends EventEmitter {
    private client: OpenAI;
    constructor(client) {
        super();
        this.client = client;
    }

    async onEvent(event: AssistantStreamEvent) {
        try {
            console.log(inspect(event,false,null));
            // Retrieve events that are denoted with 'requires_action'
            // since these will have our tool_calls
            if (event.event === "thread.run.requires_action") {
                await this.handleRequiresAction(
                    event,
                );
            }
        } catch (error) {
            console.error("Error handling event:", error);
        }
    }

    async handleRequiresAction(event: AssistantStreamEvent.ThreadRunRequiresAction) {
        const data = event.data;
        try {
            const toolOutputs =
                await Promise.all(data?.required_action?.submit_tool_outputs?.tool_calls?.map(async (toolCall) => {
                    if (toolCall.function.name === "sql_query") {
                        console.log("run sql query");
                        return await this.handleSQLQuery(event, toolCall);
                    }
                }) ?? []);
            // Submit all the tool outputs at the same time
            await this.submitToolOutputs(toolOutputs ?? [], data.id, data.thread_id);
        } catch (error) {
            console.error("Error processing required action:", error);
        }
    }

    async handleSQLQuery(event: AssistantStreamEvent.ThreadRunRequiresAction, toolCall: RequiredActionFunctionToolCall): Promise<OpenAI.Beta.Threads.Runs.RunSubmitToolOutputsParams.ToolOutput> {
        const exampleOutput = {
            isSuccess: true,
            error: null,
            output: "total_pageviews\n512"
        };
        return {
            output: JSON.stringify(exampleOutput),
            tool_call_id: toolCall.id
        };
    }
    async submitToolOutputs(toolOutputs, runId, threadId) {
        try {
            console.log(`submitting: ${toolOutputs}`);
            const stream = await this.client.beta.threads.runs.submitToolOutputsStream(
                threadId,
                runId,
                { tool_outputs: toolOutputs },
            );
            for await (const event of stream) {
                this.emit("event", event);
              }
        } catch (error) {
            console.error("Error submitting tool outputs:", error);
        }
    }
}