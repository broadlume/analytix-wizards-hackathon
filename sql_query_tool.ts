import { EventEmitter } from "node:events";
import OpenAI from "openai";
import { AssistantStreamEvent } from "openai/resources/beta/assistants";
import { RequiredActionFunctionToolCall } from "openai/resources/beta/threads/index";
import {inspect} from "util";
import { AST, Parser } from "node-sql-parser";
import { TABLE_SCHEMA } from ".";

export class SQLQueryToolEventHandler extends EventEmitter {
    private client: OpenAI;
    constructor(client) {
        super();
        this.client = client;
    }

    async onEvent(event: AssistantStreamEvent) {
        try {
            if (!event.event.includes("delta")) {
                console.log(inspect(event,false,null));
            }
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
        let output;
        try {
            const jsonArgs = JSON.parse(toolCall.function.arguments);
            const sqlQueryTool = new SQLQueryTool(jsonArgs);
            output = {
                success: true,
                error: null,
                output: "page_views\n512",
            }
        } catch (e) {
            output = {
                success: false,
                error: `${e}`,
                output: null
            }
        }
        console.log(inspect(output,false,null));
        return {
            output: JSON.stringify(output),
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

type SQLToolArgs = {
    schema_name: string;
    table_name: string;
    sql: string;
}
class SQLQueryTool {
    private args: SQLToolArgs;
    private sqlParser: Parser;
    public constructor(args: object) {
        this.sqlParser = new Parser();
        if (this.parseSQL(args)) {
            this.args = args;
        }

    }
    parseSQL(args: object): args is SQLToolArgs {
        for (const field of ["schema_name","table_name","sql"] as const) {
            if (!(field in args)) {
                throw new Error(`Field '${field} is missing or undefined`);
            }
        }
        const schemaNames = TABLE_SCHEMA.map(t => t.schema_name);
        const tableNames = TABLE_SCHEMA.map(t => t.table_name);
        const columnNames = TABLE_SCHEMA.flatMap(t => Object.keys(t.fields));
        const tableAuthority = `select::(${schemaNames.join("|")})::(${tableNames.join("|")})`;
        const columnAuthority = columnNames.map(name => `select::null::${name}`);
        const tableCheck = this.sqlParser.whiteListCheck((args as any).sql,[tableAuthority], {
            database: "Redshift",
            type: "table"
        });
        const columnCheck = this.sqlParser.whiteListCheck((args as any).sql,columnAuthority, {
            database: "Redshift",
            type: "column"
        });
        if (tableCheck != undefined) {
            throw new Error(`${tableCheck.name}: ${tableCheck.message}`);
        }
        if (columnCheck != undefined) {
            throw new Error(`${columnCheck.name}: ${columnCheck.message}`);
        }
        return true;
    }
}