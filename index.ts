import OpenAI from "openai";
import { Parser } from "node-sql-parser";
import { SQLQueryToolEventHandler } from "./sql_query_tool";

const openAI = new OpenAI({
	apiKey: "sk-ai-hackathon-gsJtmxRHihAEbkD3oM72T3BlbkFJsLD8OlkhwC899UJD2aIy"
});

const currentDate = new Date();
const schema = [
	{
		"schema_name": "ga4_floorforce",
		"table_name": "web_trends_mv",
		"description": "This is view is a summary of all metrics of interest and aggregated by retailer and calculated daily.",
		"fields": {
			"uuid": "Universal Retailer ID",
			"date": "Day the metric was counted",
			"sessions": "Sum of sessions",
			"users": "Sum of users",
			"pageviews": "Sum of pageviews",
			"avg_session_duration": "Average length of time for all sessions on the page",
			"bounce_rate": "Percentage of people that stay on the page and don’t submit data before leaving",
			"leads": "Lead count",
			"conversion_rate": "Goals (calls, chats, forms) divided by the number of sessions"
		}
	},
	{
		"schema_name": "ga4_floorforce",
		"table_name": "conversion_rates_raw",
		"description": "This view calculates daily total conversion rate and conversion rate by medium (chat, call, form).",
		"fields": {
			"uuid": "Universal Retailer ID",
			"date": "Date conversion was made",
			"call": "Conversion rate for call",
			"chat": "Conversion rate for chat",
			"form": "Conversion rate for form",
			"total": "Combined conversion rate for call, chat, and form"
		}
	},
	{
		"schema_name": "ga4_floorforce",
		"table_name": "top_pages",
		"description": "This view counts pageviews by page.",
		"fields": {
			"uuid": "Universal Retailer ID",
			"date": "Date that page was viewed",
			"page": "Page location that appears after the domain",
			"page_path": "Path of the page",
			"page_views": "Sum of pageviews for that page"
		}
	},
	{
		"schema_name": "ga4_floorforce",
		"table_name": "top_channels",
		"description": "This view counts sessions per channel group.",
		"fields": {
			"uuid": "Universal Retailer ID",
			"date": "Date session occurred",
			"channel group": "Medium for which the user found the site",
			"sessions": "Sum of sessions"
		}
	},
	{
		"schema_name": "ga4_floorforce",
		"table_name": "web_trends_daily",
		"description": "This view counts sessions, users, and pageviews daily.",
		"fields": {
			"uuid": "Universal Retailer ID",
			"date": "Date that page was viewed",
			"users": "Sum of users",
			"sessions": "Sum of sessions",
			"pageviews": "Sum of pageviews for that page"
		}
	}
];
const assistantInstructions = `You are an assistant meant to help users gain insights into their data by writing SQL queries for them that fufill their question.
Please inform the user if their query does not make sense before writing code.
Please write one valid SQL statement to fetch data to answer the user's question. Prefix all tables with their schema names. 
MAKE SURE EVERY QUERY IS FOR THE USER'S UUID. DO NOT FORGET TO INCLUDE THE UUID IN THE SQL QUERY. 
If possible respond only with SQL code. Be succinct.`;


const systemPrompt = `
The current date is: ${currentDate}
This user's UUID is: 81d8595a-0e85-4afd-a399-204958879c84
The following is a JSON document containing schema information about tables in AWS Redshift:
${JSON.stringify(schema)}
Please use this information to respond to the user's query.`;

const assistant_id = "asst_WApI6ECfRbKA3fQCoRo14H3E";
const assistant = await openAI.beta.assistants.update(assistant_id, {
	model: "gpt-4o",
	instructions: assistantInstructions,
	name: "AI Hackathon Analytic Reporter Assistant",
	tools: [
		{
			type: "function",
			function: {
				name: "sql_query",
				description: "Run an SQL query and return the results as JSON.",
				parameters: {
					type: "object",
					properties: {
						schema_name: {
							type: "string",
							description: "The schema name that contains the table"
						},
						table_name: {
							type: "string",
							description: "The table to run the SQL query on"
						},
						sql: {
							type: "string",
							description: "The raw SQL to run"
						},
					},
					required: ["schema_name", "table_name", "sql"]
				}
			}
		}
	]
});
// console.log(inspect(assistant,false,null));

const thread = await openAI.beta.threads.create({
	messages: [
		{
			role: "user",
			content: "How many page views did I see the week of June 10th?",
		}]
});

const run = await openAI.beta.threads.runs.createAndPoll(thread.id, {
	assistant_id: assistant_id,
});

const eventHandler = new SQLQueryToolEventHandler(openAI);
eventHandler.on("event", eventHandler.onEvent.bind(eventHandler));

const stream = await openAI.beta.threads.runs.stream(
	thread.id,
	{ assistant_id: assistant_id, additional_instructions: systemPrompt, tool_choice: "required" },
);


for await (const event of stream) {
	eventHandler.emit("event", event);
}



