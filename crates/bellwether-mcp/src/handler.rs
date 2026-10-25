//! The rmcp handler: a thin async wrapper over [`Server`]. Each tool call
//! runs the synchronous core on a blocking thread; nothing here interprets
//! a result.

use std::future::Future;
use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, GetPromptRequestParams,
    GetPromptResponse, GetPromptResult, Implementation, ListPromptsResult, ListResourcesResult,
    ListToolsResult, PaginatedRequestParams, Prompt, PromptArgument, PromptMessage,
    ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
    ResourceContents, Role, ServerCapabilities, ServerConfig, Tool, ToolAnnotations,
};
use rmcp::service::{MaybeSendFuture, RequestContext};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::{Map, Value};

use crate::server::{Reply, Server};
use crate::tools::{self, Tier};
use crate::{prompt, FINDINGS, RECEIPTS, SCORECARD};

/// The MCP handler over one [`Server`].
#[derive(Clone, Debug)]
pub struct Handler {
    server: Server,
}

impl Handler {
    /// Wraps `server`.
    #[must_use]
    pub fn new(server: Server) -> Self {
        Self { server }
    }
}

/// An MCP tool result from a reply: the facade's result as the text
/// content, the envelope as the structured content.
#[must_use]
pub fn to_result(reply: Reply) -> CallToolResult {
    let is_error = reply.is_error();
    let mut r = if is_error {
        CallToolResult::error(vec![ContentBlock::text(reply.text)])
    } else {
        CallToolResult::success(vec![ContentBlock::text(reply.text)])
    };
    r.structured_content = serde_json::to_value(&reply.envelope).ok();
    r
}

/// The tools as MCP lists them.
#[must_use]
pub fn tool_list() -> Vec<Tool> {
    let output = match tools::output_schema() {
        Value::Object(m) => Arc::new(m),
        _ => Arc::new(Map::new()),
    };
    tools::catalog()
        .into_iter()
        .map(|t| {
            let input = match t.input {
                Value::Object(m) => m,
                _ => Map::new(),
            };
            let mut annotations = ToolAnnotations::new();
            annotations.read_only_hint = Some(t.read_only);
            annotations.destructive_hint = Some(false);
            annotations.open_world_hint = Some(false);
            annotations.idempotent_hint = Some(matches!(t.tier, Tier::Observe));
            Tool::new(t.name, t.description, Arc::new(input))
                .with_raw_output_schema(Arc::clone(&output))
                .with_annotations(annotations)
        })
        .collect()
}

/// The resources as MCP lists them.
#[must_use]
pub fn resource_list() -> Vec<Resource> {
    [
        (
            FINDINGS,
            "findings",
            "The newest profile Evidence per root, with its lints and the receipts citing it.",
        ),
        (
            RECEIPTS,
            "receipts",
            "Every receipt, newest first, with its explain chain status.",
        ),
        (
            SCORECARD,
            "scorecard",
            "Per rule id: proposals, certificates passed and failed, receipts.",
        ),
    ]
    .into_iter()
    .map(|(uri, name, description)| {
        Resource::new(uri, name)
            .with_description(description)
            .with_mime_type("application/json")
    })
    .collect()
}

/// The one prompt as MCP lists it.
#[must_use]
pub fn prompt_def() -> Prompt {
    Prompt::new(
        prompt::NAME,
        Some(
            "Walk the loop: observe, read the evidence, propose, have the engine score and \
             verify, apply, quote the receipt.",
        ),
        Some(vec![PromptArgument::new(prompt::ARGUMENT)
            .with_description("What is slow, in the human's words (optional).")
            .with_required(false)]),
    )
    .with_title(prompt::TITLE)
}

/// The prompt's messages: `AGENTS.md`, then the symptom when given.
pub fn get_prompt(request: &GetPromptRequestParams) -> Result<GetPromptResponse, ErrorData> {
    if request.name != prompt::NAME {
        return Err(ErrorData::invalid_params(
            format!(
                "no prompt is named {:?}; the one prompt is {}",
                request.name,
                prompt::NAME
            ),
            None,
        ));
    }
    let symptom = request
        .arguments
        .as_ref()
        .and_then(|a| a.get(prompt::ARGUMENT))
        .and_then(Value::as_str);
    let messages = prompt::messages(symptom)
        .into_iter()
        .map(|m| PromptMessage::new_text(Role::User, m))
        .collect();
    let mut result = GetPromptResult::new(messages);
    result.description = Some(prompt::TITLE.to_string());
    Ok(result.into())
}

impl ServerHandler for Handler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::new(
            "bellwether-mcp",
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(
            "Bellwether over one data folder. You propose; only the engine scores, verifies, and \
             applies. Start with the prompt diagnose_slow_query, or observe.why.",
        )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(tool_list())))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let server = self.server.clone();
        let name = request.name.to_string();
        let args = Value::Object(request.arguments.unwrap_or_default());
        let reply = tokio::task::spawn_blocking(move || server.call(&name, args))
            .await
            .map_err(|e| ErrorData::internal_error(format!("the tool call failed: {e}"), None))?;
        Ok(to_result(reply).into())
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListResourcesResult::with_all_items(resource_list())))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let server = self.server.clone();
        let uri = request.uri.clone();
        let text = tokio::task::spawn_blocking(move || server.resource(&uri))
            .await
            .map_err(|e| ErrorData::internal_error(format!("the read failed: {e}"), None))?
            .map_err(|r| {
                ErrorData::resource_not_found(r.message.clone(), serde_json::to_value(&r).ok())
            })?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, request.uri).with_mime_type("application/json")
        ])
        .into())
    }

    fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListPromptsResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListPromptsResult::with_all_items(vec![prompt_def()])))
    }

    fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<GetPromptResponse, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(get_prompt(&request))
    }
}
