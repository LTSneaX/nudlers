/* eslint-disable @typescript-eslint/no-explicit-any -- MCP transport interop with Node http req/res */
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpServer } from "../../utils/mcp-setup";
import logger from "../../utils/logger";

export const config = {
    api: {
        // Disable body parsing so the MCP SDK can handle the raw request stream
        bodyParser: false,
        // Inform Next.js that the response is handled by an external resolver (MCP SDK)
        externalResolver: true,
    },
};

// Read the raw request body as a UTF-8 string. bodyParser is disabled, so we
// collect the IncomingMessage stream ourselves and hand it to the transport as
// a pre-parsed body — this is the officially documented pattern for the
// Streamable HTTP transport and avoids any ambiguity about who owns the stream
// under the Next.js Pages Router.
function readRawBody(req: any): Promise<string> {
    return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        req.on("data", (chunk: any) => {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
        req.on("error", reject);
    });
}

export default async function handler(req: any, res: any) {
    let transport: StreamableHTTPServerTransport | undefined;
    try {
        const rawBody = await readRawBody(req);

        // Stateless mode: SDK 1.30 forbids reusing a stateless transport across
        // requests ("Stateless transport cannot be reused across requests" —
        // throws, which Hono turns into a 500 with an EMPTY body). It also
        // requires the McpServer to be `connect()`ed to the transport so that
        // `onmessage` is wired up — without `connect()`, the initialize request
        // is never answered, the response stream never terminates, and Next.js
        // surfaces the hanging stream as a 500. Both are fixed here by building
        // a fresh server+transport pair per request and connecting them.
        //
        // `enableJsonResponse: true` makes the transport answer POSTs with
        // `application/json` (instead of opening an SSE stream that must be
        // flushed through Next.js), which is the robust choice for the Pages
        // Router and exactly what the MCP Streamable HTTP spec allows.
        const server = createMcpServer();
        transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: undefined, // stateless — no session management
            enableJsonResponse: true,
        });

        // Wire the server's onmessage handler into the transport. This is what
        // makes initialize / tools/list / tools/call actually get answered.
        await server.connect(transport);

        // Pass the pre-parsed body so the transport does not need to re-read the
        // (already consumed) request stream. The SDK parses this as JSON itself.
        let parsedBody: any = rawBody;
        if (rawBody && rawBody.trim().length > 0) {
            try {
                parsedBody = JSON.parse(rawBody);
            } catch {
                // Leave the raw string; the transport returns a JSON-RPC parse
                // error (400) instead of crashing.
            }
        }

        // Workaround: opencode's MCP client sends Accept: application/json only,
        // but the Streamable HTTP transport requires both application/json and
        // text/event-stream (per MCP spec). Patch the header before handing off.
        const acceptHeader = req.headers["accept"] || "";
        if (!acceptHeader.includes("text/event-stream")) {
            req.headers["accept"] = "application/json, text/event-stream";
        }

        await transport.handleRequest(req, res, parsedBody);
    } catch (error: any) {
        logger.error({ error: error?.message, stack: error?.stack }, "MCP handler error");
        if (!res.headersSent) {
            res.status(500).json({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32603, message: error?.message || "Internal error" },
            });
        }
    } finally {
        // Release per-request state. Safe: the response is fully written before
        // handleRequest resolves, and close() only clears internal stream maps.
        try {
            await transport?.close();
        } catch (e) {
            logger.warn({ error: (e as Error)?.message }, "MCP transport close error");
        }
    }
}
