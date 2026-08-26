/* eslint-disable @typescript-eslint/no-explicit-any -- MCP transport interop with Node http req/res */
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpServer } from "../../utils/mcp-setup";
import logger from "../../utils/logger";

// Stateless Streamable HTTP transport — no session table, no SSE GET-first handshake.
// The MCP client (opencode) POSTs JSON-RPC directly; the transport handles the response
// (either application/json or text/event-stream depending on the request).
const globalWithMcp = global as typeof globalThis & {
    __mcpServer: ReturnType<typeof createMcpServer> | undefined;
    __mcpTransport: StreamableHTTPServerTransport | undefined;
};

if (!globalWithMcp.__mcpServer) {
    globalWithMcp.__mcpServer = createMcpServer();
    globalWithMcp.__mcpTransport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined, // stateless — no session management
    });
}

export const config = {
    api: {
        // Disable body parsing so the MCP SDK can handle the raw request stream
        bodyParser: false,
        // Inform Next.js that the response is handled by an external resolver (MCP SDK)
        externalResolver: true,
    },
};

export default async function handler(req: any, res: any) {
    try {
        const transport = globalWithMcp.__mcpTransport!;
        await transport.handleRequest(req, res);
    } catch (error: any) {
        logger.error({ error: error.message, stack: error.stack }, "MCP handler error");
        if (!res.headersSent) {
            res.status(500).json({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32603, message: error.message || "Internal error" },
            });
        }
    }
}
