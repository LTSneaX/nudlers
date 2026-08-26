/* eslint-disable @typescript-eslint/no-explicit-any -- MCP tool handlers consume dynamic JSON responses from internal APIs whose shapes vary by endpoint; row-level typing would duplicate ad-hoc shapes from many endpoints */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

// Configuration
// In Next.js, we can trust the internal port or use localhost
const PORT = process.env.PORT || "6969";
const API_BASE = process.env.NUDLERS_API_URL || `http://localhost:${PORT}/api`;

// Helper function to make API requests
async function apiRequest<T>(
    endpoint: string,
    options: RequestInit = {}
): Promise<T> {
    const url = `${API_BASE}${endpoint}`;

    const response = await fetch(url, {
        ...options,
        headers: {
            "Content-Type": "application/json",
            ...options.headers,
        },
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API request failed: ${response.status} - ${errorText}`);
    }

    return response.json() as Promise<T>;
}

// Sum only charges (negative amounts, app-wide convention) as positive spend;
// refunds/income are excluded so they don't inflate "Total Spending" figures.
function totalSpend(amounts: Array<number | string | null | undefined>): number {
    return amounts.reduce((sum: number, raw) => {
        const p = Number(raw) || 0;
        return p < 0 ? sum - p : sum;
    }, 0);
}

// Signed price for a manual transaction: charges are stored negative,
// income positive (app-wide convention). Exported for tests.
export function signedManualPrice(price: number, type: "expense" | "income"): number {
    return type === "income" ? Math.abs(price) : -Math.abs(price);
}

// Helper to format currency
function formatCurrency(amount: number): string {
    return new Intl.NumberFormat("he-IL", {
        style: "currency",
        currency: "ILS",
    }).format(amount);
}

// ============================================================================
// Vault + sync stream helpers (used by vault/sync MCP tools)
// ============================================================================

// Vault status shape returned by GET /vault/status
interface VaultStatus {
    locked: boolean;
    initialized: boolean;
    needsMigration: boolean;
    hasPasskeys: boolean;
    passkeysCount: number;
}

// Fetch vault status without throwing on a non-2xx (caller decides).
async function fetchVaultStatus(): Promise<VaultStatus | null> {
    const response = await fetch(`${API_BASE}/vault/status`, {
        headers: { "Content-Type": "application/json" },
    });
    if (!response.ok) return null;
    return (await response.json()) as VaultStatus;
}

// Attempt to unlock the vault with a passphrase. Returns true on success.
// NEVER logs or returns the passphrase value — only success/failure.
async function unlockVaultWithPassphrase(
    passphrase: string
): Promise<boolean> {
    try {
        const response = await fetch(`${API_BASE}/vault/unlock`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ passphrase }),
        });
        if (!response.ok) return false;
        const data = (await response.json()) as { success?: boolean };
        return data?.success === true;
    } catch {
        return false;
    }
}

// Read the passphrase from the arg or env var. Returns undefined if neither.
function resolvePassphrase(arg?: string): string | undefined {
    if (arg) return arg;
    return process.env.NUDLERS_VAULT_PASSPHRASE;
}

// SSE event accumulator used by the trigger_sync tool. Data lines are single
// line JSON, so each `data:` line is parsed independently.
interface SyncEventRecord {
    event: string;
    data: any;
}

function parseSseEvent(event: string, rawData: string): SyncEventRecord | null {
    if (!rawData) return null;
    try {
        return { event, data: JSON.parse(rawData) };
    } catch {
        return null;
    }
}

// Thin typed interface for the reader options (avoids coupling the helper to
// call-site argument types).
interface SyncEventReaderOptions {
    daysBack: number;
    timeoutMs?: number;
}

// Runs a POST request against the sync-all-stream SSE endpoint and iterates
// each parsed event in order. The response body is consumed fully (or aborted
// after the caller-provided timeout). Returns ok:false with an error string if
// the request could not be established or the stream was aborted.
async function readSyncStream(
    options: SyncEventReaderOptions,
    onEvent: (record: SyncEventRecord) => void
): Promise<{ ok: boolean; error?: string }> {
    const timeoutMs = options.timeoutMs ?? 120000;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetch(`${API_BASE}/scrapers/sync-all-stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ daysBack: options.daysBack }),
            signal: controller.signal,
        });
        if (!response.ok) {
            const errorText = await response.text();
            return { ok: false, error: `Sync failed: ${response.status} - ${errorText}` };
        }

        const reader = response.body?.getReader();
        if (!reader) {
            return { ok: false, error: "No response stream available" };
        }

        const decoder = new TextDecoder();
        let buffer = "";
        let currentEvent = "message";

        // eslint-disable-next-line no-constant-condition
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });

            // Split buffered content into lines; keep the last partial line.
            const lines = buffer.split("\n");
            buffer = lines.pop() ?? "";

            for (const line of lines) {
                const trimmed = line.trim();
                if (trimmed === "") {
                    // Event terminator — emit the accumulated event.
                    currentEvent = "message";
                    continue;
                }
                if (trimmed.startsWith("event:")) {
                    currentEvent = trimmed.slice("event:".length).trim();
                    continue;
                }
                if (trimmed.startsWith("data:")) {
                    const raw = trimmed.slice("data:".length).trim();
                    const record = parseSseEvent(currentEvent, raw);
                    if (record) onEvent(record);
                }
            }
        }
        return { ok: true };
    } catch (error) {
        if (error instanceof Error && error.name === "AbortError") {
            return { ok: false, error: "AbortError" };
        }
        return { ok: false, error: String(error) };
    } finally {
        clearTimeout(timeout);
    }
}

export function createMcpServer() {
    const server = new McpServer({
        name: "nudlers",
        version: "1.0.0",
    });

    // ============================================================================
    // TOOL: Get Monthly Summary
    // ============================================================================
    server.registerTool(
        "get_monthly_summary",
        {
            description: "Get a monthly financial summary with expenses grouped by vendor/card. Returns bank income, bank expenses, card expenses, and net balance.",
            inputSchema: {
                billingCycle: z
                    .string()
                    .optional()
                    .describe("Billing cycle in YYYY-MM format (e.g., 2026-01). If not provided, uses current month."),
                startDate: z
                    .string()
                    .optional()
                    .describe("Start date in YYYY-MM-DD format (alternative to billingCycle)"),
                endDate: z
                    .string()
                    .optional()
                    .describe("End date in YYYY-MM-DD format (alternative to billingCycle)"),
                groupBy: z
                    .enum(["vendor", "description", "last4digits"])
                    .optional()
                    .describe("How to group results: 'vendor' (default), 'description', or 'last4digits'"),
            },
        },
        async ({ billingCycle, startDate, endDate, groupBy }) => {
            try {
                const params = new URLSearchParams();

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else if (startDate && endDate) {
                    params.append("startDate", startDate);
                    params.append("endDate", endDate);
                } else {
                    // Default to current month
                    const now = new Date();
                    const currentCycle = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
                    params.append("billingCycle", currentCycle);
                }

                if (groupBy) {
                    params.append("groupBy", groupBy);
                }

                const response = await apiRequest<{ items: any[] } | any[]>(`/reports/monthly-summary?${params}`);
                let data: any[] = [];

                if (Array.isArray(response)) {
                    data = response;
                } else if (response && Array.isArray(response.items)) {
                    data = response.items;
                }

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: "No data found for the specified period." }],
                    };
                }

                // Calculate totals
                let totalCardExpenses = 0;
                let totalBankIncome = 0;
                let totalBankExpenses = 0;

                const lines = data.map((row: any) => {
                    totalCardExpenses += Number(row.card_expenses) || 0;
                    totalBankIncome += Number(row.bank_income) || 0;
                    totalBankExpenses += Number(row.bank_expenses) || 0;

                    if (groupBy === "description") {
                        return `• ${row.description} (${row.category || "Uncategorized"}): ${formatCurrency(row.card_expenses)} (${row.transaction_count} transactions)`;
                    } else if (groupBy === "last4digits") {
                        return `• Card ***${row.last4digits}: ${formatCurrency(row.card_expenses)} (${row.transaction_count} transactions)`;
                    } else {
                        const name = row.vendor_nickname || row.vendor;
                        return `• ${name}: Card ${formatCurrency(row.card_expenses)}, Bank Income ${formatCurrency(row.bank_income)}, Bank Expenses ${formatCurrency(row.bank_expenses)}`;
                    }
                });

                const summary = [
                    `📊 Monthly Summary`,
                    `Period: ${billingCycle || `${startDate} to ${endDate}`}`,
                    "",
                    "--- Breakdown ---",
                    ...lines,
                    "",
                    "--- Totals ---",
                    `💳 Total Card Expenses: ${formatCurrency(totalCardExpenses)}`,
                    `📈 Total Bank Income: ${formatCurrency(totalBankIncome)}`,
                    `📉 Total Bank Expenses: ${formatCurrency(totalBankExpenses)}`,
                    `💰 Net Balance: ${formatCurrency(totalBankIncome - totalBankExpenses - totalCardExpenses)}`,
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching monthly summary: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Category Expenses
    // ============================================================================
    server.registerTool(
        "get_category_expenses",
        {
            description: "Get all transactions for a specific category in a given time period.",
            inputSchema: {
                category: z.string().describe("Category name to filter by (e.g., 'Groceries', 'Dining')"),
                billingCycle: z
                    .string()
                    .optional()
                    .describe("Billing cycle in YYYY-MM format"),
                startDate: z.string().optional().describe("Start date in YYYY-MM-DD format"),
                endDate: z.string().optional().describe("End date in YYYY-MM-DD format"),
                limit: z.number().optional().describe("Maximum number of transactions to return (default 50)"),
            },
        },
        async ({ category, billingCycle, startDate, endDate, limit = 50 }) => {
            try {
                const params = new URLSearchParams();
                params.append("category", category);

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else if (startDate && endDate) {
                    params.append("startDate", startDate);
                    params.append("endDate", endDate);
                } else {
                    const now = new Date();
                    params.append("billingCycle", `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`);
                }

                if (limit) {
                    params.append("limit", limit.toString());
                }

                const response = await apiRequest<{ items: any[] } | any[]>(`/transactions?${params}`);

                let data: any[] = [];
                if (Array.isArray(response)) {
                    data = response;
                } else if (response && Array.isArray(response.items)) {
                    data = response.items;
                }

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: `No transactions found for category "${category}".` }],
                    };
                }

                const total = totalSpend(data.map((t) => t.price));

                const transactions = data.slice(0, 20).map((t: any) => {
                    const date = new Date(t.date).toLocaleDateString("he-IL");
                    const installment = t.installments_total > 1
                        ? ` (${t.installments_number}/${t.installments_total})`
                        : "";
                    return `• ${date}: ${t.name} - ${formatCurrency(Math.abs(t.price))}${installment}`;
                });

                const summary = [
                    `📁 Category: ${category}`,
                    `💰 Total Spent: ${formatCurrency(total)} (${data.length} transactions)`,
                    "",
                    "--- Recent Transactions ---",
                    ...transactions,
                    data.length >= limit ? `\n... and more transactions available (use limit param to see more)` : "",
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching category expenses: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get All Categories
    // ============================================================================
    server.registerTool(
        "get_all_categories",
        {
            description: "List all spending categories that exist in the system.",
        },
        async () => {
            try {
                const response = await apiRequest<string[] | { items: string[] }>("/categories");
                let data: string[] = [];
                if (Array.isArray(response)) {
                    data = response;
                } else if (response && Array.isArray((response as any).items)) {
                    data = (response as any).items;
                }

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: "No categories found." }],
                    };
                }

                const summary = [
                    `📋 All Categories (${data.length} total)`,
                    "",
                    ...data.map((cat: string) => `• ${cat}`),
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching categories: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Search Transactions
    // ============================================================================
    server.registerTool(
        "search_transactions",
        {
            description: "Search for transactions by description, vendor, category, or identifier.",
            inputSchema: {
                query: z.string().min(2).describe("Search query (minimum 2 characters)"),
                billingCycle: z.string().optional().describe("Filter by billing cycle (YYYY-MM)"),
                startDate: z.string().optional().describe("Filter start date (YYYY-MM-DD)"),
                endDate: z.string().optional().describe("Filter end date (YYYY-MM-DD)"),
            },
        },
        async ({ query, billingCycle, startDate, endDate }) => {
            try {
                const params = new URLSearchParams();
                params.append("q", query);

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else if (startDate && endDate) {
                    params.append("startDate", startDate);
                    params.append("endDate", endDate);
                }

                const response = await apiRequest<{ items: any[] } | any[]>(`/transactions?${params}`);

                let data: any[] = [];
                if (Array.isArray(response)) {
                    data = response;
                } else if (response && Array.isArray(response.items)) {
                    data = response.items;
                }

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: `No transactions found matching "${query}".` }],
                    };
                }

                const total = totalSpend(data.map((t) => t.price));

                const transactions = data.slice(0, 25).map((t: any) => {
                    const date = new Date(t.date).toLocaleDateString("he-IL");
                    const category = t.category || "Uncategorized";
                    const vendor = t.vendor_nickname || t.vendor;
                    return `• ${date}: ${t.name} (${category}) - ${formatCurrency(Math.abs(t.price))} [${vendor}]`;
                });

                const summary = [
                    `🔍 Search Results for "${query}"`,
                    `Found ${data.length} transactions, Total Spent: ${formatCurrency(total)}`,
                    "",
                    ...transactions,
                    data.length > 25 ? `\n... and ${data.length - 25} more results` : "",
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error searching transactions: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Budgets
    // ============================================================================
    server.registerTool(
        "get_budgets",
        {
            description: "Get budget vs actual spending comparison for all categories.",
            inputSchema: {
                billingCycle: z
                    .string()
                    .optional()
                    .describe("Billing cycle in YYYY-MM format. Defaults to current month."),
            },
        },
        async ({ billingCycle }) => {
            try {
                const params = new URLSearchParams();

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else {
                    const now = new Date();
                    params.append("billingCycle", `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`);
                }

                const data = await apiRequest<any>(`/reports/budget-vs-actual?${params}`);

                if (!data || !data.categories || data.categories.length === 0) {
                    return {
                        content: [{ type: "text", text: "No budget data found." }],
                    };
                }

                const categories = data.categories.map((cat: any) => {
                    const budget = Number(cat.budget) || 0;
                    const actual = Number(cat.actual) || 0;
                    const remaining = budget - actual;
                    const percentage = budget > 0 ? Math.round((actual / budget) * 100) : 0;

                    let status = "✅";
                    if (percentage > 100) status = "🔴";
                    else if (percentage > 80) status = "🟡";

                    return `${status} ${cat.category}: ${formatCurrency(actual)} / ${formatCurrency(budget)} (${percentage}%) - ${remaining >= 0 ? "Remaining" : "Over"}: ${formatCurrency(Math.abs(remaining))}`;
                });

                const totalBudget = Number(data.totalBudget) || 0;
                const totalActual = Number(data.totalActual) || 0;

                const summary = [
                    `💰 Budget vs Actual - ${billingCycle || "Current Month"}`,
                    "",
                    "--- By Category ---",
                    ...categories,
                    "",
                    "--- Total ---",
                    `Budget: ${formatCurrency(totalBudget)}`,
                    `Actual: ${formatCurrency(totalActual)}`,
                    `Remaining: ${formatCurrency(totalBudget - totalActual)}`,
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching budgets: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Sync Status
    // ============================================================================
    server.registerTool(
        "get_sync_status",
        {
            description: "Get the synchronization status for all connected bank accounts and credit cards.",
        },
        async () => {
            try {
                const data = await apiRequest<any>("/scrapers/status");

                if (!data || !data.accounts || data.accounts.length === 0) {
                    return {
                        content: [{ type: "text", text: "No accounts configured." }],
                    };
                }

                const accounts = data.accounts.map((acc: any) => {
                    const lastSync = acc.last_scrape_time
                        ? new Date(acc.last_scrape_time).toLocaleString("he-IL")
                        : "Never";
                    const status = acc.last_scrape_status === "success" ? "✅" : acc.last_scrape_status === "failed" ? "❌" : "⏳";
                    const name = acc.nickname || acc.vendor;
                    return `${status} ${name}: Last sync ${lastSync}`;
                });

                const summary = [
                    `🔄 Sync Status`,
                    "",
                    ...accounts,
                    "",
                    data.autoSyncEnabled
                        ? `⚙️ Auto-sync: Enabled (every ${data.syncInterval} hours)`
                        : "⚙️ Auto-sync: Disabled",
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching sync status: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Recurring Payments
    // ============================================================================
    server.registerTool(
        "get_recurring_payments",
        {
            description: "Get a list of recurring payments and installments.",
        },
        async () => {
            try {
                const data = await apiRequest<any>("/reports/recurring-payments");

                if (!data || !data.payments || data.payments.length === 0) {
                    return {
                        content: [{ type: "text", text: "No recurring payments found." }],
                    };
                }

                const payments = data.payments.slice(0, 20).map((p: any) => {
                    const progress = p.installments_total > 1
                        ? ` (${p.installments_number}/${p.installments_total})`
                        : " (recurring)";
                    return `• ${p.name}: ${formatCurrency(Math.abs(p.price))}${progress}`;
                });

                const summary = [
                    `🔄 Recurring Payments & Installments`,
                    `Total: ${data.payments.length} active`,
                    "",
                    ...payments,
                    data.payments.length > 20 ? `\n... and ${data.payments.length - 20} more` : "",
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching recurring payments: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: List Accounts
    // ============================================================================
    server.registerTool(
        "list_accounts",
        {
            description: "List all configured bank accounts and credit cards.",
        },
        async () => {
            try {
                const data = await apiRequest<any[]>("/credentials");

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: "No accounts configured." }],
                    };
                }

                const accounts = data.map((acc: any) => {
                    const type = acc.vendor_type === "bank" ? "🏦" : "💳";
                    const name = acc.nickname || acc.vendor;
                    return `${type} ${name} (${acc.vendor})`;
                });

                const summary = [
                    `📋 Configured Accounts (${data.length} total)`,
                    "",
                    ...accounts,
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching accounts: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get All Transactions
    // ============================================================================
    server.registerTool(
        "get_all_transactions",
        {
            description: "Get all transactions for a specific time period.",
            inputSchema: {
                billingCycle: z.string().optional().describe("Billing cycle in YYYY-MM format"),
                startDate: z.string().optional().describe("Start date in YYYY-MM-DD format"),
                endDate: z.string().optional().describe("End date in YYYY-MM-DD format"),
                limit: z.number().optional().describe("Maximum number of transactions to return (default 50)"),
            },
        },
        async ({ billingCycle, startDate, endDate, limit = 50 }) => {
            try {
                const params = new URLSearchParams();

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else if (startDate && endDate) {
                    params.append("startDate", startDate);
                    params.append("endDate", endDate);
                } else {
                    const now = new Date();
                    params.append("billingCycle", `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`);
                }

                if (limit) {
                    params.append("limit", limit.toString());
                }

                const response = await apiRequest<{ items: any[] } | any[]>(`/transactions?${params}`);

                let data: any[] = [];
                if (Array.isArray(response)) {
                    data = response;
                } else if (response && Array.isArray(response.items)) {
                    data = response.items;
                }

                if (!data || data.length === 0) {
                    return {
                        content: [{ type: "text", text: "No transactions found for the specified period." }],
                    };
                }

                // Sort by date descending
                const sorted = data.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
                const total = totalSpend(sorted.map((t) => t.price));

                const transactions = sorted.slice(0, limit).map((t: any) => {
                    const date = new Date(t.date).toLocaleDateString("he-IL");
                    const category = t.category || "Uncategorized";
                    return `• ${date}: ${t.name} (${category}) - ${formatCurrency(Math.abs(t.price))}`;
                });

                const summary = [
                    `📜 All Transactions`,
                    `Period: ${billingCycle || `${startDate} to ${endDate}`}`,
                    `Total Spent: ${formatCurrency(total)} (${data.length} transactions)`,
                    "",
                    ...transactions,
                    data.length > limit ? `\n... and ${data.length - limit} more transactions` : "",
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching transactions: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Add Manual Expense
    // ============================================================================
    server.registerTool(
        "add_manual_expense",
        {
            description: "Add a manual expense or income transaction. Use this for cash purchases, transfers, or transactions not captured by bank scrapers.",
            inputSchema: {
                name: z.string().min(1).describe("Transaction description (e.g., 'Coffee at local cafe', 'Grocery shopping')"),
                price: z.number().describe("Amount in ILS as a positive number. Direction is determined by 'type'."),
                type: z.enum(["expense", "income"]).optional().describe("Transaction type: 'expense' (default) or 'income'"),
                date: z.string().describe("Transaction date in YYYY-MM-DD format"),
                category: z.string().optional().describe("Category name (e.g., 'Dining', 'Groceries', 'Transportation')"),
                memo: z.string().optional().describe("Additional notes or details about the transaction"),
            },
        },
        async ({ name, price, type = "expense", date, category, memo }) => {
            try {
                // Validate date format
                const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
                if (!dateRegex.test(date)) {
                    return {
                        content: [{ type: "text", text: `Invalid date format. Please use YYYY-MM-DD (e.g., 2024-01-15).` }],
                    };
                }

                const response = await apiRequest<any>("/transactions", {
                    method: "POST",
                    body: JSON.stringify({
                        name,
                        // App-wide convention: charges negative, income positive.
                        price: signedManualPrice(price, type),
                        date,
                        category,
                        memo,
                        vendor: "manual",
                    }),
                });

                if (response.success) {
                    const txn = response.transaction;
                    const formattedDate = new Date(txn.date).toLocaleDateString("he-IL");
                    const formattedAmount = formatCurrency(Math.abs(txn.price));

                    const summary = [
                        `✅ Manual ${type} added successfully!`,
                        "",
                        `📝 Description: ${txn.name}`,
                        `💰 Amount: ${formattedAmount}`,
                        `📅 Date: ${formattedDate}`,
                        txn.category ? `📁 Category: ${txn.category}` : "",
                        txn.memo ? `📋 Memo: ${txn.memo}` : "",
                    ].filter(Boolean).join("\n");

                    return {
                        content: [{ type: "text", text: summary }],
                    };
                } else {
                    return {
                        content: [{ type: "text", text: `Failed to add transaction: ${response.error || "Unknown error"}` }],
                    };
                }
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error adding manual expense: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Category Breakdown
    // ============================================================================
    server.registerTool(
        "get_category_breakdown",
        {
            description: "Get a breakdown of spending by category for a given period. Shows total spent per category with transaction counts.",
            inputSchema: {
                billingCycle: z.string().optional().describe("Billing cycle in YYYY-MM format"),
                startDate: z.string().optional().describe("Start date in YYYY-MM-DD format"),
                endDate: z.string().optional().describe("End date in YYYY-MM-DD format"),
            },
        },
        async ({ billingCycle, startDate, endDate }) => {
            try {
                const params = new URLSearchParams();

                if (billingCycle) {
                    params.append("billingCycle", billingCycle);
                } else if (startDate && endDate) {
                    params.append("startDate", startDate);
                    params.append("endDate", endDate);
                } else {
                    const now = new Date();
                    params.append("billingCycle", `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`);
                }

                // The refactored endpoint now returns the summary directly
                const responseData = await apiRequest<{ items: any[] }>(`/reports/monthly-summary?${params}&groupBy=category`);
                const response = responseData.items || [];

                if (!response || response.length === 0) {
                    return {
                        content: [{ type: "text", text: "No transactions found for the specified period." }],
                    };
                }

                // Sort by total DESC (highest absolute spending first)
                const sorted = [...response].sort((a, b) => {
                    const totalA = Math.abs(Number(a.total) || 0);
                    const totalB = Math.abs(Number(b.total) || 0);
                    return totalB - totalA;
                });
                const grandTotal = totalSpend(sorted.map((v) => v.total));

                const lines = sorted.map((row) => {
                    const total = Math.abs(Number(row.total) || 0);
                    const percentage = grandTotal > 0 ? Math.round((total / grandTotal) * 100) : 0;
                    return `• ${row.category || "Uncategorized"}: ${formatCurrency(total)} (${row.count} txs, ${percentage}%)`;
                });

                const summary = [
                    `📊 Category Breakdown`,
                    `Period: ${billingCycle || `${startDate} to ${endDate}`}`,
                    `Total Spending: ${formatCurrency(grandTotal)}`,
                    "",
                    "--- By Category ---",
                    ...lines,
                ].join("\n");

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching category breakdown: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Balance Projection
    // ============================================================================
    server.registerTool(
        "get_balance_projection",
        {
            description: "Get a daily balance projection for the next 30 days. Accounts for bank balances, recurring transactions, and credit card settlements.",
        },
        async () => {
            try {
                const data = await apiRequest<any>("/reports/projection");

                if (!data || !data.projection || data.projection.length === 0) {
                    return {
                        content: [{ type: "text", text: "No projection data available." }],
                    };
                }

                const summary = data.summary;
                const projection = data.projection;

                const lines = projection.filter((_: any, i: number) => i % 5 === 0 || i === projection.length - 1).map((p: any) => {
                    const date = new Date(p.date).toLocaleDateString("he-IL");
                    return `• ${date}: ${formatCurrency(p.totalBalance)}`;
                });

                const output = [
                    `📈 Balance Projection (Next 30 Days)`,
                    `Starting Balance: ${formatCurrency(summary.startingBalance)}`,
                    `Ending Balance: ${formatCurrency(summary.endingBalance)}`,
                    `Net Change: ${formatCurrency(summary.endingBalance - summary.startingBalance)}`,
                    "",
                    "--- Forecast Highlights ---",
                    ...lines,
                    "",
                    "--- Significant Upcoming Events ---",
                ];

                // Find days with significant changes or recurring events
                projection.forEach((p: any) => {
                    if (p.bankRecurring && p.bankRecurring.length > 0) {
                        const date = new Date(p.date).toLocaleDateString("he-IL");
                        p.bankRecurring.forEach((r: any) => {
                            output.push(`• ${date}: ${r.name} (${formatCurrency(r.amount)})`);
                        });
                    }
                    if (p.ccPayments && p.ccPayments.length > 0) {
                        const date = new Date(p.date).toLocaleDateString("he-IL");
                        p.ccPayments.forEach((cc: any) => {
                            output.push(`• ${date}: CC Settlement ${cc.displayName} (${formatCurrency(cc.amount)})`);
                        });
                    }
                });

                return {
                    content: [{ type: "text", text: output.join("\n") }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching balance projection: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Get Vault Status
    // ============================================================================
    server.registerTool(
        "get_vault_status",
        {
            description: "Get the vault status: whether it is locked/unlocked, initialized, needs migration, and whether passkeys are enrolled.",
        },
        async () => {
            try {
                const status = await apiRequest<VaultStatus>("/vault/status");

                if (!status) {
                    return {
                        content: [{ type: "text", text: "Unable to fetch vault status." }],
                    };
                }

                const initialized = status.initialized ? "yes" : "no";
                const passkey = status.hasPasskeys ? `yes (${status.passkeysCount})` : "no";
                const migration = status.needsMigration ? " | needs migration" : "";

                let summary: string;
                if (status.locked) {
                    summary = `Vault: 🔒 LOCKED (initialized: ${initialized} | passphrase: ${initialized} | passkey: ${passkey})${migration}`;
                } else {
                    summary = `Vault: ✅ unlocked (initialized: ${initialized} | passphrase: ${initialized} | passkey: ${passkey})${migration}`;
                }

                return {
                    content: [{ type: "text", text: summary }],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error fetching vault status: ${error}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Unlock Vault
    // ============================================================================
    server.registerTool(
        "unlock_vault",
        {
            description: "Unlock the vault using a passphrase (either provided as an argument or read from NUDLERS_VAULT_PASSPHRASE). Never echoes the passphrase.",
            inputSchema: {
                passphrase: z
                    .string()
                    .optional()
                    .describe("The vault passphrase. If not provided, NUDLERS_VAULT_PASSPHRASE is used."),
            },
        },
        async ({ passphrase }) => {
            const resolved = resolvePassphrase(passphrase);
            if (!resolved) {
                return {
                    content: [
                        {
                            type: "text",
                            text: "No passphrase available. Pass a passphrase arg or set NUDLERS_VAULT_PASSPHRASE env var.",
                        },
                    ],
                };
            }

            try {
                // apiRequest throws on non-2xx; the unlock endpoint returns 401 on a bad
                // passphrase. We detect that and surface a friendly message.
                await apiRequest<{ success?: boolean }>("/vault/unlock", {
                    method: "POST",
                    body: JSON.stringify({ passphrase: resolved }),
                });
                return {
                    content: [{ type: "text", text: "Vault unlocked ✅" }],
                };
            } catch (error) {
                const msg = error instanceof Error ? error.message : String(error);
                if (msg.includes("401")) {
                    return {
                        content: [{ type: "text", text: "Unlock failed: Invalid passphrase" }],
                    };
                }
                return {
                    content: [{ type: "text", text: `Unlock failed: ${msg}` }],
                };
            }
        }
    );

    // ============================================================================
    // TOOL: Trigger Sync
    // ============================================================================
    server.registerTool(
        "trigger_sync",
        {
            description: "Trigger a full synchronization of all bank accounts and credit cards. Streams progress and returns a per-account summary.",
            inputSchema: {
                daysBack: z
                    .number()
                    .int()
                    .positive()
                    .optional()
                    .default(30)
                    .describe("How many days of transactions to scrape. Defaults to 30."),
            },
        },
        async ({ daysBack = 30 }) => {
            const startedAt = Date.now();

            // Vault pre-check: if locked, try to unlock with env passphrase.
            let vaultStatus: VaultStatus | null = null;
            try {
                vaultStatus = await fetchVaultStatus();
            } catch {
                vaultStatus = null;
            }

            if (vaultStatus && vaultStatus.locked) {
                const passphrase = resolvePassphrase(undefined);
                if (!passphrase) {
                    return {
                        content: [
                            {
                                type: "text",
                                text: "Vault is locked and no passphrase available. Set NUDLERS_VAULT_PASSPHRASE env var or unlock via the nudlers dashboard.",
                            },
                        ],
                    };
                }
                const unlocked = await unlockVaultWithPassphrase(passphrase);
                if (!unlocked) {
                    return {
                        content: [
                            {
                                type: "text",
                                text: "Vault is locked and unlock failed. Unlock via the nudlers dashboard or set NUDLERS_VAULT_PASSPHRASE env var.",
                            },
                        ],
                    };
                }
            }

            // Accumulated state from the SSE stream.
            const accountsById = new Map<string, any>();
            let totalAccounts = 0;
            let completeEvent: any = null;
            let errorEvent: any = null;

            const result = await readSyncStream(
                { daysBack, timeoutMs: 120000 },
                (record) => {
                    switch (record.event) {
                        case "queue": {
                            const accounts = record.data?.accounts;
                            if (Array.isArray(accounts)) {
                                totalAccounts = accounts.length;
                                for (const acc of accounts) {
                                    accountsById.set(acc.id, {
                                        id: acc.id,
                                        nickname: acc.nickname || acc.vendor || "Unknown",
                                    });
                                }
                            }
                            break;
                        }
                        case "account_start": {
                            const d = record.data || {};
                            if (!accountsById.has(d.id)) {
                                accountsById.set(d.id, {
                                    id: d.id,
                                    nickname: d.nickname || "Unknown",
                                });
                            }
                            break;
                        }
                        case "account_complete": {
                            const d = record.data || {};
                            accountsById.set(d.id, {
                                id: d.id,
                                nickname:
                                    (accountsById.get(d.id)?.nickname) ||
                                    d.nickname ||
                                    "Unknown",
                                savedTransactions: d.summary?.savedTransactions ?? 0,
                            });
                            break;
                        }
                        case "account_error": {
                            const d = record.data || {};
                            accountsById.set(d.id, {
                                id: d.id,
                                nickname:
                                    (accountsById.get(d.id)?.nickname) ||
                                    d.nickname ||
                                    "Unknown",
                                error: d.message || "Unknown error",
                            });
                            break;
                        }
                        case "complete":
                            completeEvent = record.data || {};
                            break;
                        case "error":
                            errorEvent = record.data || {};
                            break;
                        default:
                            break;
                    }
                }
            );

            // A terminal `error` SSE event stops the sync immediately.
            if (errorEvent) {
                return {
                    content: [
                        {
                            type: "text",
                            text: `Sync failed: ${errorEvent.message || "Unknown error"}`,
                        },
                    ],
                };
            }

            if (!result.ok && result.error === "AbortError") {
                // Partial summary — accounts that started but did not complete.
                const lines: string[] = [
                    `🔄 Sync triggered for ${totalAccounts} accounts (daysBack: ${daysBack})`,
                    "",
                ];
                for (const acc of accountsById.values()) {
                    if (acc.savedTransactions != null) {
                        lines.push(`✅ ${acc.nickname} — ${acc.savedTransactions} new transactions`);
                    } else if (acc.error) {
                        lines.push(`❌ ${acc.nickname} — error: ${acc.error}`);
                    } else {
                        lines.push(`⏳ ${acc.nickname} — still running...`);
                    }
                }
                lines.push("");
                lines.push("⏱️ Sync still running (120s timeout). Check get_sync_status for updates.");
                return {
                    content: [{ type: "text", text: lines.join("\n") }],
                };
            }

            if (!result.ok) {
                return {
                    content: [{ type: "text", text: result.error || "Sync failed" }],
                };
            }

            // Build the complete summary.
            const lines: string[] = [
                `🔄 Sync triggered for ${totalAccounts} accounts (daysBack: ${daysBack})`,
                "",
            ];
            for (const acc of accountsById.values()) {
                if (acc.savedTransactions != null) {
                    lines.push(`✅ ${acc.nickname} — ${acc.savedTransactions} new transactions`);
                } else if (acc.error) {
                    lines.push(`❌ ${acc.nickname} — error: ${acc.error}`);
                } else {
                    lines.push(`⏳ ${acc.nickname} — still running...`);
                }
            }

            const duration =
                completeEvent?.summary?.durationSeconds ??
                Math.round((Date.now() - startedAt) / 1000);
            lines.push("");
            lines.push(`✅ Sync complete in ${duration}s`);

            return {
                content: [{ type: "text", text: lines.join("\n") }],
            };
        }
    );

    // ============================================================================
    // TOOL: Stop Sync
    // ============================================================================
    server.registerTool(
        "stop_sync",
        {
            description: "Stop any running scrapers and kill their browser processes.",
        },
        async () => {
            try {
                const data = await apiRequest<{ success?: boolean; message?: string }>(
                    "/scrapers/stop",
                    { method: "POST" }
                );

                if (data?.success) {
                    return {
                        content: [{ type: "text", text: "🛑 All scrapers stopped" }],
                    };
                }
                return {
                    content: [
                        {
                            type: "text",
                            text: `Error stopping scrapers: ${data?.message || "Unknown error"}`,
                        },
                    ],
                };
            } catch (error) {
                return {
                    content: [{ type: "text", text: `Error stopping scrapers: ${error}` }],
                };
            }
        }
    );

    return server;
}
