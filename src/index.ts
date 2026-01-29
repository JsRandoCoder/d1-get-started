// src/index.ts
/**
 * Full Worker: UI + API + D1 durable task storage + SSE events + retries/DLQ via Durable Object alarms
 * Keeps your existing /api/beverages endpoint.
 *
 * Requires bindings:
 * - D1 binding: DB
 * - Durable Object binding: SCHEDULER (class Scheduler)
 *
 * D1 tables needed (you already ran this):
 * - ara_tasks, ara_task_events
 */

export interface Env {
  DB: D1Database;
  SCHEDULER: DurableObjectNamespace;
  // Optional (if you want higher-quality answers):
  OPENAI_API_KEY?: string;
  OPENAI_MODEL?: string; // default gpt-4o-mini
}

type TaskStatus = "queued" | "running" | "retry" | "completed" | "dead" | "canceled";

type TaskRow = {
  id: string;
  status: TaskStatus;
  prompt: string;
  result: string | null;
  error: string | null;
  attempts: number;
  max_attempts: number;
  retry_at: string | null;
  created_at: string;
  updated_at: string;
};

type EventRow = {
  id: number;
  task_id: string;
  type: string;
  data: string | null;
  created_at: string;
};

type CreateTaskRequest = { prompt: string };

const UI_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>ARA (Cloudflare Worker)</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; max-width: 980px; }
    textarea { width: 100%; min-height: 120px; padding: 12px; font-size: 14px; }
    button { padding: 10px 14px; font-size: 14px; cursor: pointer; }
    .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 14px; margin-top: 14px; }
    .muted { color:#666; font-size: 13px; }
    pre { white-space: pre-wrap; word-break: break-word; }
    .log { max-height: 340px; overflow: auto; background: #fafafa; border-radius: 10px; padding: 10px; border: 1px solid #eee; }
    input[type="text"] { padding: 10px; border: 1px solid #ddd; border-radius: 10px; width: min(520px, 100%); }
    .pill { display:inline-block; padding: 2px 8px; border: 1px solid #ddd; border-radius: 999px; font-size: 12px; }
    .ok { border-color: #a7f3d0; background: #ecfdf5; }
    .bad { border-color: #fecaca; background: #fef2f2; }
    a { color: inherit; }
    table { border-collapse: collapse; width: 100%; }
    td, th { border: 1px solid #eee; padding: 8px; font-size: 13px; }
    th { background: #fafafa; text-align: left; }
    .click { cursor: pointer; }
  </style>
</head>
<body>
  <h1>ARA <span id="status" class="pill bad">idle</span></h1>
  <p class="muted">
    Single Cloudflare Worker: UI + API + D1 + Durable Object retries/DLQ.
    <span class="pill ok">/api/tasks</span>
    <span class="pill ok">SSE events</span>
    <span class="pill ok">retries</span>
    <span class="pill ok">dead-letter</span>
  </p>

  <div class="card">
    <div class="row">
      <button id="create">Create Task</button>
      <button id="cancel">Cancel</button>
      <button id="refreshList">Refresh List</button>
      <input id="taskId" type="text" placeholder="task id (auto)" />
      <a class="muted" href="/api/health" target="_blank">/api/health</a>
      <a class="muted" href="/api/beverages" target="_blank">/api/beverages</a>
    </div>

    <h3>Prompt</h3>
    <textarea id="prompt" placeholder="What should the agent do?"></textarea>
    <p class="muted">Tip: include URLs to fetch. “Latest/current” is best-effort unless you set OPENAI_API_KEY.</p>
  </div>

  <div class="card">
    <h3>Task List</h3>
    <div class="row">
      <select id="filter">
        <option value="">all</option>
        <option value="queued">queued</option>
        <option value="running">running</option>
        <option value="retry">retry</option>
        <option value="completed">completed</option>
        <option value="dead">dead</option>
        <option value="canceled">canceled</option>
      </select>
      <span class="muted">Click a row to load it.</span>
    </div>
    <div style="margin-top:10px; overflow:auto">
      <table>
        <thead>
          <tr>
            <th>ID</th><th>Status</th><th>Attempts</th><th>Updated</th><th>Prompt</th>
          </tr>
        </thead>
        <tbody id="list"></tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <h3>Output</h3>
    <pre id="output"></pre>
  </div>

  <div class="card">
    <h3>Events</h3>
    <pre id="log" class="log"></pre>
  </div>

<script>
(() => {
  const statusEl = document.getElementById("status");
  const promptEl = document.getElementById("prompt");
  const taskIdEl = document.getElementById("taskId");
  const outEl = document.getElementById("output");
  const logEl = document.getElementById("log");
  const createBtn = document.getElementById("create");
  const cancelBtn = document.getElementById("cancel");
  const refreshBtn = document.getElementById("refreshList");
  const listEl = document.getElementById("list");
  const filterEl = document.getElementById("filter");

  const setStatus = (ok, text) => {
    statusEl.textContent = text;
    statusEl.className = "pill " + (ok ? "ok" : "bad");
  };

  const log = (msg) => {
    logEl.textContent += "[" + new Date().toLocaleTimeString() + "] " + msg + "\\n";
    logEl.scrollTop = logEl.scrollHeight;
  };

  async function api(path, opts) {
    const res = await fetch(path, {
      ...opts,
      headers: { "content-type": "application/json", ...(opts && opts.headers || {}) }
    });
    const ct = res.headers.get("content-type") || "";
    const body = ct.includes("application/json") ? await res.json() : await res.text();
    if (!res.ok) throw new Error("HTTP " + res.status + " " + (typeof body === "string" ? body : JSON.stringify(body)));
    return body;
  }

  let es = null;
  function startEvents(taskId) {
    if (es) { es.close(); es = null; }
    es = new EventSource("/api/tasks/" + encodeURIComponent(taskId) + "/events");
    es.onmessage = (ev) => log(ev.data);
    es.onerror = () => {
      if (es) es.close();
      es = null;
      setTimeout(() => startEvents(taskId), 1500);
    };
  }

  async function poll(taskId) {
    for (;;) {
      const t = await api("/api/tasks/" + encodeURIComponent(taskId), { method: "GET" });
      setStatus(t.status === "completed", t.status);
      if (t.result) outEl.textContent = t.result;
      if (t.error) outEl.textContent = "ERROR:\\n" + t.error + "\\n\\n" + (t.result || "");
      if (["completed","dead","canceled"].includes(t.status)) break;
      await new Promise(r => setTimeout(r, 1200));
    }
  }

  function row(t) {
    const tr = document.createElement("tr");
    tr.className = "click";
    tr.innerHTML = "<td>" + t.id + "</td><td>" + t.status + "</td><td>" + t.attempts + "/" + t.max_attempts +
      "</td><td>" + t.updated_at + "</td><td>" + (t.prompt || "").slice(0, 80).replaceAll("<","&lt;") + "</td>";
    tr.onclick = () => {
      taskIdEl.value = t.id;
      promptEl.value = t.prompt || "";
      outEl.textContent = t.result || (t.error ? ("ERROR:\\n" + t.error) : "");
      logEl.textContent = "";
      setStatus(t.status === "completed", t.status);
      startEvents(t.id);
      poll(t.id).catch(e => log("poll error: " + e.message));
    };
    return tr;
  }

  async function refreshList() {
    const status = filterEl.value;
    const qs = status ? ("?status=" + encodeURIComponent(status)) : "";
    const data = await api("/api/tasks" + qs, { method: "GET" });
    listEl.innerHTML = "";
    (data.tasks || []).forEach(t => listEl.appendChild(row(t)));
  }

  createBtn.onclick = async () => {
    outEl.textContent = "";
    logEl.textContent = "";
    setStatus(false, "creating");
    const prompt = (promptEl.value || "").trim();
    if (!prompt) return alert("Enter a prompt.");
    const created = await api("/api/tasks", { method: "POST", body: JSON.stringify({ prompt }) });
    taskIdEl.value = created.id;
    log("created task " + created.id);
    setStatus(false, "queued");
    startEvents(created.id);
    poll(created.id).catch(e => log("poll error: " + e.message));
    refreshList().catch(() => {});
  };

  cancelBtn.onclick = async () => {
    const id = (taskIdEl.value || "").trim();
    if (!id) return alert("No task id.");
    await api("/api/tasks/" + encodeURIComponent(id) + "/cancel", { method: "POST", body: "{}" });
    log("cancel requested");
    refreshList().catch(() => {});
  };

  refreshBtn.onclick = () => refreshList().catch(e => alert(e.message));
  filterEl.onchange = () => refreshList().catch(() => {});

  refreshList().catch(() => {});
})();
</script>
</body>
</html>`;

function normalizePath(pathname: string): string {
  if (pathname.length > 1 && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
}

function nowIso(): string {
  return new Date().toISOString();
}

function json(data: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: { "content-type": "application/json; charset=utf-8", ...(init?.headers || {}) }
  });
}

function withCors(res: Response, origin = "*"): Response {
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", origin);
  h.set("Access-Control-Allow-Headers", "*");
  h.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: h });
}

async function dbExec(db: D1Database, sql: string, params: unknown[] = []): Promise<void> {
  await db.prepare(sql).bind(...params).run();
}

async function dbFirst<T>(db: D1Database, sql: string, params: unknown[] = []): Promise<T | null> {
  const res = await db.prepare(sql).bind(...params).first<T>();
  return res ?? null;
}

async function dbAll<T>(db: D1Database, sql: string, params: unknown[] = []): Promise<T[]> {
  const res = await db.prepare(sql).bind(...params).all<T>();
  return (res.results ?? []) as T[];
}

async function emitEvent(db: D1Database, taskId: string, type: string, data?: unknown): Promise<void> {
  const payload = data === undefined ? null : JSON.stringify(data);
  await dbExec(
    db,
    `INSERT INTO ara_task_events (task_id, type, data, created_at) VALUES (?, ?, ?, ?)`,
    [taskId, type, payload, nowIso()]
  );
}

async function getTask(db: D1Database, id: string): Promise<TaskRow | null> {
  return dbFirst<TaskRow>(
    db,
    `SELECT id, status, prompt, result, error, attempts, max_attempts, retry_at, created_at, updated_at
     FROM ara_tasks WHERE id=?`,
    [id]
  );
}

async function createTask(db: D1Database, prompt: string): Promise<TaskRow> {
  const id = crypto.randomUUID();
  const ts = nowIso();
  const maxAttempts = 5;

  await dbExec(
    db,
    `INSERT INTO ara_tasks (id, status, prompt, result, error, attempts, max_attempts, retry_at, created_at, updated_at)
     VALUES (?, 'queued', ?, NULL, NULL, 0, ?, NULL, ?, ?)`,
    [id, prompt, maxAttempts, ts, ts]
  );
  await emitEvent(db, id, "queued", { prompt });
  return (await getTask(db, id))!;
}

async function cancelTask(db: D1Database, id: string): Promise<void> {
  const t = await getTask(db, id);
  if (!t) return;
  if (["completed", "dead", "canceled"].includes(t.status)) return;
  await dbExec(db, `UPDATE ara_tasks SET status='canceled', updated_at=? WHERE id=?`, [nowIso(), id]);
  await emitEvent(db, id, "canceled");
}

async function schedulerWake(env: Env): Promise<void> {
  const id = env.SCHEDULER.idFromName("singleton");
  const stub = env.SCHEDULER.get(id);
  await stub.fetch("https://scheduler/enqueue", { method: "POST", body: "{}" });
}

function heuristicPlan(prompt: string): string[] {
  const steps: string[] = [];
  if (/\bhttps?:\/\/\S+/i.test(prompt)) steps.push("Fetch referenced URL(s).");
  if (/\b(latest|today|current|news)\b/i.test(prompt)) steps.push("Search (best-effort) and summarize.");
  steps.push("Draft answer.");
  return steps;
}

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url, { headers: { "user-agent": "Mozilla/5.0 (compatible; ARA/1.0)" } });
  const ct = res.headers.get("content-type") || "";
  if (!res.ok) return `Fetch failed ${res.status} ${res.statusText}`;
  if (ct.includes("application/json")) return JSON.stringify(await res.json(), null, 2);
  return await res.text();
}

async function ddgSearchHtml(query: string): Promise<string> {
  const url = `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
  const res = await fetch(url, { headers: { "user-agent": "Mozilla/5.0 (compatible; ARA/1.0)" } });
  return await res.text();
}

async function openaiAnswer(env: Env, prompt: string): Promise<string> {
  const model = env.OPENAI_MODEL || "gpt-4o-mini";
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model,
      messages: [
        { role: "system", content: "You are a helpful agent. Answer concisely and correctly." },
        { role: "user", content: prompt }
      ],
      temperature: 0.2
    })
  });

  if (!res.ok) throw new Error(`OpenAI HTTP ${res.status}: ${await res.text()}`);
  const data: any = await res.json();
  return data?.choices?.[0]?.message?.content ?? "";
}

async function runAgent(env: Env, prompt: string): Promise<{ result: string; trace: unknown }> {
  const plan = heuristicPlan(prompt);

  let evidence = "";
  const urls = Array.from(prompt.matchAll(/\bhttps?:\/\/\S+/gi)).map((m) => m[0].replace(/[)\],.]+$/g, ""));
  for (const u of urls.slice(0, 2)) {
    const t = await fetchText(u);
    evidence += `\n[fetch:${u}]\n${t.slice(0, 3500)}\n`;
  }

  if (/\b(latest|today|current|news)\b/i.test(prompt)) {
    const html = await ddgSearchHtml(prompt);
    evidence += `\n[search:duckduckgo_html]\n${html.slice(0, 3500)}\n`;
  }

  let draft =
    `Plan:\n${plan.map((s, i) => `${i + 1}. ${s}`).join("\n")}\n\n` +
    `Evidence (best-effort):\n${evidence || "(none)"}\n\nAnswer:\n`;

  if (env.OPENAI_API_KEY) {
    const llm = await openaiAnswer(env, `${prompt}\n\nUse this evidence if helpful:\n${evidence}`);
    draft += llm.trim();
  } else {
    draft += "Set OPENAI_API_KEY as a Worker secret for higher-quality answers.";
  }

  const notes: string[] = [];
  if (/\b(latest|today|current)\b/i.test(prompt) && !env.OPENAI_API_KEY) {
    notes.push("Without a model API key, ‘latest’ may be incomplete (best-effort search).");
  }
  if (notes.length) draft += `\n\nCritic notes:\n${notes.map((n) => `- ${n}`).join("\n")}\n`;

  return { result: draft, trace: { plan, urls, notes } };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);

    if (request.method === "OPTIONS") return withCors(new Response(null, { status: 204 }));

    // UI
    if (pathname === "/" && request.method === "GET") {
      return new Response(UI_HTML, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    // Health
    if (pathname === "/api/health" && request.method === "GET") {
      return withCors(json({ ok: true, ts: nowIso() }));
    }

    // Your existing D1 endpoint
    if (pathname === "/api/beverages" && request.method === "GET") {
      const { results } = await env.DB.prepare("SELECT * FROM Customers WHERE CompanyName = ?")
        .bind("Bs Beverages")
        .all();
      return withCors(json(results));
    }

    // Optional: keep DO poke test
    if (pathname === "/api/scheduler/poke" && request.method === "GET") {
      const id = env.SCHEDULER.idFromName("singleton");
      const stub = env.SCHEDULER.get(id);
      const res = await stub.fetch("https://scheduler/poke", { method: "POST" });
      return withCors(new Response(await res.text(), { status: res.status }));
    }

    // /api/tasks (create or list)
    if (pathname === "/api/tasks") {
      if (request.method === "POST") {
        const body = (await request.json().catch(() => ({}))) as Partial<CreateTaskRequest>;
        const prompt = (body.prompt || "").trim();
        if (!prompt) return withCors(json({ error: "prompt is required" }, { status: 400 }));

        const task = await createTask(env.DB, prompt);
        ctx.waitUntil(schedulerWake(env));
        return withCors(json({ id: task.id, status: task.status }));
      }

      if (request.method === "GET") {
        const status = (url.searchParams.get("status") || "").trim();
        const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || "30")));
        const params: unknown[] = [];
        let where = "";
        if (status) {
          where = "WHERE status = ?";
          params.push(status);
        }
        params.push(limit);

        const tasks = await dbAll<TaskRow>(
          env.DB,
          `SELECT id, status, prompt, result, error, attempts, max_attempts, retry_at, created_at, updated_at
           FROM ara_tasks ${where}
           ORDER BY updated_at DESC
           LIMIT ?`,
          params
        );
        return withCors(json({ tasks }));
      }
    }

    // /api/tasks/:id, /cancel, /events
    const parts = pathname.split("/").filter(Boolean);
    if (parts.length >= 3 && parts[0] === "api" && parts[1] === "tasks") {
      const taskId = parts[2];

      if (parts.length === 3 && request.method === "GET") {
        const task = await getTask(env.DB, taskId);
        if (!task) return withCors(json({ error: "not found" }, { status: 404 }));
        return withCors(json(task));
      }

      if (parts.length === 4 && parts[3] === "cancel" && request.method === "POST") {
        await cancelTask(env.DB, taskId);
        ctx.waitUntil(schedulerWake(env));
        return withCors(json({ ok: true }));
      }

      if (parts.length === 4 && parts[3] === "events" && request.method === "GET") {
        const task = await getTask(env.DB, taskId);
        if (!task) return withCors(json({ error: "not found" }, { status: 404 }));

        const encoder = new TextEncoder();
        const since = Number(url.searchParams.get("since") || "0");

        const stream = new ReadableStream<Uint8Array>({
          async start(controller) {
            const started = Date.now();
            let lastId = since;

            const send = (s: string) => controller.enqueue(encoder.encode(s));
            const sendEvent = (id: number, payload: string) => {
              send(`id: ${id}\n`);
              send(`event: message\n`);
              send(`data: ${payload.replace(/\n/g, "\\n")}\n\n`);
            };

            send(`retry: 1500\n\n`);

            while (Date.now() - started < 25_000) {
              const rows = await dbAll<EventRow>(
                env.DB,
                `SELECT id, task_id, type, data, created_at
                 FROM ara_task_events
                 WHERE task_id=? AND id>?
                 ORDER BY id ASC
                 LIMIT 50`,
                [taskId, lastId]
              );

              if (rows.length) {
                for (const r of rows) {
                  lastId = r.id;
                  const payload = JSON.stringify({
                    type: r.type,
                    data: r.data ? JSON.parse(r.data) : null,
                    at: r.created_at
                  });
                  sendEvent(r.id, payload);
                }
              } else {
                send(`event: message\ndata: ${JSON.stringify({ type: "heartbeat", at: nowIso() })}\n\n`);
              }

              await new Promise((r) => setTimeout(r, 1000));
            }

            controller.close();
          }
        });

        return withCors(
          new Response(stream, {
            headers: {
              "content-type": "text/event-stream; charset=utf-8",
              "cache-control": "no-cache",
              connection: "keep-alive"
            }
          })
        );
      }
    }

    return withCors(
      new Response("Not found", { status: 404, headers: { "content-type": "text/plain; charset=utf-8" } })
    );
  }
};

// Paste this by REPLACING your existing `export class Scheduler ...` class entirely.

export class Scheduler implements DurableObject {
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);

    if (request.method === "POST" && pathname === "/enqueue") {
      // Run immediately (fixes “stuck queued” even if alarms lag)
      await this.reclaimStuckRunning(5 * 60_000);
      await this.processDueTasks(3);

      // Schedule follow-up if needed
      await this.scheduleNextWake();
      return new Response("ok");
    }

    // Optional: keep your poke test
    if (request.method === "POST" && pathname === "/poke") {
      const count = ((await this.state.storage.get<number>("count")) ?? 0) + 1;
      await this.state.storage.put("count", count);
      return new Response(`Scheduler DO ok. count=${count}`);
    }

    return new Response("Not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    await this.reclaimStuckRunning(5 * 60_000);
    await this.processDueTasks(3);
    await this.scheduleNextWake();
  }

  private async scheduleNextWake(): Promise<void> {
    // If there’s still “due now” work, wake again soon.
    const now = nowIso();
    const due = await dbFirst<{ one: number }>(
      this.env.DB,
      `SELECT 1 as one
       FROM ara_tasks
       WHERE status IN ('queued','retry')
         AND (retry_at IS NULL OR retry_at <= ?)
       LIMIT 1`,
      [now]
    );

    if (due) {
      await this.state.setAlarm(Date.now() + 1000);
      return;
    }

    // Otherwise schedule at the next retry time (if any)
    const next = await this.nextRetryAt();
    if (next) {
      const ms = Math.max(1000, next.getTime() - Date.now());
      await this.state.setAlarm(Date.now() + ms);
    }
  }

  private async nextRetryAt(): Promise<Date | null> {
    const row = await dbFirst<{ retry_at: string }>(
      this.env.DB,
      `SELECT retry_at
       FROM ara_tasks
       WHERE status IN ('retry')
         AND retry_at IS NOT NULL
       ORDER BY retry_at ASC
       LIMIT 1`
    );
    if (!row?.retry_at) return null;
    const d = new Date(row.retry_at);
    return isNaN(d.getTime()) ? null : d;
  }

  private backoffMs(attempt: number): number {
    const base = 5000;
    const cap = 5 * 60_000;
    const exp = Math.min(cap, base * Math.pow(2, Math.max(0, attempt - 1)));
    const jitter = Math.floor(Math.random() * 750);
    return Math.min(cap, exp + jitter);
  }

  private async reclaimStuckRunning(timeoutMs: number): Promise<void> {
    const cutoff = new Date(Date.now() - timeoutMs).toISOString();

    const stuck = await dbAll<{ id: string; attempts: number; max_attempts: number }>(
      this.env.DB,
      `SELECT id, attempts, max_attempts
       FROM ara_tasks
       WHERE status='running' AND updated_at <= ?
       LIMIT 10`,
      [cutoff]
    );

    for (const s of stuck) {
      if (s.attempts >= s.max_attempts) {
        await dbExec(this.env.DB, `UPDATE ara_tasks SET status='dead', error=?, updated_at=? WHERE id=?`, [
          "Task lease expired (stuck running).",
          nowIso(),
          s.id
        ]);
        await emitEvent(this.env.DB, s.id, "dead", { error: "Task lease expired (stuck running)." });
      } else {
        const retryAt = new Date(Date.now() + this.backoffMs(s.attempts)).toISOString();
        await dbExec(this.env.DB, `UPDATE ara_tasks SET status='retry', error=?, retry_at=?, updated_at=? WHERE id=?`, [
          "Task lease expired (stuck running). Retrying.",
          retryAt,
          nowIso(),
          s.id
        ]);
        await emitEvent(this.env.DB, s.id, "retry_scheduled", { error: "lease expired", retry_at: retryAt });
      }
    }
  }

  private async claimBatch(limit: number): Promise<TaskRow[]> {
    const now = nowIso();

    const due = await dbAll<TaskRow>(
      this.env.DB,
      `SELECT id, status, prompt, result, error, attempts, max_attempts, retry_at, created_at, updated_at
       FROM ara_tasks
       WHERE status IN ('queued','retry')
         AND (retry_at IS NULL OR retry_at <= ?)
       ORDER BY created_at ASC
       LIMIT ?`,
      [now, limit]
    );

    for (const t of due) {
      await dbExec(
        this.env.DB,
        `UPDATE ara_tasks
         SET status='running', attempts=attempts+1, updated_at=?
         WHERE id=? AND status IN ('queued','retry')`,
        [nowIso(), t.id]
      );
      await emitEvent(this.env.DB, t.id, "running", { attempt: t.attempts + 1 });
    }

    const claimed: TaskRow[] = [];
    for (const t of due) {
      const latest = await getTask(this.env.DB, t.id);
      if (latest && latest.status === "running") claimed.push(latest);
    }
    return claimed;
  }

  private async processDueTasks(limit: number): Promise<void> {
    const batch = await this.claimBatch(limit);

    for (const task of batch) {
      const latest = await getTask(this.env.DB, task.id);
      if (!latest || latest.status !== "running") continue;

      try {
        await emitEvent(this.env.DB, latest.id, "executor_start");
        const { result, trace } = await runAgent(this.env, latest.prompt);

        await dbExec(
          this.env.DB,
          `UPDATE ara_tasks SET status='completed', result=?, error=NULL, retry_at=NULL, updated_at=? WHERE id=?`,
          [result, nowIso(), latest.id]
        );
        await emitEvent(this.env.DB, latest.id, "completed", { trace });
      } catch (err: any) {
        const msg = err?.message || String(err);
        const fresh = await getTask(this.env.DB, latest.id);
        const attempts = fresh?.attempts ?? latest.attempts;
        const maxAttempts = fresh?.max_attempts ?? latest.max_attempts;

        if (attempts >= maxAttempts) {
          await dbExec(this.env.DB, `UPDATE ara_tasks SET status='dead', error=?, updated_at=? WHERE id=?`, [
            msg,
            nowIso(),
            latest.id
          ]);
          await emitEvent(this.env.DB, latest.id, "dead", { error: msg });
        } else {
          const retryAt = new Date(Date.now() + this.backoffMs(attempts)).toISOString();
          await dbExec(
            this.env.DB,
            `UPDATE ara_tasks SET status='retry', error=?, retry_at=?, updated_at=? WHERE id=?`,
            [msg, retryAt, nowIso(), latest.id]
          );
          await emitEvent(this.env.DB, latest.id, "retry_scheduled", {
            error: msg,
            retry_at: retryAt,
            attempt: attempts
          });
        }
      }
    }
  }
}
