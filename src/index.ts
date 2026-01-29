// src/index.ts
export interface Env {
  DB: D1Database;
  SCHEDULER: DurableObjectNamespace;
}

function normalizePath(pathname: string): string {
  if (pathname.length > 1 && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);

    if (pathname === "/api/health") {
      return Response.json({ ok: true, ts: new Date().toISOString() });
    }

    if (pathname === "/api/beverages") {
      const { results } = await env.DB.prepare(
        "SELECT * FROM Customers WHERE CompanyName = ?"
      )
        .bind("Bs Beverages")
        .all();

      return Response.json(results);
    }

    if (pathname === "/api/scheduler/poke") {
      const id = env.SCHEDULER.idFromName("singleton");
      const stub = env.SCHEDULER.get(id);
      const res = await stub.fetch("https://scheduler/poke", { method: "POST" });
      return new Response(await res.text(), { status: res.status });
    }

    return new Response(
      "Call /api/health or /api/beverages or /api/scheduler/poke",
      { headers: { "content-type": "text/plain; charset=utf-8" } }
    );
  },
};

export class Scheduler implements DurableObject {
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname.endsWith("/") && url.pathname !== "/" ? url.pathname.slice(0, -1) : url.pathname;

    if (request.method === "POST" && pathname === "/poke") {
      const count = ((await this.state.storage.get<number>("count")) ?? 0) + 1;
      await this.state.storage.put("count", count);
      return new Response(`Scheduler DO ok. count=${count}`);
    }

    return new Response("Not found", { status: 404 });
  }
}
