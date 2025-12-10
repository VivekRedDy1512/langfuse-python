import { URL } from "node:url";
import fetch from "node-fetch";
import { env } from "../../env";

const PY_BACKEND_URL = "http://localhost:8001";

export async function pyGet<T>(path: string, params?: Record<string, any>): Promise<T> {
  const url = new URL(path, PY_BACKEND_URL);

  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    });
  }

  const res = await fetch(url.toString(), { method: "GET" });

  if (!res.ok) {
    throw new Error(`Python backend GET ${path} failed: ${res.status} ${await res.text()}`);
  }

  return (await res.json()) as T;
}

export async function pyPost<T>(path: string, body: any): Promise<T> {
  const url = new URL(path, PY_BACKEND_URL);

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Python backend POST ${path} failed: ${res.status} ${await res.text()}`);
  }

  return (await res.json()) as T;
}
