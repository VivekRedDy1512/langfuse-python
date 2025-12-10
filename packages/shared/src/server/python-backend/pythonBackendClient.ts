

const PY_BACKEND_URL =
  process.env.NEXT_PUBLIC_PY_BACKEND_URL ?? "http://localhost:8001";

async function handleResponse<T>(res: Response, path: string): Promise<T> {
  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Python backend error for ${path}: ${res.status} ${res.statusText} - ${text}`,
    );
  }
  return (await res.json()) as T;
}

export async function pyGet<T>(
  path: string,
  params?: Record<string, unknown>,
): Promise<T> {
  const url = new URL(path, PY_BACKEND_URL);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      url.searchParams.set(k, String(v));
    });
  }

  const res = await fetch(url.toString(), {
    method: "GET",
  });

  return handleResponse<T>(res, path);
}

export async function pyPost<T>(
  path: string,
  body?: Record<string, unknown>,
): Promise<T> {
  const url = new URL(path, PY_BACKEND_URL);

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });

  return handleResponse<T>(res, path);
}
