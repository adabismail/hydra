// Tiny REST client for the Hydra master API.
const BASE = import.meta.env.VITE_API || "http://127.0.0.1:8000";

async function j(method, path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}`);
  return res.json();
}

export const api = {
  status: () => j("GET", "/api/status"),
  logs: (after = 0) => j("GET", `/api/logs?after=${after}`),
  datasets: () => j("GET", "/api/datasets"),
  submit: (payload) => j("POST", "/api/job", payload),
  reset: () => j("POST", "/api/reset"),
  clearLogs: () => j("POST", "/api/logs/clear"),
};
