const DOT = { IDLE: "green", BUSY: "blue", DEAD: "red" };
const LABEL = { IDLE: "Idle", BUSY: "Busy", DEAD: "Dead" };

export default function WorkerTable({ workers }) {
  return (
    <div className="card">
      <div className="card-h">
        <h2>Workers</h2>
        <span className="right">{workers.length} registered</span>
      </div>
      <div className="card-b tight">
        {workers.length === 0 ? (
          <div style={{ padding: "0 16px 16px" }}><div className="empty">No workers registered. Start one with <span className="mono">run_worker.py</span>.</div></div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Node</th><th>Status</th><th>Current task</th><th className="num">Heartbeat</th><th>Address</th>
              </tr>
            </thead>
            <tbody>
              {workers.map((w) => (
                <tr key={w.worker_id}>
                  <td className="mono">{w.worker_id}</td>
                  <td><span className={`st ${DOT[w.status]}`}><span className={`dot ${DOT[w.status]}`} />{LABEL[w.status]}</span></td>
                  <td className="mono">{w.current_task || <span className="dim">—</span>}</td>
                  <td className="num mono">
                    {w.status === "DEAD"
                      ? <span style={{ color: "var(--red)" }}>no signal</span>
                      : `${w.seconds_since_hb}s`}
                  </td>
                  <td className="mono dim">{w.url.replace("http://", "")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
