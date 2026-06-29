const LV = { info: "info", success: "ok", warn: "warn", error: "err" };

function fmt(ts) {
  return new Date(ts * 1000).toLocaleTimeString([], { hour12: false });
}

export default function LogPanel({ events, onClear }) {
  return (
    <div className="card">
      <div className="card-h">
        <h2>Event log</h2>
        <button className="btn-clear" onClick={onClear} disabled={events.length === 0}>Clear</button>
      </div>
      <div className="card-b">
        {events.length === 0 ? (
          <div className="empty">Assignments, failures and reassignments stream here.</div>
        ) : (
          <div className="log">
            {events.map((e) => (
              <div className={`ll lv-${e.level}`} key={e.seq}>
                <span className="ts mono">{fmt(e.ts)}</span>
                <span className="lv">{LV[e.level]}</span>
                <span className="msg">{e.message}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
