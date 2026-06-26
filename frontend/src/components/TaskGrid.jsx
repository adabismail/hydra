const DOT = { PENDING: "grey", RUNNING: "blue", SUCCESS: "green", RETRYING: "blue", FAILED: "red" };
const LABEL = { PENDING: "pending", RUNNING: "running", SUCCESS: "done", RETRYING: "retrying", FAILED: "failed" };

function Tile({ t }) {
  return (
    <div className={`tile s-${t.state}`} title={`${t.task_id} · ${t.state} · attempt ${t.attempt}${t.worker_id ? " · " + t.worker_id : ""}`}>
      <span className="id mono">{t.task_id}</span>
      <span className="ft">
        <span className={`st ${DOT[t.state]}`}><span className={`dot ${DOT[t.state]}`} />{LABEL[t.state]}</span>
        {t.attempt > 1 && <span className="att mono">#{t.attempt}</span>}
      </span>
    </div>
  );
}

export default function TaskGrid({ tasks }) {
  const maps = tasks.filter((t) => t.kind === "map");
  const reds = tasks.filter((t) => t.kind === "reduce");
  return (
    <div className="card">
      <div className="card-h">
        <h2>Tasks</h2>
        <span className="right">{maps.length} map · {reds.length} reduce</span>
      </div>
      <div className="card-b">
        {tasks.length === 0 ? (
          <div className="empty">Tasks appear here once a job is submitted.</div>
        ) : (
          <>
            <div className="tg">
              <div className="h">Map</div>
              <div className="cells">{maps.map((t) => <Tile key={t.task_id} t={t} />)}</div>
            </div>
            <div className="tg">
              <div className="h">Reduce</div>
              <div className="cells">{reds.map((t) => <Tile key={t.task_id} t={t} />)}</div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
