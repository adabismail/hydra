export default function ClusterReadout({ cluster }) {
  const items = [
    { l: "Workers", n: cluster.workers_total, dot: null },
    { l: "Idle", n: cluster.idle, dot: "green" },
    { l: "Busy", n: cluster.busy, dot: "blue" },
    { l: "Dead", n: cluster.dead, dot: "red", red: cluster.dead > 0 },
  ];
  return (
    <div className="card">
      <div className="kpis">
        {items.map((it) => (
          <div className="kpi" key={it.l}>
            <div className="l">{it.dot && <span className={`dot ${it.dot}`} />}{it.l}</div>
            <div className={`n ${it.red ? "red" : ""}`}>{it.n}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
