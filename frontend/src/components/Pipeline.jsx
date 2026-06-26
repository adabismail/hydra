function Bar({ name, pct, done, total }) {
  return (
    <div className="pl">
      <span className="nm">{name}</span>
      <span className="track"><i className={pct === 100 ? "ok" : "run"} style={{ width: `${pct}%` }} /></span>
      <span className="rt"><b>{pct}%</b> &nbsp;{done}/{total}</span>
    </div>
  );
}

export default function Pipeline({ job }) {
  if (!job) {
    return (
      <div className="card">
        <div className="card-h"><h2>Pipeline</h2></div>
        <div className="card-b"><div className="empty">No job running. Submit one to trace map, shuffle and reduce.</div></div>
      </div>
    );
  }
  const sh = job.shuffle_status;
  const map = { PENDING: ["grey", "Waiting"], IN_PROGRESS: ["blue", "In progress"], DONE: ["green", "Complete"] }[sh];
  const shFill = sh === "DONE" ? 100 : sh === "IN_PROGRESS" ? 55 : 0;
  return (
    <div className="card">
      <div className="card-h">
        <h2>Pipeline</h2>
        <span className="right mono">{job.job_id} · {job.input_name}</span>
      </div>
      <div className="card-b">
        <div className="pipe">
          <Bar name="Map" pct={job.map.pct} done={job.map.done} total={job.map.total} />
          <div className="pl">
            <span className="nm">Shuffle</span>
            <span className="track"><i className={sh === "DONE" ? "ok" : "run"} style={{ width: `${shFill}%` }} /></span>
            <span className="rt"><span className={`st ${map[0]}`}><span className={`dot ${map[0]}`} />{map[1]}</span></span>
          </div>
          <Bar name="Reduce" pct={job.reduce.pct} done={job.reduce.done} total={job.reduce.total} />
        </div>
      </div>
    </div>
  );
}
