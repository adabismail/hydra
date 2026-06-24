export default function Header({ phase, connected, cluster }) {
  return (
    <div className="topbar">
      <div className="topbar-in">
        <div className="wordmark">
          <span className="name">Hydra</span>
          <span className="tag">MapReduce</span>
        </div>
        <div className="tb-spacer" />
        <div className="tb-stats">
          <div className="tb-stat">
            <span className="l">Phase</span>
            <span className="v">{phase === "IDLE" ? "Idle" : phase[0] + phase.slice(1).toLowerCase()}</span>
          </div>
          <div className="tb-stat">
            <span className="l">Nodes</span>
            <span className="v mono">{cluster.workers_total}</span>
          </div>
          <div className="tb-stat">
            <span className="l">Busy</span>
            <span className="v mono">{cluster.busy}</span>
          </div>
          <div className="tb-stat">
            <span className="l">Dead</span>
            <span className={`v mono ${cluster.dead ? "red" : ""}`}>{cluster.dead}</span>
          </div>
          <div className="tb-conn">
            <span className={`pulse ${connected ? "" : "off"}`} />
            {connected ? "Connected" : "Offline"}
          </div>
        </div>
      </div>
    </div>
  );
}
