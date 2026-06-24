import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";
import Header from "./components/Header";
import JobForm from "./components/JobForm";
import ClusterReadout from "./components/StatCards";
import Pipeline from "./components/Pipeline";
import WorkerTable from "./components/WorkerTable";
import TaskGrid from "./components/TaskGrid";
import LogPanel from "./components/LogPanel";
import TopWords from "./components/TopWords";

const EMPTY = {
  phase: "IDLE",
  cluster: { workers_total: 0, idle: 0, busy: 0, dead: 0 },
  job: null,
  workers: [],
  tasks: [],
};

export default function App() {
  const [status, setStatus] = useState(EMPTY);
  const [events, setEvents] = useState([]);
  const [connected, setConnected] = useState(false);
  const lastSeq = useRef(0);

  const poll = useCallback(async () => {
    try {
      const s = await api.status();
      setStatus(s);
      setConnected(true);
      const { events: newEvents } = await api.logs(lastSeq.current);
      if (newEvents.length) {
        lastSeq.current = newEvents[newEvents.length - 1].seq;
        setEvents((prev) => [...prev, ...newEvents].slice(-300));
      }
    } catch {
      setConnected(false);
    }
  }, []);

  useEffect(() => {
    poll();
    const id = setInterval(poll, 1000);
    return () => clearInterval(id);
  }, [poll]);

  const resetEvents = () => {
    lastSeq.current = 0;
    setEvents([]);
  };

  const clearLog = useCallback(async () => {
    try { await api.clearLogs(); } catch { /* clear locally regardless */ }
    resetEvents();
  }, []);

  return (
    <>
      <Header phase={status.phase} connected={connected} cluster={status.cluster} />
      <div className="wrap">
        <div className="rowfull"><ClusterReadout cluster={status.cluster} /></div>
        <div className="rowfull"><Pipeline job={status.job} /></div>
        <div className="cols">
          <div className="stack">
            <WorkerTable workers={status.workers} />
            <TaskGrid tasks={status.tasks} />
            <LogPanel events={[...events].reverse()} onClear={clearLog} />
          </div>
          <div className="stack">
            <JobForm phase={status.phase} onChange={() => { resetEvents(); poll(); }} />
            <TopWords job={status.job} />
          </div>
        </div>
      </div>
      <div className="foot">
        <span>Hydra <b>v1.0</b></span>
        <span>Master <b>127.0.0.1:8000</b></span>
        <span>Protocol <b>HTTP / JSON</b></span>
        <span>Shuffle <b>file-based</b></span>
      </div>
    </>
  );
}
