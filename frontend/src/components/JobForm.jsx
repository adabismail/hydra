import { useEffect, useState } from "react";
import { api } from "../api";

export default function JobForm({ phase, onChange }) {
  const [mode, setMode] = useState("dataset");
  const [datasets, setDatasets] = useState([]);
  const [dataset, setDataset] = useState("sample.txt");
  const [text, setText] = useState("the quick brown fox\nthe lazy dog\nthe fox runs");
  const [numMap, setNumMap] = useState(6);
  const [numReduce, setNumReduce] = useState(3);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    api.datasets().then((d) => {
      setDatasets(d.datasets);
      if (d.datasets.length && !d.datasets.includes(dataset)) setDataset(d.datasets[0]);
    }).catch(() => {});
  }, []);

  const running = phase === "MAP" || phase === "REDUCE";

  async function submit() {
    setBusy(true); setError("");
    try {
      const payload = { num_map: Number(numMap), num_reduce: Number(numReduce) };
      if (mode === "dataset") payload.dataset = dataset;
      else payload.text = text;
      const r = await api.submit(payload);
      if (!r.ok) setError(r.error || "submit failed");
      onChange && onChange();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function reset() {
    await api.reset();
    onChange && onChange();
  }

  return (
    <div className="card">
      <div className="card-h"><h2>Submit job</h2><span className="right">word count</span></div>
      <div className="card-b">
        <div className="seg">
          <button className={mode === "dataset" ? "on" : ""} onClick={() => setMode("dataset")}>Dataset</button>
          <button className={mode === "text" ? "on" : ""} onClick={() => setMode("text")}>Custom text</button>
        </div>

        {mode === "dataset" ? (
          <div className="fld">
            <label>Input dataset</label>
            <select value={dataset} onChange={(e) => setDataset(e.target.value)}>
              {datasets.map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
        ) : (
          <div className="fld">
            <label>Input text</label>
            <textarea value={text} onChange={(e) => setText(e.target.value)} rows={5} />
          </div>
        )}

        <div className="grid2">
          <div className="fld">
            <label>Map chunks</label>
            <input type="number" min="1" max="32" value={numMap} onChange={(e) => setNumMap(e.target.value)} />
          </div>
          <div className="fld">
            <label>Reducers</label>
            <input type="number" min="1" max="12" value={numReduce} onChange={(e) => setNumReduce(e.target.value)} />
          </div>
        </div>

        <button className="btn" onClick={submit} disabled={busy || running}>
          {running ? "Job running" : busy ? "Submitting…" : "Run job"}
        </button>
        <button className="btn ghost" onClick={reset}>Reset cluster</button>

        {error && <div className="warn-note">{error}</div>}
      </div>
    </div>
  );
}
