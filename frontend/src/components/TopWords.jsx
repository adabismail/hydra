export default function TopWords({ job }) {
  const words = job?.top_words || [];
  const max = words.length ? words[0][1] : 1;
  return (
    <div className="card">
      <div className="card-h">
        <h2>Output</h2>
        <span className="right">{words.length ? "top words" : "—"}</span>
      </div>
      <div className="card-b">
        {words.length === 0 ? (
          <div className="empty">Final word counts appear here after the reduce phase.</div>
        ) : (
          <>
            <div className="tw">
              {words.slice(0, 12).map(([w, c], i) => (
                <div className="tw-row" key={w}>
                  <span className="tw-rank mono">{String(i + 1).padStart(2, "0")}</span>
                  <span className="tw-w mono">{w}</span>
                  <span className="tw-track"><i style={{ width: `${(c / max) * 100}%` }} /></span>
                  <span className="tw-c mono">{c}</span>
                </div>
              ))}
            </div>
            {job.output_file && (
              <div className="tw-foot">
                <b>{job.total_words}</b> tokens counted across <b>{job.num_reduce}</b> reducers.
                <div className="path mono">{job.output_file}</div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
