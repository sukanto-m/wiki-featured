import { useEffect, useMemo, useState } from "react";

function stripHtml(s) {
  if (!s) return "";
  return s.replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim();
}

function pickRandom(arr) {
  if (!arr || arr.length === 0) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

function makeClozeQuestion(text) {
  // Minimal “professional” cloze:
  // hide one longer word (>= 6 chars) that isn’t the first word.
  const words = text.split(" ");
  const candidates = words
    .map((w, i) => ({ w, i }))
    .filter(({ w, i }) => i > 0 && w.replace(/[^\p{L}]/gu, "").length >= 6);

  if (candidates.length === 0) {
    return { prompt: "True or False:", question: text, answer: text, mode: "tf" };
  }

  const target = pickRandom(candidates);
  const raw = words[target.i];
  const clean = raw.replace(/[^\p{L}]/gu, "");
  const blank = "_______";

  const questionWords = [...words];
  questionWords[target.i] = raw.replace(clean, blank);

  return {
    prompt: "Fill in the blank:",
    question: questionWords.join(" "),
    answer: text,
    mode: "cloze",
  };
}

export default function App() {
  const [state, setState] = useState({
    loading: true,
    error: "",
    date: "",
    facts: [],
  });

  const [current, setCurrent] = useState(null);
  const [revealed, setRevealed] = useState(false);

  async function load() {
    setState((s) => ({ ...s, loading: true, error: "" }));
    setRevealed(false);

    try {
      const res = await fetch("/dyk/today.json", { cache: "no-store" });
      if (!res.ok) throw new Error(`Failed to load today.json (${res.status})`);

      const data = await res.json();
      const facts = (data.facts || []).map(stripHtml).filter(Boolean);

      setState({
        loading: false,
        error: "",
        date: data.date || "",
        facts,
      });

      const picked = pickRandom(facts);
      setCurrent(picked ? makeClozeQuestion(picked) : null);
    } catch (e) {
      setState({
        loading: false,
        error: e?.message || "Something went wrong.",
        date: "",
        facts: [],
      });
      setCurrent(null);
    }
  }

  function nextQuestion() {
    setRevealed(false);
    const picked = pickRandom(state.facts);
    setCurrent(picked ? makeClozeQuestion(picked) : null);
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const subtitle = useMemo(() => {
    if (state.loading) return "Loading today’s facts…";
    if (state.error) return "Couldn’t load today’s feed.";
    if (!state.facts.length) return "No facts available today (or feed export missing).";
    return `${state.facts.length} facts available today`;
  }, [state.loading, state.error, state.facts.length]);

  return (
    <div className="page">
      <header className="header">
        <div className="brand">
          <div className="logo" aria-hidden="true">W</div>
          <div>
            <h1>Did You Know Quiz</h1>
            <p className="muted">
              {state.date ? `Daily from Wikipedia • ${state.date}` : "Daily from Wikipedia"}
            </p>
          </div>
        </div>

        <div className="actions">
          <button className="btn ghost" onClick={load} disabled={state.loading}>
            Refresh
          </button>
          <a className="btn ghost" href="https://en.wikipedia.org/wiki/Main_Page" target="_blank" rel="noreferrer">
            Source
          </a>
        </div>
      </header>

      <main className="main">
        <section className="card">
          <div className="cardTop">
            <div>
              <h2>Today’s question</h2>
              <p className="muted">{subtitle}</p>
            </div>
            <span className="pill">DYK</span>
          </div>

          {state.loading && (
            <div className="skeleton">
              <div className="bar" />
              <div className="bar short" />
              <div className="bar" />
            </div>
          )}

          {!state.loading && state.error && (
            <div className="error">
              <p><strong>Error:</strong> {state.error}</p>
              <p className="muted">
                Make sure your pipeline generated <code>site/public/dyk/today.json</code>.
              </p>
            </div>
          )}

          {!state.loading && !state.error && !current && (
            <div className="empty">
              <p>No question available.</p>
              <p className="muted">
                Either the feed had zero DYK facts, or the export file is empty.
              </p>
            </div>
          )}

          {!state.loading && !state.error && current && (
            <>
              <div className="qa">
                <div className="prompt">{current.prompt}</div>
                <div className="question">{current.question}</div>
              </div>

              <div className="controls">
                <button
                  className="btn primary"
                  onClick={() => setRevealed((r) => !r)}
                  disabled={!current}
                >
                  {revealed ? "Hide answer" : "Reveal answer"}
                </button>
                <button
                  className="btn"
                  onClick={nextQuestion}
                  disabled={state.facts.length < 2}
                  title={state.facts.length < 2 ? "Need at least 2 facts" : "Get another question"}
                >
                  New question
                </button>
              </div>

              {revealed && (
                <div className="answer">
                  <div className="answerTop">
                    <span className="pill subtle">Answer</span>
                  </div>
                  <p>{current.answer}</p>
                </div>
              )}
            </>
          )}
        </section>

        <footer className="footer">
          <p className="muted">
            Built to be light. Designed to last. (And yes, you can add Llama later.)
          </p>
        </footer>
      </main>
    </div>
  );
}
