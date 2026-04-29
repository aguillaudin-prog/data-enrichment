import { getSupabaseServer } from "@/lib/supabase";
import { approveDraft, rejectDraft, resetDraft } from "./actions";

type Draft = {
  id: string;
  operator_name: string;
  email: string;
  source_url: string;
  snippet: string;
  fetched_at: string;
  method: string;
  score: number;
  is_best: boolean;
  mx_valid: boolean;
  post_check_failed: boolean | null;
  validation_status: "approved" | "rejected" | "needs_more_info" | null;
  validated_at: string | null;
};

export const dynamic = "force-dynamic";

const FILTERS = [
  { key: "pending", label: "Pending" },
  { key: "approved", label: "Approved" },
  { key: "rejected", label: "Rejected" },
  { key: "all", label: "All" },
];

function scoreClass(s: number): string {
  if (s >= 80) return "score score-high";
  if (s >= 50) return "score score-mid";
  return "score score-low";
}

export default async function DraftsPage({
  searchParams,
}: {
  searchParams: Promise<{ status?: string }>;
}) {
  const params = await searchParams;
  const filter = params.status ?? "pending";

  const sb = getSupabaseServer();

  let query = sb
    .from("operator_enrichment_drafts")
    .select("*")
    .order("operator_name", { ascending: true })
    .order("score", { ascending: false });

  if (filter === "pending") query = query.is("validation_status", null);
  else if (filter === "approved") query = query.eq("validation_status", "approved");
  else if (filter === "rejected") query = query.eq("validation_status", "rejected");

  const { data, error } = await query;

  if (error) {
    return (
      <main>
        <h1>APEX Enrichment Drafts</h1>
        <div className="empty" style={{ color: "crimson" }}>
          Erreur Supabase: {error.message}
        </div>
      </main>
    );
  }

  const drafts = (data ?? []) as Draft[];

  // Group by operator
  const byOperator = new Map<string, Draft[]>();
  for (const d of drafts) {
    const list = byOperator.get(d.operator_name) ?? [];
    list.push(d);
    byOperator.set(d.operator_name, list);
  }

  // Counters: independent of filter
  const { data: allRows } = await sb
    .from("operator_enrichment_drafts")
    .select("operator_name, validation_status, is_best");
  const all = (allRows ?? []) as Pick<Draft, "operator_name" | "validation_status" | "is_best">[];
  const opSet = new Set(all.map((d) => d.operator_name));
  const approvedOps = new Set(
    all.filter((d) => d.validation_status === "approved").map((d) => d.operator_name),
  );
  const pendingDrafts = all.filter((d) => d.validation_status === null).length;

  return (
    <main>
      <h1>APEX Enrichment Drafts</h1>
      <div className="counters">
        <span>{opSet.size} opérateurs avec drafts</span>
        <span>{approvedOps.size} validés</span>
        <span>{pendingDrafts} drafts en attente</span>
      </div>

      <nav className="filters">
        {FILTERS.map((f) => (
          <a
            key={f.key}
            href={`/drafts?status=${f.key}`}
            className={filter === f.key ? "active" : ""}
          >
            {f.label}
          </a>
        ))}
      </nav>

      {byOperator.size === 0 && <div className="empty">Aucun draft pour ce filtre.</div>}

      {Array.from(byOperator.entries()).map(([op, list]) => (
        <section key={op}>
          <h2>{op}</h2>
          <table>
            <thead>
              <tr>
                <th>Email</th>
                <th>Score</th>
                <th>MX</th>
                <th>Best</th>
                <th>Method</th>
                <th>Post-check</th>
                <th>Source</th>
                <th>Snippet</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {list.map((d) => (
                <tr key={d.id} className={d.validation_status ?? ""}>
                  <td>
                    <a href={`mailto:${d.email}`}>{d.email}</a>
                  </td>
                  <td className={scoreClass(d.score)}>{d.score}</td>
                  <td>
                    <span className={d.mx_valid ? "badge badge-ok" : "badge badge-bad"}>
                      {d.mx_valid ? "OK" : "no MX"}
                    </span>
                  </td>
                  <td>{d.is_best ? "★" : ""}</td>
                  <td>{d.method}</td>
                  <td>
                    {d.post_check_failed === true ? (
                      <span className="badge badge-bad">disparu</span>
                    ) : d.post_check_failed === false ? (
                      <span className="badge badge-ok">OK</span>
                    ) : (
                      <span className="badge badge-warn">pending</span>
                    )}
                  </td>
                  <td>
                    <a href={d.source_url} target="_blank" rel="noreferrer">
                      link
                    </a>
                  </td>
                  <td className="snippet">{d.snippet}</td>
                  <td>{d.validation_status ?? "pending"}</td>
                  <td style={{ whiteSpace: "nowrap" }}>
                    {d.validation_status === null ? (
                      <>
                        <form action={approveDraft} style={{ display: "inline" }}>
                          <input type="hidden" name="id" value={d.id} />
                          <button type="submit" className="approve">
                            Approve
                          </button>
                        </form>
                        <form action={rejectDraft} style={{ display: "inline" }}>
                          <input type="hidden" name="id" value={d.id} />
                          <button type="submit" className="reject">
                            Reject
                          </button>
                        </form>
                      </>
                    ) : (
                      <form action={resetDraft} style={{ display: "inline" }}>
                        <input type="hidden" name="id" value={d.id} />
                        <button type="submit">Reset</button>
                      </form>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ))}
    </main>
  );
}
