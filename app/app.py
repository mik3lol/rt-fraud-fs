"""FE Bar fraud review queue (synthetic data).

Live analyst queue from Lakebase (fraud_review.review_queue), customer feature/event drilldown from
Unity Catalog (Feature View latest views + silver tables via a SQL warehouse), and approve / reject /
escalate actions written back to Lakebase together with an audit row in fraud_review.review_actions.
"""
import os
import time

import pandas as pd
import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

st.set_page_config(page_title="Fraud review queue", layout="wide")

LAKEBASE_ENDPOINT = os.environ["LAKEBASE_ENDPOINT"]
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]
C = f"{os.environ.get('FRAUD_CATALOG', 'mlo_serverless_stable_catalog')}.{os.environ.get('FRAUD_SCHEMA', 'fraud_rt')}"
STATUSES = ["pending", "escalated", "approved", "rejected"]
TOKEN_TTL_S = 45 * 60  # Lakebase OAuth tokens last 1h; refresh well before expiry


# ---------------------------------------------------------------- clients
@st.cache_resource
def ws() -> WorkspaceClient:
    return WorkspaceClient()  # app service principal (injected DATABRICKS_CLIENT_ID/SECRET)


@st.cache_resource
def pg_host() -> str:
    return ws().postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host


def pg_user() -> str:
    return os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or ws().current_user.me().user_name


_token = {"value": None, "minted": 0.0}


def pg_token() -> str:
    if _token["value"] is None or time.time() - _token["minted"] > TOKEN_TTL_S:
        _token["value"] = ws().postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
        _token["minted"] = time.time()
    return _token["value"]


def pg_conn():
    return psycopg.connect(host=pg_host(), dbname=LAKEBASE_DATABASE, user=pg_user(), password=pg_token(),
                           sslmode="require", connect_timeout=15)


def reviewer() -> str:
    h = st.context.headers
    return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "unknown-reviewer"


# ---------------------------------------------------------------- data access
def uc_query(statement: str, params: dict | None = None) -> pd.DataFrame:
    """Parameterized query on the SQL warehouse (Statement Execution API)."""
    w = ws()
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="30s",
        parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in (params or {}).items()])
    deadline = time.time() + 120
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING) and time.time() < deadline:
        time.sleep(1.5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query failed: {r.status.state} {r.status.error.message if r.status.error else ''}")
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame((r.result.data_array or []) if r.result else [], columns=cols)


def load_queue(statuses, corridors, payouts, min_score) -> pd.DataFrame:
    where, args = ["fraud_score >= %s"], [min_score]
    if statuses:
        where.append("status = ANY(%s)"); args.append(list(statuses))
    if corridors:
        where.append("corridor = ANY(%s)"); args.append(list(corridors))
    if payouts:
        where.append("payout_method = ANY(%s)"); args.append(list(payouts))
    sql = f"""SELECT transfer_id, customer_id, corridor, payout_method, amount_usd, transfer_ts, fraud_score,
                     score_threshold, model_version, status, reviewer, reviewed_at, review_notes
              FROM fraud_review.review_queue WHERE {' AND '.join(where)}
              ORDER BY fraud_score DESC, transfer_ts DESC LIMIT 500"""
    with pg_conn() as conn:
        cur = conn.execute(sql, args)
        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    if not df.empty:
        df["amount_usd"] = df["amount_usd"].astype(float)
    return df


def status_counts() -> dict:
    with pg_conn() as conn:
        return dict(conn.execute("SELECT status, count(*) FROM fraud_review.review_queue GROUP BY status").fetchall())


def load_actions(transfer_id: str) -> pd.DataFrame:
    with pg_conn() as conn:
        cur = conn.execute("""SELECT acted_at, reviewer, old_status, new_status, notes FROM fraud_review.review_actions
                              WHERE transfer_id = %s ORDER BY acted_at DESC""", (transfer_id,))
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def set_status(transfer_id: str, expected_old: str, new_status: str, notes: str) -> str:
    """Update the queue row and append the audit row in ONE transaction; refuse if someone else changed it."""
    with pg_conn() as conn:
        with conn.transaction():
            row = conn.execute("SELECT status FROM fraud_review.review_queue WHERE transfer_id = %s FOR UPDATE",
                               (transfer_id,)).fetchone()
            if row is None:
                raise ValueError("transfer is no longer in the queue")
            old = row[0]
            if old != expected_old:
                raise ValueError(f"status changed to '{old}' by someone else; refresh and retry")
            if old == new_status:
                raise ValueError(f"already {new_status}")
            who = reviewer()
            conn.execute("""UPDATE fraud_review.review_queue
                            SET status = %s, reviewer = %s,
                                reviewed_at = CASE WHEN %s = 'pending' THEN NULL ELSE now() END,
                                review_notes = %s, updated_at = now()
                            WHERE transfer_id = %s""",
                         (new_status, who, new_status, notes or None, transfer_id))
            conn.execute("""INSERT INTO fraud_review.review_actions (transfer_id, old_status, new_status, reviewer, notes)
                            VALUES (%s, %s, %s, %s, %s)""", (transfer_id, old, new_status, who, notes or None))
    return old


# (alias, Feature View offline table suffix, window-end column, features)
FEATURE_VIEWS = [
    ("otp", "wbhelw", "event_ts", ["failed_otp_count_10m", "failed_otp_count_1h"]),
    ("fl", "hz9zvb", "event_ts", ["failed_login_count_1h"]),
    ("lg", "spmlku", "event_ts", ["login_count_1h"]),
    ("dv", "if51a1", "event_ts", ["distinct_devices_24h"]),
    ("sv", "k5d481", "event_ts", ["screen_view_count_10m", "avg_dwell_ms_10m"]),
    ("ls", "2wcu7y", "event_ts", ["last_10_screens"]),
    ("tr", "vtzpar", "ts", ["transfer_sum_7d", "transfer_sum_30d", "transfer_count_30d", "distinct_corridors_30d"]),
    ("co", "xr0078", "ts", ["corridor_transfer_count_30d"]),
]


def _feature_sql() -> str:
    ctes, sel, joins = [], [], []
    for a, t, tc, cols in FEATURE_VIEWS:
        extra = " AND corridor = :corridor" if a == "co" else ""
        ctes.append(f"{a} AS (SELECT {tc} AS window_end, {', '.join(cols)} FROM {C}.fv_offline_{t} "
                    f"WHERE customer_id = :customer_id{extra} AND {tc} = (SELECT max({tc}) FROM {C}.fv_offline_{t}))")
        sel += [f"to_json({a}.{c}) AS {c}" if c == "last_10_screens" else f"{a}.{c}" for c in cols]
        joins.append(f"LEFT JOIN {a} ON true")
    return ("WITH " + ",\n".join(ctes) + "\nSELECT :customer_id AS customer_id, " + ", ".join(sel)
            + ", greatest(" + ", ".join(f"{a}.window_end" for a, *_ in FEATURE_VIEWS) + ") AS latest_window_end"
            + "\nFROM (SELECT 1) b " + " ".join(joins))


FEATURE_SQL = _feature_sql()


@st.cache_data(ttl=60, show_spinner=False)
def customer_features(customer_id: str, corridor: str) -> pd.DataFrame:
    # Each Feature View's latest materialized window, read from the offline tables directly. The
    # fv_offline_*_latest_view views are re-created by every materialization run, which drops their grants.
    # Same logic as those views (rows at the table's max window end); verified to match them.
    # A NULL count means the window was empty (= 0).
    return uc_query(FEATURE_SQL, {"customer_id": customer_id, "corridor": corridor})


@st.cache_data(ttl=60, show_spinner=False)
def customer_events(customer_id: str) -> dict:
    p = {"customer_id": customer_id}
    # is_fraud (synthetic label) is deliberately not shown to analysts. PII columns arrive masked for the app.
    return {
        "Transfers": uc_query(f"""SELECT ts, transfer_id, amount_usd, corridor, payout_method, recipient_id, device_id
                                  FROM {C}.silver_transfers WHERE customer_id = :customer_id ORDER BY ts DESC LIMIT 20""", p),
        "Logins": uc_query(f"""SELECT event_ts, success, failure_reason, auth_method, geo_country, device_id, app_version
                               FROM {C}.silver_login WHERE customer_id = :customer_id ORDER BY event_ts DESC LIMIT 20""", p),
        "OTPs": uc_query(f"""SELECT event_ts, status, otp_channel, attempt_no, device_id
                             FROM {C}.silver_otp WHERE customer_id = :customer_id ORDER BY event_ts DESC LIMIT 20""", p),
        "Screens": uc_query(f"""SELECT event_ts, screen_name, screen_seq, dwell_ms, session_id
                                FROM {C}.silver_screen_view WHERE customer_id = :customer_id ORDER BY event_ts DESC LIMIT 30""", p),
    }


@st.cache_resource
def startup_check() -> None:
    """One-time access check, logged to the app logs (stdout) so permissions can be verified without the UI."""
    try:
        print(f"[startup_check] lakebase OK as {pg_user()}: {status_counts()}", flush=True)
    except Exception as e:
        print(f"[startup_check] lakebase FAILED: {e}", flush=True)
    try:
        n = uc_query(f"SELECT count(*) AS n FROM {C}.silver_transfers").iloc[0, 0]
        fr = uc_query(FEATURE_SQL, {"customer_id": "C001699", "corridor": "US-PH"})
        print(f"[startup_check] unity catalog OK: silver_transfers rows={n}; features as of {fr.iloc[0]['latest_window_end']}", flush=True)
    except Exception as e:
        print(f"[startup_check] unity catalog FAILED: {e}", flush=True)


startup_check()

# ---------------------------------------------------------------- UI
st.title("Transfer fraud review queue")
st.caption("Synthetic data. Scores from transfer_fraud_model; features from the Feature Views; decisions are written to Lakebase.")

with st.sidebar:
    st.header("Filters")
    f_status = st.multiselect("Status", STATUSES, default=["pending", "escalated"])
    f_corr = st.multiselect("Corridor", ["US-MX", "US-PH", "US-IN", "US-GT", "US-NG"])
    f_pay = st.multiselect("Payout method", ["bank_deposit", "cash_pickup", "mobile_wallet", "home_delivery"])
    f_min = st.slider("Minimum score", 0.0, 1.0, 0.0, 0.01)
    auto = st.toggle("Auto-refresh every 30 s", value=True)
    st.caption(f"Signed in as {reviewer()}")


def render_detail(row: pd.Series):
    tid = row["transfer_id"]
    st.subheader(f"Transfer {tid}")
    a, b, c, d = st.columns(4)
    a.metric("Fraud score", f"{row['fraud_score']:.4f}", help=f"threshold {row['score_threshold']:.4f}, model v{row['model_version']}")
    b.metric("Amount", f"${row['amount_usd']:,.2f}")
    c.metric("Corridor / payout", f"{row['corridor']} / {row['payout_method']}")
    d.metric("Status", row["status"])

    left, right = st.columns([3, 2])
    with left:
        st.markdown(f"**Customer {row['customer_id']}: current features**")
        try:
            fdf = customer_features(row["customer_id"], row["corridor"])
            if fdf.empty:
                st.info("No feature rows for this customer.")
            else:
                st.caption(f"Latest materialized window ends {fdf.iloc[0]['latest_window_end'] or 'n/a'} UTC "
                           "(Feature Views refresh every 10 minutes).")
                f = fdf.drop(columns=["customer_id", "latest_window_end"]).iloc[0]
                st.dataframe(pd.DataFrame({"feature": f.index, "value": [("0 (empty window)" if v is None and "count" in k else v)
                                                                           for k, v in f.items()]}),
                             hide_index=True, width="stretch")
            st.markdown("**Recent activity**")
            ev = customer_events(row["customer_id"])
            for tab, (name, edf) in zip(st.tabs(list(ev)), ev.items()):
                with tab:
                    st.dataframe(edf, hide_index=True, width="stretch")
        except Exception as e:  # keep the queue usable if the warehouse is cold or access is missing
            st.warning(f"Could not load features/events from Unity Catalog: {e}")

    with right:
        st.markdown("**Decision**")
        notes = st.text_area("Notes", key=f"notes_{tid}", placeholder="Why approve / reject / escalate?")
        cols = st.columns(3)
        choices = [("Approve", "approved"), ("Reject", "rejected"), ("Escalate", "escalated")]
        if row["status"] != "pending":
            choices = [("Reopen", "pending")] + [x for x in choices if x[1] != row["status"]][:2]
        for col, (label, new) in zip(cols, choices):
            if col.button(label, key=f"{label}_{tid}", width="stretch",
                          type="primary" if new == "rejected" else "secondary"):
                try:
                    old = set_status(tid, row["status"], new, notes)
                    st.toast(f"{tid[:8]}: {old} -> {new}")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        if row.get("reviewer"):
            st.caption(f"Last decision by {row['reviewer']} at {row['reviewed_at']}: {row['review_notes'] or ''}")
        st.markdown("**Audit trail**")
        st.dataframe(load_actions(tid), hide_index=True, width="stretch")


@st.fragment(run_every=30 if auto else None)
def queue_view():
    try:
        counts = status_counts()
        df = load_queue(f_status, f_corr, f_pay, f_min)
    except Exception as e:
        st.error(f"Cannot reach the Lakebase review queue: {e}")
        return
    m = st.columns(len(STATUSES) + 1)
    for col, s in zip(m, STATUSES):
        col.metric(s.capitalize(), counts.get(s, 0))
    m[-1].caption(f"Refreshed {pd.Timestamp.now(tz='UTC'):%H:%M:%S} UTC")

    if df.empty:
        st.info("No transfers match the filters.")
        return
    event = st.dataframe(
        df, key="queue", on_select="rerun", selection_mode="single-row", hide_index=True, width="stretch",
        column_config={"fraud_score": st.column_config.ProgressColumn("fraud_score", min_value=0.0, max_value=1.0, format="%.4f"),
                       "amount_usd": st.column_config.NumberColumn(format="$%.2f")})
    rows = event.selection.rows if event and event.selection else []
    if rows:
        render_detail(df.iloc[rows[0]])
    else:
        st.caption("Select a row to see the customer's features, recent activity and decision buttons.")


queue_view()
