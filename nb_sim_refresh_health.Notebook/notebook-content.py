# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1697cd57-854d-4130-87ae-4817924ecb8d",
# META       "default_lakehouse_name": "lh_ops",
# META       "default_lakehouse_workspace_id": "16c5b45f-b48f-449f-ad26-8fbd79f55c3f"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_sim_refresh_health (WP8, UC3 test)
# Writes synthetic rows straight into `kql_ops.sig_refresh_health` to test `oa_refresh_health`, using the same row
# shape `nb_build_signals` appends. Thresholds come from `refresh` in `config/thresholds.yaml`.
# - `FAILING_MODELS`: `ConsecutiveFailures` = threshold. Agent should alert and recommend a retry.
# - `ESCALATED_MODELS`: `ConsecutiveFailures` = `ESCALATED_FAILURES` (above 4). Agent should recommend disabling
#   the schedule and opening a ticket.
# - `SLOW_MODELS`: `SignalType = SlowRefresh` with `DurationVsBaselinePct` = `SLOW_PCT`. Agent should recommend a
#   ticket only.
# - `CONTROL_ROWS`: near misses (threshold-1 failures, 150%) that must **not** alert (negative test).
# - Model IDs are random GUIDs, so approved retry/disable actions run as dry runs against nothing real.
# - Safety: DEV only, workspace name starts with `TEST-`, nothing is deleted. `DRY_RUN = True` previews only.
# - Needs `Files/fuam_ops` in `lh_ops` (`deploy/upload_package.py`).

# PARAMETERS CELL ********************

ENV = "dev"
DRY_RUN = False
OWNER_UPN = ""                 # empty = agent creator from config
FAILING_MODELS = 1
ESCALATED_MODELS = 1
ESCALATED_FAILURES = 5
SLOW_MODELS = 1
SLOW_PCT = 250.0
BASELINE_MIN = 10.0
CONTROL_ROWS = False
WORKSPACE_NAME = "TEST-refresh-ws"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys, time, uuid
from datetime import datetime, timezone
sys.path.insert(0, "/lakehouse/default/Files/fuam_ops/src")

from fuam_ops.config import load_env, load_thresholds
from fuam_ops.kusto_rest import KustoRest
from fuam_ops import fabric_io as io
from fuam_ops import simulation as sim

if ENV != "dev":
    raise ValueError("Simulators only run in DEV")
cfg = load_env(ENV)
th = load_thresholds(env_cfg=cfg)
owner = OWNER_UPN or cfg["agent"]["creator_upn"]
now = datetime.now(timezone.utc).replace(microsecond=0)
run_id = str(uuid.uuid4())

TABLE = "sig_refresh_health"
s = sim.RefreshScenario(owner_upn=owner, failing_models=int(FAILING_MODELS), escalated_models=int(ESCALATED_MODELS),
                        escalated_failures=int(ESCALATED_FAILURES), slow_models=int(SLOW_MODELS),
                        slow_pct=float(SLOW_PCT), baseline_min=float(BASELINE_MIN),
                        control_rows=bool(CONTROL_ROWS), workspace_name=WORKSPACE_NAME)
rows = sim.refresh_rows(s, now, th["refresh"], run_id)
print(f"{len(rows)} rows -> expect {sim.expected_refresh_alerts(rows, th['refresh'])} alert(s) from oa_refresh_health "
      f"(thresholds: {th['refresh']['consecutive_failures']} failures, {th['refresh']['duration_vs_baseline_pct']}%)")
for r in rows:
    print(r["SemanticModelName"], r["SignalType"], r["ConsecutiveFailures"], r["DurationVsBaselinePct"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DRY_RUN:
    print("DRY_RUN = True: nothing written")
else:
    uri = cfg["ops"]["eventhouse_query_uri"]
    k = KustoRest(uri, cfg["ops"]["kql_database"], lambda: io.token(uri))
    sim.write_signal_rows(k, TABLE, rows)
    print(f"wrote {len(rows)} rows to {TABLE} (SnapshotUtc {now.isoformat()}, run {run_id})")
    time.sleep(5)
    print(sim.verify_signal_rows(k, TABLE, now))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
