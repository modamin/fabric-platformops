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

# # nb_sim_capacity_throttling (WP8, UC1 test)
# Simulates a capacity throttling event by writing rows straight into `kql_ops`, to test `oa_capacity_overload`.
# - Profile: baseline → ramp → peak → recover, in 30-second windows, plus Overloaded / Active state events.
# - `VIA = "raw"` (default): events go into `raw_capacity_events`, and the update policy flattens them into
#   `sig_capacity_health` / `sig_capacity_state`. That's the path real Real-Time hub events take. `VIA = "sig"`
#   writes the flattened tables directly.
# - `INTERVAL_SECONDS = 0`: one batch dated to end now. Above 0: one window every N seconds. The agent polls every
#   5 minutes, so use 30 or more to spread the event over several polls.
# - Safety: DEV only, capacity name must start with `TEST-`, nothing is ever deleted (rows age out with retention).
# - `DRY_RUN = True` prints the scenario without writing.
# Same logic as `tests/e2e/simulate_capacity_throttling.py` (`fuam_ops.simulation`).
# Needs `Files/fuam_ops` in `lh_ops` (`deploy/upload_package.py`).


# PARAMETERS CELL ********************

ENV = "dev"
DRY_RUN = False
CAPACITY_ID = "ffffffff-0000-0000-0000-00000000c0f1"
CAPACITY_NAME = "TEST-throttle-sim"
SKU = "F2"
PEAK_INTERACTIVE_PCT = 120.0    # InteractiveDelayPct at the peak (>100 = throttled)
PEAK_BACKGROUND_PCT = 95.0      # BackgroundRejectionPct at the peak
BASELINE_WINDOWS = 6
RAMP_WINDOWS = 6
PEAK_WINDOWS = 10
RECOVER_WINDOWS = 6
WITH_STATE_EVENTS = True
VIA = "raw"                     # "raw" or "sig"
INTERVAL_SECONDS = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys, time
from datetime import datetime, timezone
sys.path.insert(0, "/lakehouse/default/Files/fuam_ops/src")

from fuam_ops.config import load_env, load_thresholds
from fuam_ops.kusto_rest import KustoRest
from fuam_ops import fabric_io as io
from fuam_ops.simulation import WINDOW, Scenario, run, state_transitions, summary_line, verify, windows

if ENV != "dev":
    raise ValueError("The throttling simulator only runs in DEV")
if not CAPACITY_NAME.startswith("TEST-"):
    raise ValueError("CAPACITY_NAME must start with 'TEST-' so simulated rows can be identified")
if VIA not in ("raw", "sig"):
    raise ValueError("VIA must be 'raw' or 'sig'")

cfg = load_env(ENV)
cap_th = load_thresholds(env_cfg=cfg)["capacity"]
s = Scenario(capacity_id=CAPACITY_ID, capacity_name=CAPACITY_NAME, sku=SKU,
             baseline_windows=int(BASELINE_WINDOWS), ramp_windows=int(RAMP_WINDOWS),
             peak_windows=int(PEAK_WINDOWS), recover_windows=int(RECOVER_WINDOWS),
             peak_interactive=float(PEAK_INTERACTIVE_PCT), peak_background=float(PEAK_BACKGROUND_PCT),
             with_state_events=bool(WITH_STATE_EVENTS))

preview = windows(s, datetime.now(timezone.utc).replace(microsecond=0) - s.total_windows * WINDOW)
print(summary_line(s, preview, state_transitions(s, preview), cap_th))
for r in preview:
    print(r["WindowStartUtc"].strftime("%H:%M:%S"), f"delay={r['InteractiveDelayPct']:>6}",
          f"bg={r['BackgroundRejectionPct']:>6}", f"util={r['UtilizationPct']:>6}")

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
    rows, states = run(k, s, via=VIA, interval=float(INTERVAL_SECONDS))
    print(f"wrote {len(rows)} windows and {len(states)} state events via {VIA}")
    time.sleep(5)
    for table, result in verify(k, CAPACITY_NAME).items():
        print(f"{table}: {result}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
