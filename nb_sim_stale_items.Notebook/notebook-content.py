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

# # nb_sim_stale_items (WP8, UC2 test)
# Writes synthetic rows straight into `kql_ops.sig_stale_items` to test `oa_stale_items`, using the same row shape
# `nb_build_signals` appends. Each row is one (workspace, owner) group of items that newly passed
# `stale_items.days_inactive` (`config/thresholds.yaml`, with the env override).
# - Expected: **one notification per row** (rule "NewlyStaleCount is above 0").
# - `ItemIds` are random GUIDs, so an approved tag action (dry run) gets rejected harmlessly or logs a dry run.
# - `GROUPS = 50` reproduces the WP8 load test (message volume / throttling).
# - Safety: DEV only, workspace names start with `TEST-`, nothing is deleted. `DRY_RUN = True` previews only.
# - Needs `Files/fuam_ops` in `lh_ops` (`deploy/upload_package.py`).

# PARAMETERS CELL ********************

ENV = "dev"
DRY_RUN = False
OWNER_UPN = ""                 # empty = agent creator from config
GROUPS = 1                     # rows to write (one per workspace/owner)
ITEMS_PER_GROUP = 3
OLDEST_DAYS_INACTIVE = 120
ITEM_TYPES = "Report,SemanticModel"
WORKSPACE_PREFIX = "TEST-stale-ws"

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

TABLE = "sig_stale_items"
s = sim.StaleScenario(owner_upn=owner, groups=int(GROUPS), items_per_group=int(ITEMS_PER_GROUP),
                      oldest_days_inactive=int(OLDEST_DAYS_INACTIVE),
                      item_types=tuple(t.strip() for t in ITEM_TYPES.split(",") if t.strip()),
                      workspace_prefix=WORKSPACE_PREFIX)
rows = sim.stale_rows(s, now, th["stale_items"], run_id)
print(f"{len(rows)} rows -> expect {len(rows)} notification(s) from oa_stale_items "
      f"(days_inactive threshold {th['stale_items']['days_inactive']})")
for r in rows:
    print(r["WorkspaceName"], r["OwnerUpn"], r["NewlyStaleCount"], r["OldestDaysInactive"], r["ItemList"][:80])

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
