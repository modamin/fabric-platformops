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

# # nb_act_pause_schedules (UC1)
# Turns off schedules of the top background CU consumers on an allow-listed capacity. Consumers come from
# `sig_capacity_operations` (real-time capacity operation events, ADR-005): background operations whose smoothing
# window has not ended yet still add load now. The previous schedule is stored in the audit row so it can be re-enabled.
# Called by an operations agent `FabricJobAction`. Parameter names match the action's `parameters`
# (VERIFY plan Q1: how agent parameters reach the parameter cell; test in WP8 step 3).

# PARAMETERS CELL ********************

ENV = "dev"
CapacityId = ""
MAX_ITEMS = 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys, json
sys.path.insert(0, "/lakehouse/default/Files/fuam_ops/src")

from fuam_ops.config import load_env
from fuam_ops import fabric_io as io
from fuam_ops.actions import runners
from fuam_ops.actions.audit import AuditRecord, to_frame
from fuam_ops.actions.rest import RestClient
from fuam_ops.actions.validation import is_guid
from fuam_ops.actions.fabric_api import FabricApi, FABRIC_BASE
from fuam_ops.actions.powerbi_api import PowerBiApi, POWERBI_BASE

cfg = load_env(ENV)
safety = cfg["safety"]  # DRY_RUN is decided by safety.live_actions only, never by an agent parameter
fabric = FabricApi(RestClient(FABRIC_BASE, lambda: io.token("https://api.fabric.microsoft.com")))
pbi = PowerBiApi(RestClient(POWERBI_BASE, lambda: io.token("https://analysis.windows.net/powerbi/api")))

ACTION = "pause_schedules"
PARAMS = {"CapacityId": CapacityId}


def run():
    top = []
    if is_guid(CapacityId):  # validated before it is used in a query
        cap = CapacityId.strip().lower()
        # Single expression: the Kusto Spark connector rejects multi-statement (`let ...;`) queries.
        q = (f"sig_capacity_operations "
             f"| where CapacityId == '{cap}' and WindowEndUtc > now() "
             f"and UtilizationType == 'Background' and isnotempty(ItemId) "
             f"| summarize take_any(WorkspaceId, ItemId, ItemName, ItemKind, CapacityUnitMs) by OperationId "
             f"| summarize CapacityUnitMs = sum(CapacityUnitMs), ItemName = take_any(ItemName), "
             f"ItemKind = take_any(ItemKind) by WorkspaceId, ItemId "
             f"| order by CapacityUnitMs desc | take 20")
        top = io.read_kql(spark, q, cfg).to_dict("records")
    return runners.pause_schedules(fabric, safety, CapacityId, top, int(MAX_ITEMS))


try:
    records = run()
except Exception as exc:  # unexpected failure: still audited
    records = [AuditRecord(ACTION, True, "", PARAMS, f"Error: {type(exc).__name__}: {exc}")]
io.write_kql(spark, to_frame(records), "ops_action_audit", cfg)
summary = [{"result": r.result, "target": r.target, "dryRun": r.dry_run, "correlationId": r.correlation_id} for r in records]
print(json.dumps(summary, indent=2))
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
