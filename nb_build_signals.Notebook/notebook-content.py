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

# # nb_build_signals
# Daily FUAM-based signals (plan WP4): stale items (UC2) and refresh health (UC3).
# Reads `fuam_*` shortcuts in `lh_ops`, updates state tables and appends event rows to `kql_ops`.
# The capacity signal (UC1) is real-time and is not built here: eventstream `es_capacity_events` writes capacity
# events straight to `kql_ops` (ADR-005), because FUAM data is only refreshed daily.
# Signal construction lives in `fuam_ops.signals.spark`; pandas equivalents are retained for unit tests.
# Requires `Files/fuam_ops/{src,config}` in `lh_ops` (uploaded by `deploy/upload_package.py`).

# PARAMETERS CELL ********************

ENV = "dev"
RUN_STALE = True
RUN_REFRESH = True
NOW_OVERRIDE = ""   # ISO timestamp for replays/tests; empty = now

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sys, traceback
from datetime import timedelta
sys.path.insert(0, "/lakehouse/default/Files/fuam_ops/src")

from pyspark.sql import functions as F

from fuam_ops.config import ColumnMap, load_env, load_thresholds
from fuam_ops import fabric_io as io
from fuam_ops.signals import heartbeat as hb
from fuam_ops.signals import spark as signals

cfg = load_env(ENV)
th = load_thresholds(env_cfg=cfg)
cmap = ColumnMap.load()
now = signals.parse_utc(NOW_OVERRIDE) if NOW_OVERRIDE else signals.utc_now()
if cmap.status != "confirmed" and ENV != "dev":
    raise RuntimeError("config/fuam_column_map.yaml is still 'draft'; finish WP0 before running outside DEV")

# WP1 task 4: state tables (idempotent).
spark.sql("CREATE TABLE IF NOT EXISTS ops_item_last_activity (item_id STRING, last_activity_utc TIMESTAMP) USING DELTA")
spark.sql("CREATE TABLE IF NOT EXISTS ops_stale_state (item_id STRING, status STRING, stale_since_utc TIMESTAMP, updated_utc TIMESTAMP) USING DELTA")
spark.sql("CREATE TABLE IF NOT EXISTS ops_refresh_state (model_id STRING, flagged_failure_streak_start_utc TIMESTAMP, last_slow_flagged_start_utc TIMESTAMP, updated_utc TIMESTAMP) USING DELTA")

def fuam(logical_table):
    return spark.read.table(f"fuam_{cmap.table(logical_table)}")

def select_logical(sdf, logical_table, required):
    """Select + rename mapped columns in Spark (fails fast on missing required columns, risk R1)."""
    cols = cmap.columns(logical_table)
    lookup = {c.lower(): c for c in sdf.columns}
    exprs = []
    for logical, phys in cols.items():
        actual = lookup.get(phys.lower()) if phys else None
        if actual is None:
            if logical in required:
                raise RuntimeError(f"FUAM column for {logical_table}.{logical} ({phys}) not found; re-run WP0")
            exprs.append(F.lit(None).cast("string").alias(logical))
        else:
            exprs.append(F.col(f"`{actual}`").alias(logical))
    return sdf.select(*exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

counts = {"stale": 0, "refresh": 0}
status, message = hb.STATUS_OK, ""
fuam_latest = None
try:
    load_times = [io.last_write_utc(spark, f"fuam_{cmap.table(t)}") for t in ("active_items", "activities", "refresh_details")]
    load_times = [t for t in load_times if t is not None]
    fuam_latest = max(load_times) if load_times else None
    if not hb.fuam_is_fresh(fuam_latest, now, cfg["fuam"].get("max_data_age_hours", 30)):
        status, message = hb.STATUS_FUAM_STALE, f"FUAM latest load {fuam_latest}; no signals emitted"
    else:
        # Lookups shared by all signals (FUAM lacks names/owners on some tables; see fuam_column_map.yaml).
        items_all = select_logical(fuam("active_items"), "active_items",
                                   required=["id", "name", "type", "workspace_id", "last_updated", "deleted_flag"])
        wss = select_logical(fuam("workspaces"), "workspaces", required=["id", "name"])

        # ---- 4b: UC2 stale items ----
        if RUN_STALE:
            st = th["stale_items"]
            prev_la = io.read_delta_spark(spark, "ops_item_last_activity")
            prev_la = prev_la if prev_la is not None else signals.empty_last_activity(spark)
            act = select_logical(fuam("activities"), "activities", required=["activity_time", "artifact_id", "activity_name"])
            act = act.withColumn("activity_time", signals.safe_timestamp("activity_time"))
            latest_activity = prev_la.agg(F.max("last_activity_utc").alias("latest")).first()["latest"]
            if latest_activity is not None:
                # 2-day overlap: max() is idempotent and late-arriving activity is still picked up.
                watermark = latest_activity.replace(tzinfo=None) - timedelta(days=2)
                act = act.where(F.col("activity_time") >= F.lit(watermark))
            use = [a.lower() for a in st["use_activities"]]
            new_la = (act.where(F.lower(F.col("activity_name")).isin(use) & F.col("artifact_id").isNotNull())
                         .groupBy(F.lower(F.col("artifact_id")).alias("item_id"))
                         .agg(F.max("activity_time").alias("last_activity_utc")))
            la = signals.merge_last_activity(prev_la, new_la)

            state = io.read_delta_spark(spark, "ops_stale_state")
            rows, new_state = signals.build_stale_signals(
                items_all, wss, la, state if state is not None else signals.empty_stale_state(spark),
                st, cfg["safety"], now)
            # KQL first, then state: a failure in between re-emits (duplicate) rather than losing signals.
            counts["stale"] = io.write_kql_spark(spark, rows, "sig_stale_items", cfg)
            io.overwrite_delta_spark(spark, la, "ops_item_last_activity")
            io.overwrite_delta_spark(spark, new_state, "ops_stale_state")

        # ---- 4c: UC3 refresh health ----
        if RUN_REFRESH:
            rt = th["refresh"]
            ref = select_logical(fuam("refresh_details"), "refresh_details",
                                 required=["model_id", "start_time", "end_time", "status"])
            lookback = int(rt["baseline_days"]) + 16
            ref = ref.where(signals.safe_timestamp("start_time") >= F.lit(now) - F.expr(f"INTERVAL {lookback} DAYS"))
            ref = signals.enrich_refreshes(ref, items_all, wss)
            state = io.read_delta_spark(spark, "ops_refresh_state")
            rows, new_state = signals.build_refresh_signals(
                ref, state if state is not None else signals.empty_refresh_state(spark), rt, now)
            counts["refresh"] = io.write_kql_spark(spark, rows, "sig_refresh_health", cfg)
            io.overwrite_delta_spark(spark, new_state, "ops_refresh_state")
except Exception as exc:
    status, message = hb.STATUS_FAILED, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-600:]}"
    raise
finally:
    heartbeat_latest = fuam_latest.to_pydatetime() if fuam_latest is not None else None
    heartbeat = signals.heartbeat_row(
        spark, cfg["env"], now, heartbeat_latest, counts["stale"], counts["refresh"], status, message)
    io.write_kql_spark(spark, heartbeat, "ops_heartbeat", cfg)
    print(status, counts, message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
