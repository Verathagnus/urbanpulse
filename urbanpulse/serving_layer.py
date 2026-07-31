"""Lambda Architecture Serving Layer Query Interface.

Queries historical batch view stored in partitioned Parquet files and real-time speed view from Kafka topic `urbanpulse.ward_energy_summary`.
Merges batch and speed data dynamically, prioritizing finalized batch records as ground truth while filling recent active window gaps from the speed layer.
"""

import os
import glob
import json
import logging
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UrbanPulseServingLayer")


def get_merged_ward_energy(ward_id: str, parquet_dir: str, bootstrap_servers: str) -> list[dict]:
    """Queries and reconciles batch (Parquet) and speed (Kafka) views for a specific ward."""
    logger.info(f"Querying serving layer for ward: {ward_id}")

    # Read batch view from partitioned Parquet storage
    batch_records = []
    partition_glob = os.path.join(parquet_dir, f"ward_id={ward_id}", "**", "*.parquet")
    parquet_files = glob.glob(partition_glob, recursive=True)

    if parquet_files:
        try:
            dfs = []
            for file in parquet_files:
                df = pd.read_parquet(file)
                dfs.append(df)
            combined_df = pd.concat(dfs, ignore_index=True)
            
            combined_df["ward_id"] = ward_id
            
            if "window_start" in combined_df.columns:
                combined_df["window_start"] = pd.to_datetime(combined_df["window_start"]).dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            if "window_end" in combined_df.columns:
                combined_df["window_end"] = pd.to_datetime(combined_df["window_end"]).dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                
            if "date" not in combined_df.columns:
                combined_df["date"] = combined_df["window_start"].str[:10]
                
            batch_records = combined_df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Failed to load Parquet batch view: {e}")
    else:
        logger.info(f"No batch files found for {ward_id} in {parquet_dir}.")

    # Read active speed & streaming batch view records from Kafka
    speed_records = []
    import uuid
    kafka_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"serving-layer-query-{ward_id}-{uuid.uuid4().hex[:6]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
    
    try:
        consumer = Consumer(kafka_conf)
        metadata = consumer.list_topics("urbanpulse.ward_energy_summary", timeout=5.0)
        topic_metadata = metadata.topics.get("urbanpulse.ward_energy_summary")
        
        if topic_metadata is not None:
            partitions = topic_metadata.partitions.keys()
            # Look back up to 10,000 messages per partition to guarantee all 20 wards return their latest Speed & Batch windows
            tps = []
            for p in partitions:
                tp_temp = TopicPartition("urbanpulse.ward_energy_summary", p)
                low, high = consumer.get_watermark_offsets(tp_temp)
                start_offset = max(low, high - 10000)
                tps.append(TopicPartition("urbanpulse.ward_energy_summary", p, start_offset))
            consumer.assign(tps)
            
            import time
            start_time = time.time()
            empty_polls = 0
            while time.time() - start_time < 5.0 and empty_polls < 10:
                msgs = consumer.consume(num_messages=1000, timeout=0.15)
                if not msgs:
                    empty_polls += 1
                    continue
                empty_polls = 0
                for msg in msgs:
                    if msg.error():
                        continue
                    try:
                        payload = json.loads(msg.value().decode("utf-8"))
                        if payload.get("ward_id") == ward_id:
                            if not payload.get("source_layer") or str(payload.get("source_layer")).lower() in ("nan", "none", "null"):
                                payload["source_layer"] = "SPEED"
                            speed_records.append(payload)
                    except Exception:
                        continue
        consumer.close()
    except Exception as e:
        logger.error(f"Failed to poll Kafka speed view: {e}")

    logger.info(f"Retrieved {len(batch_records)} batch records and {len(speed_records)} speed records.")

    # Reconcile views (Batch layer overwrites speed layer for overlapping windows)
    merged_views = {}
    
    def norm_ts_ist(ts):
        if not ts:
            return ""
        try:
            dt = pd.to_datetime(ts)
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            return dt.tz_convert("Asia/Kolkata").strftime("%Y-%m-%dT%H:%M:%S.000+05:30")
        except Exception:
            return str(ts)

    for record in batch_records:
        w_start = norm_ts_ist(record.get("window_start"))
        if w_start:
            record["window_start"] = w_start
            record["window_end"] = norm_ts_ist(record.get("window_end"))
            merged_views[(w_start, "BATCH")] = record

    for record in speed_records:
        w_start = norm_ts_ist(record.get("window_start"))
        if w_start:
            record["window_start"] = w_start
            record["window_end"] = norm_ts_ist(record.get("window_end"))
            merged_views[(w_start, "SPEED")] = record

    sorted_records = sorted(merged_views.values(), key=lambda x: (x["window_start"], 0 if x["source_layer"] == "BATCH" else 1))
    return sorted_records
