"""PyFlink Complex Event Processing & Incident Detection Engine.

Consumes heterogeneous stream topics via PyFlink DataStream API using event-time watermarking:
  - `urbanpulse.air_quality` -> Detects AQI Emergency (AQI > 300) with 2-min cooldown windowing
  - `urbanpulse.traffic_signals` -> Detects Traffic Gridlock (avg_wait > 180s over 3 consecutive cycles)
  - `urbanpulse.enriched_bus_gps` -> Detects Bus Bunching (pairs < 200m for > 5 mins)
  - `urbanpulse.smart_meters` -> Computes 15-min tumbling window speed view rollups
Publishes generated alert payloads to `urbanpulse.incidents` topic.
"""

import os
import sys
import json
import math
import logging
from datetime import datetime, timezone
from typing import Tuple

from pyflink.common import Types, WatermarkStrategy, Duration, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink,
    KafkaRecordSerializationSchema
)

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')

AQI_TOPIC = 'urbanpulse.air_quality'
TRAFFIC_TOPIC = 'urbanpulse.traffic_signals'
BUS_GPS_TOPIC = 'urbanpulse.enriched_bus_gps'
INCIDENTS_TOPIC = 'urbanpulse.incidents'

AQI_EMERGENCY_THRESHOLD = 300
GRIDLOCK_WAIT_THRESHOLD = 180
GRIDLOCK_CONSECUTIVE_CYCLES = 3
BUS_BUNCHING_DISTANCE_M = 200
BUS_BUNCHING_DURATION_SEC = 300

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logging.getLogger("apache_beam").setLevel(logging.WARNING)
logger = logging.getLogger("FlinkIncidentDetection")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Computes spherical geodesic distance between two spatial coordinates in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


class EventTimestampAssigner(TimestampAssigner):
    """Extracts event timestamps from JSON string records for event-time processing."""
    def extract_timestamp(self, value, record_timestamp) -> int:
        try:
            event = json.loads(value)
            ts_str = event.get('timestamp', '')
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


class AQIEmergencyDetector(KeyedProcessFunction):
    """Evaluates individual sensor AQI streams against hazardous threshold with stateful alert throttling."""

    def __init__(self):
        self.last_alert_time = None
        self.COOLDOWN_MS = 120000

    def open(self, runtime_context: RuntimeContext):
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor("last_aqi_alert_time", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            return

        aqi = event.get('aqi')
        if aqi is None or not isinstance(aqi, (int, float)):
            return

        if aqi > AQI_EMERGENCY_THRESHOLD:
            current_time = ctx.timestamp()
            last_alert = self.last_alert_time.value()

            if last_alert is None or (current_time - last_alert) > self.COOLDOWN_MS:
                alert = {
                    "incident_type": "AQI_EMERGENCY",
                    "severity": "CRITICAL",
                    "sensor_id": event.get('sensor_id'),
                    "zone": event.get('zone'),
                    "aqi_value": aqi,
                    "pm25": event.get('pm25'),
                    "pm10": event.get('pm10'),
                    "no2": event.get('no2'),
                    "threshold": AQI_EMERGENCY_THRESHOLD,
                    "event_timestamp": event.get('timestamp'),
                    "alert_timestamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "description": (
                        f"HAZARDOUS AQI ({aqi:.0f}) detected at sensor "
                        f"{event.get('sensor_id')} in {event.get('zone')}."
                    )
                }
                self.last_alert_time.update(current_time)
                yield json.dumps(alert)

                logger.warning(
                    f"AQI EMERGENCY: sensor={event.get('sensor_id')}, "
                    f"zone={event.get('zone')}, aqi={aqi}")


class TrafficGridlockDetector(KeyedProcessFunction):
    """Tracks sliding window of junction wait cycles using ListState to detect sustained congestion."""

    def __init__(self):
        self.wait_times = None
        self.last_alert_time = None
        self.COOLDOWN_MS = 300000

    def open(self, runtime_context: RuntimeContext):
        self.wait_times = runtime_context.get_list_state(
            ListStateDescriptor("last_wait_times", Types.FLOAT())
        )
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor("last_gridlock_alert_time", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            return

        avg_wait = event.get('avg_wait_sec', 0)

        current_waits = list(self.wait_times.get())
        current_waits.append(float(avg_wait))

        if len(current_waits) > GRIDLOCK_CONSECUTIVE_CYCLES:
            current_waits = current_waits[-GRIDLOCK_CONSECUTIVE_CYCLES:]

        self.wait_times.clear()
        for w in current_waits:
            self.wait_times.add(w)

        if len(current_waits) >= GRIDLOCK_CONSECUTIVE_CYCLES:
            all_above = all(
                w > GRIDLOCK_WAIT_THRESHOLD for w in current_waits
            )

            if all_above:
                current_time = ctx.timestamp()
                last_alert = self.last_alert_time.value()

                if last_alert is None or (current_time - last_alert) > self.COOLDOWN_MS:
                    avg_of_waits = sum(current_waits) / len(current_waits)

                    alert = {
                        "incident_type": "TRAFFIC_GRIDLOCK",
                        "severity": "HIGH",
                        "junction_id": event.get('junction_id'),
                        "zone": event.get('zone'),
                        "consecutive_cycles": GRIDLOCK_CONSECUTIVE_CYCLES,
                        "wait_times": current_waits,
                        "average_wait_sec": round(avg_of_waits, 1),
                        "threshold": GRIDLOCK_WAIT_THRESHOLD,
                        "vehicle_count": event.get('vehicle_count'),
                        "signal_phase": event.get('signal_phase'),
                        "event_timestamp": event.get('timestamp'),
                        "alert_timestamp": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                        "description": (
                            f"GRIDLOCK at junction {event.get('junction_id')} "
                            f"in {event.get('zone')}. Average wait "
                            f"{avg_of_waits:.0f}s over {GRIDLOCK_CONSECUTIVE_CYCLES} cycles."
                        )
                    }
                    self.last_alert_time.update(current_time)
                    yield json.dumps(alert)

                    logger.warning(
                        f"GRIDLOCK DETECTED: junction={event.get('junction_id')}, "
                        f"zone={event.get('zone')}, "
                        f"avg_wait={avg_of_waits:.0f}s")


class BusBunchingDetector(KeyedProcessFunction):
    """Tracks intra-route vehicle proximity in MapState to identify temporal bus clustering."""

    def __init__(self):
        self.bus_positions = None
        self.bunching_pairs = None
        self.last_alert_time = None

    def open(self, runtime_context: RuntimeContext):
        self.bus_positions = runtime_context.get_map_state(
            MapStateDescriptor("bus_positions", Types.STRING(), Types.STRING())
        )
        self.bunching_pairs = runtime_context.get_map_state(
            MapStateDescriptor("bunching_pairs", Types.STRING(), Types.LONG())
        )
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor("last_bunching_alert_time", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            return

        bus_id = event.get('bus_id')
        lat = event.get('lat')
        lon = event.get('lon')
        current_time = ctx.timestamp()

        if not all([bus_id, lat, lon]):
            return

        position_data = json.dumps({
            "lat": lat, "lon": lon, "timestamp": current_time
        })
        self.bus_positions.put(bus_id, position_data)

        try:
            all_entries = list(self.bus_positions.entries())
        except Exception:
            return

        for entry in all_entries:
            other_bus_id = entry.getKey()
            if other_bus_id == bus_id:
                continue

            try:
                other_pos = json.loads(entry.getValue())
            except (json.JSONDecodeError, TypeError):
                continue

            other_lat = other_pos.get('lat')
            other_lon = other_pos.get('lon')

            if other_lat is None or other_lon is None:
                continue

            distance_m = haversine_distance(lat, lon, other_lat, other_lon)
            pair_key = "|".join(sorted([bus_id, other_bus_id]))

            if distance_m < BUS_BUNCHING_DISTANCE_M:
                try:
                    first_seen = self.bunching_pairs.get(pair_key)
                except Exception:
                    first_seen = None

                if first_seen is None:
                    self.bunching_pairs.put(pair_key, current_time)
                else:
                    duration_sec = (current_time - first_seen) / 1000.0

                    if duration_sec > BUS_BUNCHING_DURATION_SEC:
                        last_alert = self.last_alert_time.value()
                        cooldown = 600000

                        if last_alert is None or (current_time - last_alert) > cooldown:
                            bus_ids = pair_key.split("|")
                            alert = {
                                "incident_type": "BUS_BUNCHING",
                                "severity": "MEDIUM",
                                "route_id": event.get('route_id'),
                                "bus_id_1": bus_ids[0],
                                "bus_id_2": bus_ids[1],
                                "distance_metres": round(distance_m, 1),
                                "duration_seconds": round(duration_sec, 0),
                                "threshold_distance_m": BUS_BUNCHING_DISTANCE_M,
                                "threshold_duration_sec": BUS_BUNCHING_DURATION_SEC,
                                "bus_1_position": {"lat": lat, "lon": lon},
                                "bus_2_position": {"lat": other_lat, "lon": other_lon},
                                "event_timestamp": event.get('timestamp'),
                                "alert_timestamp": datetime.now(timezone.utc).strftime(
                                    "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                                "description": (
                                    f"BUS BUNCHING on route {event.get('route_id')}: "
                                    f"{bus_ids[0]} and {bus_ids[1]} are {distance_m:.0f}m apart."
                                )
                            }
                            self.last_alert_time.update(current_time)
                            self.bunching_pairs.remove(pair_key)
                            yield json.dumps(alert)

                            logger.warning(
                                f"BUS BUNCHING: route={event.get('route_id')}, "
                                f"buses={bus_ids}, distance={distance_m:.0f}m")
            else:
                try:
                    if self.bunching_pairs.contains(pair_key):
                        self.bunching_pairs.remove(pair_key)
                except Exception:
                    pass


class WardEnergySpeedAggregator(KeyedProcessFunction):
    """Computes real-time 15-minute tumbling aggregations representing the speed layer view."""

    def __init__(self):
        self.windows_state = None

    def open(self, runtime_context: RuntimeContext):
        self.windows_state = runtime_context.get_map_state(
            MapStateDescriptor("ward_energy_windows", Types.LONG(), Types.STRING())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            return

        ward_id = event.get('ward_id')
        kwh = event.get('kwh_reading')
        pf = event.get('power_factor')
        voltage = event.get('voltage')
        ts_str = event.get('timestamp')

        if not all(x is not None for x in [ward_id, kwh, pf, voltage, ts_str]):
            return

        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            event_time = int(dt.timestamp() * 1000)
        except Exception:
            return

        window_start = event_time - (event_time % 900000)
        window_end = window_start + 900000

        state_str = self.windows_state.get(window_start)
        if state_str is None:
            state = {
                "min_kwh": float(kwh),
                "max_kwh": float(kwh),
                "sum_pf": float(pf),
                "count_pf": 1,
                "max_v": float(voltage)
            }
            ctx.timer_service().register_event_time_timer(window_end)
        else:
            state = json.loads(state_str)
            state["min_kwh"] = min(state["min_kwh"], float(kwh))
            state["max_kwh"] = max(state["max_kwh"], float(kwh))
            state["sum_pf"] += float(pf)
            state["count_pf"] += 1
            state["max_v"] = max(state["max_v"], float(voltage))

        self.windows_state.put(window_start, json.dumps(state))

        total_kwh = state["max_kwh"] - state["min_kwh"]
        avg_pf = state["sum_pf"] / state["count_pf"] if state["count_pf"] > 0 else 0.0
        peak_v = state["max_v"]

        window_start_dt = datetime.fromtimestamp(window_start / 1000.0, tz=timezone.utc)
        window_end_dt = datetime.fromtimestamp(window_end / 1000.0, tz=timezone.utc)

        output = {
            "window_start": window_start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "window_end": window_end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "ward_id": ward_id,
            "date": window_start_dt.strftime("%Y-%m-%d"),
            "total_kwh_consumed": float(total_kwh),
            "avg_power_factor": float(avg_pf),
            "peak_voltage": float(peak_v),
            "source_layer": "SPEED"
        }
        yield json.dumps(output)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        window_end = timestamp
        window_start = window_end - 900000

        state_str = self.windows_state.get(window_start)
        if state_str is not None:
            state = json.loads(state_str)
            total_kwh = state["max_kwh"] - state["min_kwh"]
            avg_pf = state["sum_pf"] / state["count_pf"] if state["count_pf"] > 0 else 0.0
            peak_v = state["max_v"]

            window_start_dt = datetime.fromtimestamp(window_start / 1000.0, tz=timezone.utc)
            window_end_dt = datetime.fromtimestamp(window_end / 1000.0, tz=timezone.utc)

            output = {
                "window_start": window_start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "window_end": window_end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "ward_id": ctx.get_current_key(),
                "date": window_start_dt.strftime("%Y-%m-%d"),
                "total_kwh_consumed": float(total_kwh),
                "avg_power_factor": float(avg_pf),
                "peak_voltage": float(peak_v),
                "source_layer": "SPEED"
            }

            yield json.dumps(output)
            self.windows_state.remove(window_start)


def build_and_run():
    logger.info("Initializing Flink Incident Detection Environment")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(4)
    env.enable_checkpointing(300000)

    lib_dir = os.path.join(os.path.dirname(__file__), 'lib')
    os.makedirs(lib_dir, exist_ok=True)
    kafka_jar = os.path.join(lib_dir, 'flink-sql-connector-kafka-3.0.2-1.18.jar')
    if not os.path.exists(kafka_jar):
        try:
            import urllib.request
            url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar"
            urllib.request.urlretrieve(url, kafka_jar)
        except Exception as e:
            logger.error(f"Failed to fetch connector JAR: {e}")

    if os.path.exists(kafka_jar):
        jar_path = kafka_jar.replace('\\', '/')
        jar_url = f"file:///{jar_path}" if sys.platform == 'win32' else f"file://{kafka_jar}"
        env.add_jars(jar_url)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
        .with_idleness(Duration.of_minutes(1))
    )

    aqi_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(AQI_TOPIC)
        .set_group_id('flink-aqi-emergency-detector')
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    traffic_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TRAFFIC_TOPIC)
        .set_group_id('flink-gridlock-detector')
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    bus_gps_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(BUS_GPS_TOPIC)
        .set_group_id('flink-bus-bunching-detector')
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    smart_meters_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics('urbanpulse.smart_meters')
        .set_group_id('flink-ward-energy-speed-detector')
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    incidents_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(INCIDENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    ward_energy_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic('urbanpulse.ward_energy_summary')
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    aqi_stream = env.from_source(aqi_source, watermark_strategy, "AQI Source").name("AQI Air Quality Stream")
    aqi_alerts = (
        aqi_stream
        .key_by(lambda v: json.loads(v).get('sensor_id', 'unknown'), key_type=Types.STRING())
        .process(AQIEmergencyDetector(), output_type=Types.STRING())
        .name("AQI Emergency Detector")
    )

    traffic_stream = env.from_source(traffic_source, watermark_strategy, "Traffic Source").name("Traffic Signal Stream")
    gridlock_alerts = (
        traffic_stream
        .key_by(lambda v: json.loads(v).get('junction_id', 'unknown'), key_type=Types.STRING())
        .process(TrafficGridlockDetector(), output_type=Types.STRING())
        .name("Traffic Gridlock Detector")
    )

    bus_stream = env.from_source(bus_gps_source, watermark_strategy, "Bus GPS Source").name("Enriched Bus GPS Stream")
    bunching_alerts = (
        bus_stream
        .key_by(lambda v: json.loads(v).get('route_id', 'unknown'), key_type=Types.STRING())
        .process(BusBunchingDetector(), output_type=Types.STRING())
        .name("Bus Bunching Detector")
    )

    smart_meters_stream = env.from_source(smart_meters_source, watermark_strategy, "Smart Meters Source").name("Smart Meters Stream")
    ward_energy_speed = (
        smart_meters_stream
        .key_by(lambda v: json.loads(v).get('ward_id', 'unknown'), key_type=Types.STRING())
        .process(WardEnergySpeedAggregator(), output_type=Types.STRING())
        .name("Ward Energy Speed Aggregator")
    )

    ward_energy_speed.sink_to(ward_energy_sink).name("Ward Energy Speed Kafka Sink")

    all_incidents = aqi_alerts.union(gridlock_alerts, bunching_alerts)
    all_incidents.sink_to(incidents_sink).name("Incidents Kafka Sink")

    env.execute("UrbanPulse - Real-Time Incident & Speed Energy Processor")


if __name__ == '__main__':
    build_and_run()
