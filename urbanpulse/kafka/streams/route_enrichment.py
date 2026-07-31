"""Stream-Table Join for Transit Telemetry Enrichment.

Executes real-time stream-table join (`urbanpulse.bus_gps` ⋈ `route_schedule`)
to produce enriched bus telemetry carrying scheduled arrival timestamps and terminal metadata
to `urbanpulse.enriched_bus_gps`.
"""

import os
import csv
import json
import logging
from datetime import datetime, timezone
import faust

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
INPUT_TOPIC = 'urbanpulse.bus_gps'
OUTPUT_TOPIC = 'urbanpulse.enriched_bus_gps'
ROUTE_SCHEDULE_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    'data', 'route_schedule.csv'
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("RouteEnrichment")


app = faust.App(
    'urbanpulse-route-enrichment',
    broker=f'kafka://{KAFKA_BOOTSTRAP}',
    value_serializer='json',
    producer_compression_type='lz4',
)


class BusGPSEvent(faust.Record, serializer='json'):
    """Raw vehicle location record."""
    bus_id: str
    route_id: str
    lat: float
    lon: float
    speed_kmh: float
    occupancy_pct: int
    timestamp: str


class EnrichedBusGPSEvent(faust.Record, serializer='json'):
    """Vehicle location record enriched with transit schedule metadata."""
    bus_id: str
    route_id: str
    lat: float
    lon: float
    speed_kmh: float
    occupancy_pct: int
    timestamp: str
    route_name: str
    terminal: str
    scheduled_arrival_time: str
    enrichment_timestamp: str


class RouteScheduleKTable:
    """In-memory state table representing reference route schedule metadata (KTable model)."""

    def __init__(self, csv_path: str):
        self.table = {}
        self._load_from_csv(csv_path)

    def _load_from_csv(self, csv_path: str):
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    route_id = row['route_id']
                    if route_id not in self.table:
                        self.table[route_id] = {
                            'route_name': row['route_name'],
                            'terminal': row['terminal'],
                            'scheduled_arrival_time': row['scheduled_arrival_time'],
                            'direction': row.get('direction', 'UP'),
                            'stops': []
                        }
                    self.table[route_id]['stops'].append({
                        'stop_id': row['stop_id'],
                        'stop_sequence': int(row['stop_sequence']),
                        'scheduled_arrival_time': row['scheduled_arrival_time']
                    })

            logger.info(f"Loaded schedule lookup table: {len(self.table)} routes from {csv_path}")

        except FileNotFoundError:
            logger.error(f"Route schedule file missing at: {csv_path}")
        except Exception as e:
            logger.error(f"Error parsing route schedule file: {e}")

    def lookup(self, route_id: str) -> dict:
        return self.table.get(route_id)

    def get_nearest_scheduled_time(self, route_id: str) -> str:
        """Finds next scheduled stop arrival time relative to current wall-clock time."""
        route_info = self.table.get(route_id)
        if not route_info or not route_info['stops']:
            return "N/A"

        now = datetime.now()
        current_time_str = now.strftime("%H:%M:%S")

        for stop in sorted(route_info['stops'], key=lambda s: s['scheduled_arrival_time']):
            if stop['scheduled_arrival_time'] >= current_time_str:
                return stop['scheduled_arrival_time']

        return route_info['stops'][0]['scheduled_arrival_time']


route_ktable = RouteScheduleKTable(ROUTE_SCHEDULE_CSV)

bus_gps_topic = app.topic(INPUT_TOPIC, value_type=BusGPSEvent)
enriched_topic = app.topic(OUTPUT_TOPIC, value_type=EnrichedBusGPSEvent)


@app.agent(bus_gps_topic, sink=[enriched_topic])
async def enrich_bus_gps(stream):
    """Stream processing task performing non-blocking KTable lookups per key."""
    events_processed = 0
    enriched_count = 0
    unenriched_count = 0

    async for event in stream:
        events_processed += 1

        route_info = route_ktable.lookup(event.route_id)

        if route_info:
            enriched_event = EnrichedBusGPSEvent(
                bus_id=event.bus_id,
                route_id=event.route_id,
                lat=event.lat,
                lon=event.lon,
                speed_kmh=event.speed_kmh,
                occupancy_pct=event.occupancy_pct,
                timestamp=event.timestamp,
                route_name=route_info['route_name'],
                terminal=route_info['terminal'],
                scheduled_arrival_time=route_ktable.get_nearest_scheduled_time(event.route_id),
                enrichment_timestamp=datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )
            enriched_count += 1
        else:
            enriched_event = EnrichedBusGPSEvent(
                bus_id=event.bus_id,
                route_id=event.route_id,
                lat=event.lat,
                lon=event.lon,
                speed_kmh=event.speed_kmh,
                occupancy_pct=event.occupancy_pct,
                timestamp=event.timestamp,
                route_name="UNKNOWN_ROUTE",
                terminal="UNKNOWN_TERMINAL",
                scheduled_arrival_time="N/A",
                enrichment_timestamp=datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )
            unenriched_count += 1

        if events_processed % 1000 == 0:
            logger.info(
                f"Enrichment Status: {events_processed:,} processed | "
                f"Joined: {enriched_count:,} | Misses: {unenriched_count:,}")

        yield enriched_event


@app.task
async def on_started():
    logger.info(f"Started Route Enrichment Faust Agent -> {INPUT_TOPIC} to {OUTPUT_TOPIC}")


if __name__ == '__main__':
    app.main()
