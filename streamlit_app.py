"""Streamlit Unified Testing Dashboard & Platform Process Controller.

Provides an interactive GUI for managing, monitoring, and evaluating the entire UrbanPulse platform.
Supports cross-platform execution (Linux/macOS and Windows PowerShell/WSL), live subprocess management, log streaming,
metrics aggregation, and direct query access to the Lambda Architecture serving layer.
"""

import os
import re
import sys
import time
import json
import shlex
import queue
import threading
import subprocess
from datetime import datetime
from collections import deque
import streamlit as st
import pandas as pd

def split_command(cmd: str):
    """Splits shell command string into tokens, taking OS platform context into account."""
    import shutil
    os_target = st.session_state.get("os_target", "Linux/macOS" if sys.platform != 'win32' else "Windows")
    use_wsl_docker = st.session_state.get("use_wsl_docker", False)
    
    if use_wsl_docker and cmd.strip().startswith(("docker ", "docker-compose ", "docker compose ")):
        cmd = "wsl " + cmd
        
    if os_target == "Windows":
        return [arg.strip('"').strip("'") for arg in shlex.split(cmd, posix=False)]
    else:
        return shlex.split(cmd)

# ============================================================
# Page Configuration & Custom Styling
# ============================================================
st.set_page_config(
    page_title="UrbanPulse — Testing & Verification Dashboard",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Sleek Dark Theme & Glassmorphism CSS
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background-color: #0d1117;
        color: #c9d1d9;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Header & Title banner */
    .title-banner {
        background: linear-gradient(135deg, #1f6feb 0%, #238636 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 8px 24px rgba(31, 111, 235, 0.2);
    }
    .title-banner h1 {
        margin: 0;
        font-weight: 800;
        font-size: 2.2rem;
        letter-spacing: -0.5px;
    }
    .title-banner p {
        margin: 0.5rem 0 0 0;
        font-size: 1.05rem;
        opacity: 0.9;
    }
    
    /* Script reference pill badge */
    .script-pill {
        display: inline-block;
        background-color: #161b22;
        border: 1px solid #30363d;
        color: #58a6ff;
        padding: 0.35rem 0.75rem;
        border-radius: 6px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.88rem;
        margin-bottom: 0.75rem;
    }
    .script-pill b {
        color: #8b949e;
    }
    
    /* Status indicator dot */
    .status-running {
        color: #3fb950;
        font-weight: bold;
    }
    .status-stopped {
        color: #f85149;
        font-weight: bold;
    }
    
    /* Card wrapper */
    .card {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 1.25rem;
        margin-bottom: 1.25rem;
    }
    
    /* Log console output */
    .log-box {
        background-color: #010409;
        border: 1px solid #21262d;
        border-radius: 8px;
        padding: 1rem;
        font-family: 'JetBrains Mono', 'Courier New', monospace;
        font-size: 0.82rem;
        color: #7ee787;
        height: 260px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    
    /* Metric mini-card */
    .metric-card {
        background: linear-gradient(135deg, #161b22 0%, #1c2333 100%);
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 0.6rem 0.9rem;
        margin-bottom: 0.5rem;
    }
    .metric-card .label {
        font-size: 0.72rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-card .value {
        font-size: 1.4rem;
        font-weight: 700;
        color: #58a6ff;
    }
    .metric-card .value.green { color: #3fb950; }
    .metric-card .value.orange { color: #d29922; }
    .metric-card .value.red { color: #f85149; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Background Process Manager (Persistent across rerun)
# ============================================================
@st.cache_resource
class ProcessManager:
    """Manages asynchronous subprocess execution with live stdout/stderr buffering."""
    def __init__(self):
        self.processes = {}  # key -> subprocess.Popen
        self.logs = {}       # key -> deque of log lines
        self.commands = {}   # key -> command string executed
        self.metrics = {}    # key -> list of metric data points
        self.lock = threading.Lock()

    def start_process(self, key: str, cmd: str, cwd: str = None):
        with self.lock:
            # Stop existing if running
            if key in self.processes and self.processes[key].poll() is None:
                self.stop_process(key)

            self.logs[key] = deque(maxlen=500)
            self.commands[key] = cmd
            self.metrics[key] = []
            
            # Start process
            try:
                proc = subprocess.Popen(
                    split_command(cmd),
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=1,
                    universal_newlines=True,
                    env=dict(os.environ, PYTHONUNBUFFERED="1")
                )
                self.processes[key] = proc
                
                # Start background log reader thread
                t = threading.Thread(target=self._reader_thread, args=(key, proc), daemon=True)
                t.start()
                return True, f"Started process: {cmd}"
            except Exception as e:
                self.logs[key].append(f"❌ ERROR starting process: {str(e)}")
                return False, str(e)

    def _reader_thread(self, key: str, proc: subprocess.Popen):
        try:
            for line in iter(proc.stdout.readline, ''):
                if line:
                    with self.lock:
                        if key in self.logs:
                            self.logs[key].append(line.rstrip())
                            self._parse_metrics(key, line)
        except Exception:
            pass
        finally:
            with self.lock:
                if key in self.logs:
                    self.logs[key].append(f"🏁 Process exited with code: {proc.poll()}")

    def _parse_metrics(self, key: str, line: str):
        """Parse periodic stats from all script types and append metric data points."""
        now = time.time()

        # -----------------------------------------------------------
        # 1. Producers (bus_gps, air_quality, traffic_signal, smart_meter)
        #    bus_gps:        "Progress: 12,340 events sent | Rate: 200 evt/s | Errors: 0"
        #    air_quality:    "Progress: 5,000 events | Rate: 60/s | Null AQI: 250 (5.0%)"
        #    traffic_signal: "Progress: 3,800 events | Rate: 100/s"
        #    smart_meter:    "Progress: 11,000 events | Rate: 150/s"
        # -----------------------------------------------------------
        prod_match = re.search(r"Progress:\s*([\d,]+)\s*events", line)
        if prod_match:
            total = int(prod_match.group(1).replace(',', ''))
            rate = 0
            # Two formats: "Rate: 200 evt/s" or "Rate: 200/s"
            rate_match = re.search(r"Rate:\s*([\d,]+)\s*(?:evt/s|/s)", line)
            if rate_match:
                rate = int(rate_match.group(1).replace(',', ''))
            errors = 0
            err_match = re.search(r"Errors:\s*(\d+)", line)
            if err_match:
                errors = int(err_match.group(1))
            null_aqi = 0
            null_match = re.search(r"Null AQI:\s*(\d+)", line)
            if null_match:
                null_aqi = int(null_match.group(1))
            self.metrics[key].append({
                "time": now, "total": total, "rate": rate,
                "errors": errors, "null_aqi": null_aqi
            })
            return

        # -----------------------------------------------------------
        # 2. High-Priority Consumer
        #    "📊 HIGH_PRIORITY Stats | Processed: 1,230 | Rate: 240/s | Lag: 0 | ..."
        # -----------------------------------------------------------
        hp_match = re.search(r"HIGH_PRIORITY.*Processed:\s*([\d,]+)\s*\|\s*Rate:\s*([\d,]+)/s\s*\|\s*Lag:\s*(-?[\d,]+)", line)
        if hp_match:
            lag_val = int(hp_match.group(3).replace(',', ''))
            self.metrics[key].append({
                "time": now,
                "total": int(hp_match.group(1).replace(',', '')),
                "rate": int(hp_match.group(2).replace(',', '')),
                "lag": max(0, lag_val),  # Treat -1 (error) as 0
            })
            return

        # -----------------------------------------------------------
        # 3. Standard-Priority Consumer
        #    "📊 STANDARD Consumer-1 | Processed: 500 | Rate: 5/s | Zones: 4 | Delay: 200ms/msg"
        # -----------------------------------------------------------
        std_match = re.search(r"STANDARD.*Processed:\s*([\d,]+)\s*\|\s*Rate:\s*([\d,]+)/s", line)
        if std_match:
            total = int(std_match.group(1).replace(',', ''))
            rate = int(std_match.group(2).replace(',', ''))
            lag = 0
            # Lag is reported separately; try to capture it from "Total Lag:" lines
            lag_match = re.search(r"Total Lag:\s*([\d,]+)", line)
            if lag_match:
                lag = int(lag_match.group(1).replace(',', ''))
            self.metrics[key].append({
                "time": now, "total": total, "rate": rate, "lag": lag
            })
            return

        # Also capture lag from separate lag report lines emitted by standard consumer
        # Format: "📈 STANDARD Consumer-1 LAG: Total=1,234 | Partitions=[...] | ⚠️ LAG BUILDING UP"
        lag_line_match = re.search(r"STANDARD Consumer-\d+\s*LAG:\s*Total=([\d,]+)", line)
        if lag_line_match:
            lag = int(lag_line_match.group(1).replace(',', ''))
            last = self.metrics[key][-1] if self.metrics.get(key) else {}
            self.metrics[key].append({
                "time": now,
                "total": last.get("total", 0),
                "rate": last.get("rate", 0),
                "lag": lag,
            })
            return

        # -----------------------------------------------------------
        # 4. DLQ Router
        #    "📊 DLQ Router | Validated: 10,000 | Invalid: 50 (0.5%) | DLQ Sent: 50 | Errors: {..}"
        # -----------------------------------------------------------
        dlq_match = re.search(r"DLQ Router.*Validated:\s*([\d,]+)\s*\|\s*Invalid:\s*([\d,]+)", line)
        if dlq_match:
            total = int(dlq_match.group(1).replace(',', ''))
            invalid = int(dlq_match.group(2).replace(',', ''))
            dlq_sent = 0
            dlq_sent_match = re.search(r"DLQ Sent:\s*([\d,]+)", line)
            if dlq_sent_match:
                dlq_sent = int(dlq_sent_match.group(1).replace(',', ''))
            self.metrics[key].append({
                "time": now, "total": total, "invalid": invalid, "dlq_sent": dlq_sent
            })
            return

        # -----------------------------------------------------------
        # 5. DLQ Report
        #    "Collected 150 DLQ messages (30s / 300s)"
        # -----------------------------------------------------------
        dlq_report_match = re.search(r"Collected\s*([\d,]+)\s*DLQ messages", line)
        if dlq_report_match:
            total = int(dlq_report_match.group(1).replace(',', ''))
            self.metrics[key].append({"time": now, "total": total, "rate": 0})
            return

        # -----------------------------------------------------------
        # 6. Faust Route Enrichment
        #    "🔄 Enrichment Progress: 1,000 processed | Enriched: 980 | Unenriched: 20 | Join Rate: 98.0%"
        # -----------------------------------------------------------
        faust_match = re.search(r"Enrichment Progress:\s*([\d,]+)\s*processed\s*\|\s*Enriched:\s*([\d,]+)\s*\|\s*Unenriched:\s*([\d,]+)", line)
        if faust_match:
            total = int(faust_match.group(1).replace(',', ''))
            enriched = int(faust_match.group(2).replace(',', ''))
            unenriched = int(faust_match.group(3).replace(',', ''))
            self.metrics[key].append({
                "time": now, "total": total, "enriched": enriched, "unenriched": unenriched
            })
            return

        # -----------------------------------------------------------
        # 7. Flink Incident Detection / Console Consumer
        #    Count lines containing incident_type or Alert keywords
        # -----------------------------------------------------------
        if "incident_type" in line.lower() or "aqi_emergency" in line.lower() or \
           "traffic_gridlock" in line.lower() or "bus_bunching" in line.lower():
            last_total = self.metrics[key][-1]["total"] if self.metrics.get(key) and self.metrics[key] else 0
            self.metrics[key].append({"time": now, "total": last_total + 1, "rate": 0, "alert_type": "incident"})
            return

        # -----------------------------------------------------------
        # 8. Spark Stage Progress (from stderr merged to stdout)
        #    "[Stage 4:===============================>       (2 + 1) / 3]"
        # -----------------------------------------------------------
        spark_stage_match = re.search(r"\[Stage\s+(\d+).*\((\d+)\s*\+\s*(\d+)\)\s*/\s*(\d+)\]", line)
        if spark_stage_match:
            stage = int(spark_stage_match.group(1))
            done = int(spark_stage_match.group(2))
            running = int(spark_stage_match.group(3))
            total = int(spark_stage_match.group(4))
            self.metrics[key].append({
                "time": now, "total": done, "rate": running,
                "spark_stage": stage, "spark_total_tasks": total,
                "spark_done_tasks": done
            })
            return

    def get_metrics(self, key: str):
        with self.lock:
            if key in self.metrics:
                return list(self.metrics[key])
            return []

    def stop_process(self, key: str):
        with self.lock:
            if key in self.processes:
                proc = self.processes[key]
                if proc.poll() is None:
                    try:
                        proc.terminate()
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    except Exception:
                        pass
                if key in self.logs:
                    self.logs[key].append("🛑 Process manually stopped by user.")

    def stop_all(self):
        with self.lock:
            for key in list(self.processes.keys()):
                self.stop_process(key)

    def is_running(self, key: str) -> bool:
        with self.lock:
            if key in self.processes:
                return self.processes[key].poll() is None
            return False

    def get_logs(self, key: str) -> str:
        with self.lock:
            if key in self.logs:
                return "\n".join(self.logs[key])
            return "No logs generated yet. Click 'Start' above to run."


# Initialize process manager singleton
pm = ProcessManager()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KAFKA_BOOTSTRAP = "localhost:9092"


# ============================================================
# Title Banner & Sidebar Navigation
# ============================================================
st.markdown("""
<div class="title-banner">
    <h1>🏙️ UrbanPulse</h1>
    <p>Stream Processing & Analytics Situated Learning Assignment</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🧭 Navigation & Phases")
    selected_tab = st.radio(
        "Select Verification Stage:",
        [
            "1. 🐳 Infrastructure & Kafka Setup",
            "2. 📡 Telemetry Producers (Task B)",
            "3. 🏎️ Priority Consumers (Task B)",
            "4. 🔀 Stream Enrichment & DLQ (Task B)",
            "5. ⚡ Flink Speed Layer (Task C Part I)",
            "6. 🔥 Spark Analytics Layer (Task C Part II)",
            "7. 📊 System Health & Process Manager",
            "8. 🌐 Traditional Lambda Serving Layer",
            "9. 🔎 Kafka Explorer",
            "10. 🌐 Component Web UIs & Ports"
        ]
    )
    
    st.markdown("---")
    st.markdown("### 🚨 Global Process Control")
    if st.button("🛑 STOP ALL RUNNING PROCESSES", type="primary", use_container_width=True):
        pm.stop_all()
        st.toast("✅ All background processes terminated.", icon="🛑")
        time.sleep(0.5)
        st.rerun()
    st.caption(f"📁 Workspace: `{BASE_DIR}`")
    st.markdown("---")
    st.caption("📁 Workspace: `.` (SPA)")
    st.caption("Python Version: " + sys.version.split(" ")[0])
    
    # Target Platform & Docker Environment settings
    st.markdown("---")
    st.markdown("### ⚙️ Environment Settings")
    
    # Auto-select default based on sys.platform
    default_os_idx = 1 if sys.platform == 'win32' else 0
    os_target = st.radio(
        "Target Operating System:",
        ["Linux/macOS", "Windows"],
        index=default_os_idx,
        key="os_target",
        help="Adapts path splitting and script execution commands for the selected OS."
    )
    
    # Checkbox option for WSL-based Docker
    import shutil
    default_wsl_docker = False
    if sys.platform == 'win32':
        if not shutil.which("docker") and shutil.which("wsl"):
            default_wsl_docker = True
            
    use_wsl_docker = st.checkbox(
        "🐳 Use WSL-based Docker",
        value=default_wsl_docker,
        key="use_wsl_docker",
        help="Prepend 'wsl ' to all docker/docker-compose commands (e.g. wsl docker exec)."
    )
    
    st.markdown("---")
    st.markdown("### ⚙️ Dashboard Settings")
    auto_refresh = st.checkbox("🔄 Auto-Refresh (5s)", value=False, 
                                help="Automatically refresh the dashboard every 5 seconds to update metrics and logs.")


# ============================================================
# Helper: Render Metric Visualization Panel
# ============================================================
def render_metrics_panel(process_key: str, script_type: str = "generic"):
    """Render the live metrics visualization panel for a given process.
    
    IMPORTANT: This function is called INSIDE a st.columns() column,
    so it must NOT use st.columns() itself (Streamlit forbids nested columns).
    
    script_type can be: 'producer', 'consumer_hp', 'consumer_std', 'dlq_router',
                        'dlq_report', 'enrichment', 'flink', 'flink_monitor',
                        'spark', 'none', 'generic'
    """
    import pandas as pd
    
    # Skip visualization entirely for infra/none types
    if script_type == "none":
        return
    
    metrics_data = pm.get_metrics(process_key)
    
    if not metrics_data:
        st.markdown("📊 **Live Metrics**")
        st.info("Waiting for data… Start the script and refresh to see live metrics.")
        return
    
    last = metrics_data[-1]
    st.markdown("📊 **Live Metrics**")

    # --- Producer Metrics ---
    if script_type == "producer":
        st.metric("Total Events Sent", f"{last.get('total', 0):,}")
        st.metric("Throughput", f"{last.get('rate', 0):,} evt/s")
        if last.get("errors", 0) > 0:
            st.metric("❌ Errors", f"{last['errors']:,}")
        if last.get("null_aqi", 0) > 0:
            st.metric("🫧 Null AQI Injected", f"{last['null_aqi']:,}")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            chart_data = df.set_index("Elapsed (s)")[["rate"]].rename(columns={"rate": "Events/s"})
            st.line_chart(chart_data, height=140, use_container_width=True)

    # --- High-Priority Consumer ---
    elif script_type == "consumer_hp":
        st.metric("Processed", f"{last.get('total', 0):,}")
        st.metric("Rate", f"{last.get('rate', 0):,} /s")
        lag_val = last.get("lag", 0)
        st.metric("Lag", f"{lag_val:,}",
                   delta="ZERO ✅" if lag_val == 0 else f"{lag_val:,} behind",
                   delta_color="off" if lag_val == 0 else "inverse")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            cols = ["rate"]
            if "lag" in df.columns and df["lag"].max() > 0:
                cols.append("lag")
            st.line_chart(df.set_index("Elapsed (s)")[cols], height=140, use_container_width=True)

    # --- Standard Consumer ---
    elif script_type == "consumer_std":
        st.metric("Processed", f"{last.get('total', 0):,}")
        st.metric("Rate", f"{last.get('rate', 0):,} /s")
        lag_val = last.get("lag", 0)
        st.metric("Lag", f"{lag_val:,}",
                   delta="BUILDING ⚠️" if lag_val > 0 else "Catching up",
                   delta_color="inverse" if lag_val > 0 else "off")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            cols = []
            if "rate" in df.columns:
                cols.append("rate")
            if "lag" in df.columns and df["lag"].max() > 0:
                cols.append("lag")
            if cols:
                st.line_chart(df.set_index("Elapsed (s)")[cols], height=140, use_container_width=True)

    # --- DLQ Router ---
    elif script_type == "dlq_router":
        st.metric("Validated", f"{last.get('total', 0):,}")
        inv = last.get("invalid", 0)
        total = max(1, last.get("total", 1))
        pct = inv / total * 100
        st.metric("Invalid", f"{inv:,} ({pct:.1f}%)")
        st.metric("DLQ Sent", f"{last.get('dlq_sent', 0):,}")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            cols = []
            if "invalid" in df.columns and df["invalid"].max() > 0:
                cols.append("invalid")
            if "dlq_sent" in df.columns and df["dlq_sent"].max() > 0:
                cols.append("dlq_sent")
            if cols:
                st.line_chart(df.set_index("Elapsed (s)")[cols], height=140, use_container_width=True)

    # --- DLQ Report ---
    elif script_type == "dlq_report":
        st.metric("DLQ Messages Collected", f"{last.get('total', 0):,}")

    # --- Faust Enrichment ---
    elif script_type == "enrichment":
        st.metric("Total Processed", f"{last.get('total', 0):,}")
        st.metric("Enriched ✅", f"{last.get('enriched', 0):,}")
        st.metric("Unenriched ⚠️", f"{last.get('unenriched', 0):,}")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            cols = []
            if "enriched" in df.columns:
                cols.append("enriched")
            if "unenriched" in df.columns:
                cols.append("unenriched")
            if cols:
                st.line_chart(df.set_index("Elapsed (s)")[cols], height=140, use_container_width=True)

    # --- Flink Job / Flink Monitor ---
    elif script_type in ("flink", "flink_monitor"):
        alert_count = last.get("total", 0)
        st.metric("🚨 Alerts Detected", f"{alert_count:,}")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            st.line_chart(df.set_index("Elapsed (s)")[["total"]].rename(columns={"total": "Cumulative Alerts"}),
                          height=140, use_container_width=True)

    # --- Spark Streaming ---
    elif script_type == "spark":
        if "spark_stage" in last:
            st.metric("Current Stage", f"Stage {last.get('spark_stage', '?')}")
            done = last.get("spark_done_tasks", 0)
            total = last.get("spark_total_tasks", 1)
            pct = done / max(1, total) * 100
            st.metric("Tasks", f"{done}/{total} ({pct:.0f}%)")
            if len(metrics_data) >= 2:
                df = pd.DataFrame(metrics_data)
                if "spark_done_tasks" in df.columns:
                    df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
                    st.line_chart(df.set_index("Elapsed (s)")[["spark_done_tasks"]].rename(
                        columns={"spark_done_tasks": "Tasks Completed"}),
                        height=140, use_container_width=True)
        else:
            st.metric("Total Events", f"{last.get('total', 0):,}")

    # --- Generic fallback ---
    else:
        if "total" in last:
            st.metric("Events", f"{last['total']:,}")
        if "rate" in last and last["rate"] > 0:
            st.metric("Rate", f"{last['rate']:,} /s")
        if len(metrics_data) >= 2:
            df = pd.DataFrame(metrics_data)
            df["Elapsed (s)"] = (df["time"] - metrics_data[0]["time"]).round().astype(int)
            if "total" in df.columns:
                st.line_chart(df.set_index("Elapsed (s)")[["total"]], height=120, use_container_width=True)


# ============================================================
# Helper: Display Script Control Card
# ============================================================
def render_script_controller(
    title: str,
    script_rel_path: str,
    desc: str,
    process_key: str,
    default_cmd: str,
    cwd: str = BASE_DIR,
    script_type: str = "generic"
):
    st.markdown(f"### {title}")
    st.markdown(f"<div class='script-pill'><b>File Reference:</b> <code>{script_rel_path}</code></div>", unsafe_allow_html=True)
    st.markdown(desc)
    
    running = pm.is_running(process_key)
    status_text = "<span class='status-running'>● RUNNING (Active in background)</span>" if running else "<span class='status-stopped'>● STOPPED / IDLE</span>"
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        cmd_input = st.text_input("Execution Command:", value=default_cmd, key=f"cmd_{process_key}")
    with col2:
        st.write("Status:")
        st.markdown(status_text, unsafe_allow_html=True)
    with col3:
        st.write("Action:")
        if not running:
            if st.button("▶️ Start Script", key=f"start_{process_key}", use_container_width=True):
                success, msg = pm.start_process(process_key, cmd_input, cwd)
                if success:
                    st.toast(f"Started: {script_rel_path}", icon="▶️")
                else:
                    st.error(msg)
                time.sleep(0.5)
                st.rerun()
        else:
            if st.button("🛑 Stop Script", key=f"stop_{process_key}", use_container_width=True):
                pm.stop_process(process_key)
                st.toast(f"Stopped: {script_rel_path}", icon="🛑")
                time.sleep(0.5)
                st.rerun()

    # Log Output Box & Live Metrics
    if script_type == "none":
        # Full-width log only (no metrics panel for infrastructure scripts)
        st.markdown("**Terminal Console Output:**")
        logs = pm.get_logs(process_key)
        st.markdown(f"<div class='log-box'>{logs}</div>", unsafe_allow_html=True)
        if running:
            if st.button("🔄 Refresh Logs", key=f"ref_{process_key}"):
                st.rerun()
    else:
        # Side-by-side: log on left, live metrics on right
        col_log, col_viz = st.columns([3, 2])
        
        with col_log:
            st.markdown("**Terminal Console Output:**")
            logs = pm.get_logs(process_key)
            st.markdown(f"<div class='log-box'>{logs}</div>", unsafe_allow_html=True)
            if running:
                if st.button("🔄 Refresh Logs", key=f"ref_{process_key}"):
                    st.rerun()
                    
        with col_viz:
            render_metrics_panel(process_key, script_type=script_type)

    st.markdown("---")


# ============================================================
# Phase 1: Infrastructure & Kafka Setup
# ============================================================
if selected_tab.startswith("1."):
    st.header("🐳 Phase 1: Infrastructure & Topic Initialization")
    st.write("Launch and verify the 3-broker KRaft Kafka cluster, Flink, Spark, and TimescaleDB containers (`docker-compose.yml`), then initialize all 9 topics (`cluster_setup.sh`).")
    
    os_target = st.session_state.get("os_target", "Linux/macOS" if sys.platform != 'win32' else "Windows")
    if os_target == "Windows":
        cmd_up = "docker compose up -d"
        cmd_down = "docker compose down"
        cmd_ps = "docker compose ps"
        cmd_setup = "docker exec urbanpulse-kafka-1 bash /opt/kafka/cluster_setup.sh"
    else:
        cmd_up = "docker-compose up -d"
        cmd_down = "docker-compose down"
        cmd_ps = "docker-compose ps"
        cmd_setup = "docker exec -i urbanpulse-kafka-1 bash /opt/kafka/cluster_setup.sh"

    col_a, col_b = st.columns(2)
    with col_a:
        render_script_controller(
            title="1. Launch Docker Infrastructure (`Up`)",
            script_rel_path="urbanpulse/docker/docker-compose.yml",
            desc="Runs standard container launch to start all 10 UrbanPulse services (KRaft Kafka, Flink, Spark, TimescaleDB, MinIO, Grafana).",
            process_key="docker_up",
            default_cmd=cmd_up,
            cwd=os.path.join(BASE_DIR, "urbanpulse", "docker"),
            script_type="none"
        )
        render_script_controller(
            title="3. Stop Docker Infrastructure (`Down`)",
            script_rel_path="urbanpulse/docker/docker-compose.yml",
            desc="Gracefully stops and removes all UrbanPulse containers and virtual networks when testing is complete.",
            process_key="docker_down",
            default_cmd=cmd_down,
            cwd=os.path.join(BASE_DIR, "urbanpulse", "docker"),
            script_type="none"
        )
    with col_b:
        render_script_controller(
            title="2. Check Infrastructure Status (`PS`)",
            script_rel_path="urbanpulse/docker/docker-compose.yml",
            desc="Inspects the real-time running state, health checks, and port mappings of all 10 UrbanPulse containers.",
            process_key="docker_ps",
            default_cmd=cmd_ps,
            cwd=os.path.join(BASE_DIR, "urbanpulse", "docker"),
            script_type="none"
        )
        render_script_controller(
            title="4. Kafka Topic & Retention Setup Script",
            script_rel_path="urbanpulse/kafka/config/cluster_setup.sh",
            desc="Executes the topic creation script inside `urbanpulse-kafka-1` (`Q4 requirement`: 24h GPS, 7d traffic, 90d AQI, 365d smart meters).",
            process_key="cluster_setup",
            default_cmd=cmd_setup,
            cwd=BASE_DIR,
            script_type="none"
        )

# ============================================================
# Phase 2: Telemetry Producers (Task B)
# ============================================================
elif selected_tab.startswith("2."):
    st.header("📡 Phase 2: Live Data Telemetry Producers (Task B)")
    st.write("Test the four Python stream simulation scripts. Each script continuously emits JSON payloads to Kafka using specific keying and fault-tolerant delivery semantics.")
    
    col1, col2 = st.columns(2)
    with col1:
        render_script_controller(
            title="1. Bus GPS Telemetry Producer (`~2,400 events/sec`)",
            script_rel_path="urbanpulse/kafka/producers/bus_gps_producer.py",
            desc="Keyed by `route_id` to guarantee strict FIFO ordering per route (`Q5 requirement`). Simulates 12,000 buses with occasional bus bunching.",
            process_key="prod_gps",
            default_cmd=f"{sys.executable} urbanpulse/kafka/producers/bus_gps_producer.py --rate 200 --duration 0",
            script_type="producer"
        )
        render_script_controller(
            title="3. Traffic Signal Producer (`~380 events/sec`)",
            script_rel_path="urbanpulse/kafka/producers/traffic_signal_producer.py",
            desc="Keyed by `junction_id`. Simulates 3,800 traffic signals and injects gridlock events (`avg_wait > 180s`) across consecutive cycles.",
            process_key="prod_traffic",
            default_cmd=f"{sys.executable} urbanpulse/kafka/producers/traffic_signal_producer.py --rate 100 --duration 0",
            script_type="producer"
        )
    with col2:
        render_script_controller(
            title="2. Air Quality Producer (`~60 events/sec` | 5% Nulls)",
            script_rel_path="urbanpulse/kafka/producers/air_quality_producer.py",
            desc="Enforces at-least-once delivery with exponential backoff and injects exactly 5% `None` AQI readings (`Q5 requirement`).",
            process_key="prod_aqi",
            default_cmd=f"{sys.executable} urbanpulse/kafka/producers/air_quality_producer.py --rate 20 --duration 0",
            script_type="producer"
        )
        render_script_controller(
            title="4. Smart Meter Producer (`~1,100 events/sec`)",
            script_rel_path="urbanpulse/kafka/producers/smart_meter_producer.py",
            desc="Keyed by `ward_id`. Simulates 1.1 million meters across 20 municipal wards with diurnal power factor variations.",
            process_key="prod_meters",
            default_cmd=f"{sys.executable} urbanpulse/kafka/producers/smart_meter_producer.py --rate 150 --duration 0",
            script_type="producer"
        )

# ============================================================
# Phase 3: Priority Consumers (Task B)
# ============================================================
elif selected_tab.startswith("3."):
    st.header("🏎️ Phase 3: Priority Consumer & Zero-Lag Demonstration (Task B)")
    st.write("Demonstrate `Q6 requirement`: A single `HIGH_PRIORITY_SIGNAL_CONTROL` consumer processes messages instantly (0 lag) while a 3-consumer `STANDARD_ANALYTICS_DASHBOARD` group with an intentional 200ms processing delay builds up lag without impacting signal control.")
    
    st.info("💡 **Tip:** First make sure `Traffic Signal Producer` is running in Stage 2 so there are active messages in `urbanpulse.traffic_signals`!")
    
    col1, col2 = st.columns(2)
    with col1:
        render_script_controller(
            title="HIGH_PRIORITY Signal Control Consumer",
            script_rel_path="urbanpulse/kafka/consumers/high_priority_consumer.py",
            desc="Subscribes to all 6 partitions of `urbanpulse.traffic_signals`. Configured with low-latency fetch (`0ms delay`) to maintain near-zero lag.",
            process_key="cons_high",
            default_cmd=f"{sys.executable} urbanpulse/kafka/consumers/high_priority_consumer.py --duration 180",
            script_type="consumer_hp"
        )
        render_script_controller(
            title="STANDARD_PRIORITY Consumer Instance #2",
            script_rel_path="urbanpulse/kafka/consumers/standard_priority_consumer.py (ID: 2)",
            desc="Second consumer in the `STANDARD_ANALYTICS_DASHBOARD` group sharing partitions with ID 1 and ID 3.",
            process_key="cons_std_2",
            default_cmd=f"{sys.executable} urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 2 --delay-ms 200 --duration 180",
            script_type="consumer_std"
        )
    with col2:
        render_script_controller(
            title="STANDARD_PRIORITY Consumer Instance #1",
            script_rel_path="urbanpulse/kafka/consumers/standard_priority_consumer.py (ID: 1)",
            desc="First consumer in the analytics group. Includes intentional `200ms` processing delay (`--delay-ms 200`) causing expected lag buildup.",
            process_key="cons_std_1",
            default_cmd=f"{sys.executable} urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 1 --delay-ms 200 --duration 180",
            script_type="consumer_std"
        )
        render_script_controller(
            title="STANDARD_PRIORITY Consumer Instance #3",
            script_rel_path="urbanpulse/kafka/consumers/standard_priority_consumer.py (ID: 3)",
            desc="Third consumer in the analytics group.",
            process_key="cons_std_3",
            default_cmd=f"{sys.executable} urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 3 --delay-ms 200 --duration 180",
            script_type="consumer_std"
        )

# ============================================================
# Phase 4: Stream Enrichment & DLQ (Task B)
# ============================================================
elif selected_tab.startswith("4."):
    st.header("🔀 Phase 4: Stream Enrichment & Dead-Letter Queue Validation (Task B)")
    st.write("Test `Q7 requirement` (Faust KTable join of `bus_gps` with `route_schedule.csv`) and `Q8 requirement` (6 DLQ validation rules & 5-minute error distribution report).")
    
    col1, col2 = st.columns(2)
    with col1:
        render_script_controller(
            title="1. Faust Kafka Streams Route Enrichment Service",
            script_rel_path="urbanpulse/kafka/streams/route_enrichment.py",
            desc="Performs stream-table KTable join (`bus_gps ⋈ route_schedule.csv`). Enriches coordinates with `route_name`, `terminal`, and `scheduled_arrival_time`.",
            process_key="enrichment",
            default_cmd=f"{sys.executable} urbanpulse/kafka/streams/route_enrichment.py worker --without-web -l info",
            script_type="enrichment"
        )
        render_script_controller(
            title="3. DLQ 5-Minute Error Distribution Report Generator",
            script_rel_path="urbanpulse/kafka/dlq/dlq_report.py",
            desc="Consumes from `urbanpulse.dlq` for a specified duration and produces a formatted analysis table showing percentages of `NULL_AQI`, `IMPOSSIBLE_GPS`, and timestamp errors (`Q8 requirement`).",
            process_key="dlq_report",
            default_cmd=f"{sys.executable} urbanpulse/kafka/dlq/dlq_report.py --duration 30",
            script_type="dlq_report"
        )
    with col2:
        render_script_controller(
            title="2. Dead-Letter Queue (DLQ) Validation Router",
            script_rel_path="urbanpulse/kafka/dlq/dlq_router.py",
            desc="Subscribes to all 4 streams and enforces 6 rules (`null AQI, range 0-500, GPS bounding box, negative speed, future timestamp > 5m, missing fields`). Routes failures to `urbanpulse.dlq`.",
            process_key="dlq_router",
            default_cmd=f"{sys.executable} urbanpulse/kafka/dlq/dlq_router.py --duration 180",
            script_type="dlq_router"
        )

# ============================================================
# Phase 5: Flink Speed Layer (Task C Part I)
# ============================================================
elif selected_tab.startswith("5."):
    st.header("⚡ Phase 5: Apache Flink Speed Processing Engine (Task C Part I & Speed Layer)")
    st.write("Test `Q9` and Speed Layer requirements: PyFlink DataStream application with bounded out-of-orderness watermarks (`30s`) and RocksDB state backend to detect three critical emergencies, and aggregate smart meters over 15-minute tumbling event-time windows in real-time.")
    
    st.markdown("""
    | Pattern / Aggregate | Input Topic | Keyed By | Detection / Window Condition | Output Topic |
    | :--- | :--- | :--- | :--- | :--- |
    | **(a) AQI Emergency** | `urbanpulse.air_quality` | `sensor_id` | AQI > 300 within 2 minutes of reading (ValueState cooldown) | `urbanpulse.incidents` |
    | **(b) Traffic Gridlock** | `urbanpulse.traffic_signals` | `junction_id` | `avg_wait_sec > 180s` for 3 consecutive cycles (ListState buffer) | `urbanpulse.incidents` |
    | **(c) Bus Bunching** | `urbanpulse.enriched_bus_gps` | `route_id` | Haversine distance `< 200m` for `> 5 minutes` (MapState positions) | `urbanpulse.incidents` |
    | **(d) Ward Energy (Speed)** | `urbanpulse.smart_meters` | `ward_id` | 15-minute tumbling windows in real-time (MapState + event-time timers) | `urbanpulse.ward_energy_summary` |
    """)
    
    col1, col2 = st.columns([3, 2])
    with col1:
        render_script_controller(
            title="PyFlink Incident Detection Pipeline",
            script_rel_path="urbanpulse/flink/incident_detection.py",
            desc="Executes the Flink job directly using local PyFlink (`python incident_detection.py`) or via `flink run`.",
            process_key="flink_job",
            default_cmd=f"{sys.executable} urbanpulse/flink/incident_detection.py",
            script_type="flink"
        )
    with col2:
        os_target = st.session_state.get("os_target", "Linux/macOS" if sys.platform != 'win32' else "Windows")
        cmd_monitor = "docker exec urbanpulse-kafka-1 kafka-console-consumer --bootstrap-server kafka-broker-1:29092 --topic urbanpulse.incidents --from-beginning" if os_target == "Windows" else "docker exec -i urbanpulse-kafka-1 kafka-console-consumer --bootstrap-server kafka-broker-1:29092 --topic urbanpulse.incidents --from-beginning"
        render_script_controller(
            title="Live Incident Alert Sink Monitor",
            script_rel_path="urbanpulse.incidents (Kafka Topic)",
            desc="Consumes and prints real-time alerts emitted by Flink (`AQI_EMERGENCY`, `TRAFFIC_GRIDLOCK`, `BUS_BUNCHING`).",
            process_key="flink_monitor",
            default_cmd=cmd_monitor,
            script_type="flink_monitor"
        )

# ============================================================
# Phase 6: Spark Analytics Layer (Task C Part II)
# ============================================================
elif selected_tab.startswith("6."):
    st.header("🔥 Phase 6: Apache Spark Analytics & Streaming SQL Layer (Task C Part II)")
    st.write("Test `Q10 requirement` (15-min tumbling window ward energy rollups with 45-min late watermark) and `Q11 requirement` (Streaming SQL 10-min rolling AQI with static `zone_profile` join).")
    
    col1, col2 = st.columns(2)
    with col1:
        render_script_controller(
            title="1. Spark Ward Energy Analytics (`Q10`)",
            script_rel_path="urbanpulse/spark/ward_energy_streaming.py",
            desc="Reads `smart_meters`, applies 45-min watermark, computes 15-min tumbling window (`total_kwh`, `avg_power_factor`, `peak_voltage`), and writes simultaneously to Kafka and partitioned Parquet (`partitionBy('ward_id', 'date')`).",
            process_key="spark_energy",
            default_cmd=f"{sys.executable} urbanpulse/spark/ward_energy_streaming.py --bootstrap-servers localhost:9092",
            script_type="spark"
        )
    with col2:
        render_script_controller(
            title="2. Spark Streaming SQL AQI Advisory (`Q11`)",
            script_rel_path="urbanpulse/spark/aqi_health_advisory.py",
            desc="Executes Streaming SQL on `air_quality` using a 10-min sliding window (1-min slide). Joins with static `zone_profile.csv` (`population`, `num_schools`), filters `rolling_avg_aqi > 150`, and outputs in Update mode to `urbanpulse.health_advisories`.",
            process_key="spark_aqi",
            default_cmd=f"{sys.executable} urbanpulse/spark/aqi_health_advisory.py --bootstrap-servers localhost:9092",
            script_type="spark"
        )

# ============================================================
# Phase 7: System Health & Process Manager
# ============================================================
elif selected_tab.startswith("7."):
    st.header("📊 Global Process Manager & System Health Check")
    st.write("Monitor all active background tasks launched by this dashboard.")
    
    active_keys = [k for k in pm.processes if pm.is_running(k)]
    if not active_keys:
        st.success("✅ No background verification processes currently running.")
    else:
        st.warning(f"⚠️ **{len(active_keys)} Active Background Processes Running:**")
        for k in active_keys:
            col_x, col_y, col_z = st.columns([3, 1, 1])
            with col_x:
                st.markdown(f"**Key:** `{k}` | **Command:** `{pm.commands.get(k, 'N/A')}`")
            with col_y:
                metrics_data = pm.get_metrics(k)
                if metrics_data:
                    last = metrics_data[-1]
                    st.metric("Events", f"{last.get('total', 0):,}")
                else:
                    st.caption("No metrics yet")
            with col_z:
                if st.button("🛑 Terminate", key=f"term_{k}"):
                    pm.stop_process(k)
                    st.rerun()
            st.markdown("---")

    # Kafka Topic Health Overview
    st.subheader("📬 Kafka Topic Message Counts")
    st.caption("Queries the Kafka cluster for the latest high-water-mark offsets across all UrbanPulse topics.")
    if st.button("🔍 Fetch Topic Stats", use_container_width=False):
        try:
            os_target = st.session_state.get("os_target", "Linux/macOS" if sys.platform != 'win32' else "Windows")
            exec_prefix = "docker exec" if os_target == "Windows" else "docker exec -i"
            topic_cmd = (
                f"{exec_prefix} urbanpulse-kafka-1 "
                "kafka-run-class kafka.tools.GetOffsetShell "
                "--broker-list kafka-broker-1:29092 "
                "--topic-partitions "
                "urbanpulse.bus_gps:0,urbanpulse.air_quality:0,"
                "urbanpulse.traffic_signals:0,urbanpulse.smart_meters:0,"
                "urbanpulse.enriched_bus_gps:0,urbanpulse.incidents:0,"
                "urbanpulse.dlq:0,urbanpulse.ward_energy_summary:0,"
                "urbanpulse.health_advisories:0 "
                "--time -1"
            )
            result = subprocess.run(
                split_command(topic_cmd),
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                st.code(result.stdout, language="text")
            elif result.stderr.strip():
                st.warning(result.stderr[:500])
            else:
                st.info("No output — topics may not exist yet. Run the cluster_setup script first.")
        except subprocess.TimeoutExpired:
            st.error("Timed out querying Kafka. Are the Docker containers running?")
        except Exception as e:
            st.error(f"Error: {e}")

    st.subheader("📁 Quick Workspace Verification")
    if st.button("Inspect `urbanpulse/` Source Directory Tree"):
        os_target = st.session_state.get("os_target", "Linux/macOS" if sys.platform != 'win32' else "Windows")
        if os_target == "Windows":
            lines = []
            for root, dirs, files in os.walk("urbanpulse"):
                dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
                rel_path = os.path.relpath(root, "urbanpulse")
                if rel_path == ".":
                    depth = 0
                else:
                    depth = rel_path.replace("\\", "/").count("/") + 1
                if depth >= 3:
                    continue
                indent = "  " * depth
                lines.append(f"{indent}📁 {os.path.basename(root)}/")
                sub_indent = "  " * (depth + 1)
                for f in files:
                    if not f.startswith(".") and not f.endswith(".pyc"):
                        lines.append(f"{sub_indent}📄 {f}")
            out = "\n".join(lines)
            st.code(out, language="text")
        else:
            tree_cmd = "find urbanpulse/ -maxdepth 3 -not -path '*/.*'"
            out = subprocess.getoutput(tree_cmd)
            st.code(out, language="bash")

# ============================================================
# Phase 8: Traditional Lambda Serving Layer
# ============================================================
elif selected_tab.startswith("8."):
    st.header("🌐 Traditional Lambda Serving Layer (Query & Merge)")
    st.markdown("This query engine represents the **Serving Layer** of a pure Lambda Architecture.")
    st.markdown("It queries the **Batch View** (Spark Parquet files on MinIO/local storage) and unions them with live streaming updates from the **Speed View** (Flink's real-time outputs in the Kafka topic `urbanpulse.ward_energy_summary`). It deduplicates overlapping windows on-the-fly and prefers the Batch Layer as the absolute source of truth.")

    from urbanpulse.serving_layer import get_merged_ward_energy

    # Select Ward ID and execute query
    col_w, col_btn = st.columns([2, 1])
    with col_w:
        ward_options = [f"WARD_{i:02d}" for i in range(1, 21)]
        selected_ward = st.selectbox("Select Municipal Ward ID:", ward_options)
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        execute_query = st.button("🔍 Execute Serving Layer Query", use_container_width=True)

    if execute_query:
        parquet_dir = os.path.join(BASE_DIR, "urbanpulse/data/ward_energy_parquet")
        with st.spinner(f"Querying and merging batch and speed views for {selected_ward}..."):
            records = get_merged_ward_energy(
                ward_id=selected_ward,
                parquet_dir=parquet_dir,
                bootstrap_servers=KAFKA_BOOTSTRAP
            )

        if not records:
            st.info(f"No records found for {selected_ward}. Make sure smart_meter_producer.py and the stream processors (Flink/Spark) are running and generating output.")
        else:
            st.success(f"✓ Retrieved and merged {len(records)} unique windowed energy records!")
            
            # Convert to DataFrame for visualization
            df = pd.DataFrame(records)
            
            # Display metrics summary of the latest record
            latest = df.iloc[-1]
            c1, c2, c3, c4 = st.columns(4)
            w_start_raw = str(latest.get("window_start", ""))
            try:
                w_start_fmt = pd.to_datetime(w_start_raw).strftime("%H:%M:%S")
            except Exception:
                w_start_fmt = w_start_raw[:19]
            c1.metric("Latest Window Start", w_start_fmt if w_start_raw else "N/A")
            c2.metric("Total Consumption (kWh)", f"{latest.get('total_kwh_consumed', 0.0):.2f}")
            c3.metric("Peak Voltage (V)", f"{latest.get('peak_voltage', 0.0):.1f}")
            c4.metric("Active Layer Source", latest.get("source_layer", "N/A"))

            # Display line chart comparing layers
            st.subheader("📈 Real-Time Unified Consumption View (Tumbling Windows)")
            
            chart_df = df[["window_start", "total_kwh_consumed", "source_layer"]].copy()
            chart_df.rename(columns={"window_start": "Window Start", "total_kwh_consumed": "Total kWh Consumed"}, inplace=True)
            
            # Create continuous plot data by connecting the last finalized BATCH point to the live SPEED point
            batch_rows = chart_df[chart_df["source_layer"] == "BATCH"].sort_values("Window Start")
            speed_rows = chart_df[chart_df["source_layer"] == "SPEED"].sort_values("Window Start")
            
            plot_rows = []
            for _, r in batch_rows.iterrows():
                plot_rows.append(r.to_dict())
                
            if not batch_rows.empty and not speed_rows.empty:
                # Add the last batch point as an anchor for the SPEED line so it draws a continuous line segment
                last_batch_anchor = batch_rows.iloc[-1].to_dict()
                last_batch_anchor["source_layer"] = "SPEED"
                plot_rows.append(last_batch_anchor)
                
            for _, r in speed_rows.iterrows():
                plot_rows.append(r.to_dict())
                
            chart_data = pd.DataFrame(plot_rows).drop_duplicates() if plot_rows else chart_df.dropna(subset=["Total kWh Consumed"])
            
            import altair as alt
            chart = alt.Chart(chart_data).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X("Window Start:N", title="Window Start Time", sort=None),
                y=alt.Y("Total kWh Consumed:Q", title="Total Consumption (kWh)"),
                color=alt.Color("source_layer:N", title="Layer Source", 
                                scale=alt.Scale(domain=["BATCH", "SPEED"], range=["#1f77b4", "#ff7f0e"])),
                tooltip=["Window Start", "source_layer", "Total kWh Consumed"]
            ).properties(height=350).interactive()
            
            st.altair_chart(chart, use_container_width=True)

            # Display raw records table
            st.subheader("📋 Unified Query Result Set")
            st.dataframe(
                df[["window_start", "window_end", "ward_id", "total_kwh_consumed", "avg_power_factor", "peak_voltage", "source_layer"]],
                use_container_width=True
            )

# ============================================================
# Phase 9: Kafka Explorer
# ============================================================
elif selected_tab.startswith("9."):
    st.header("🔎 Kafka Explorer — Live Topic Browser")
    st.markdown("Browse all UrbanPulse Kafka topics, view partition metadata, and inspect the latest messages in real-time.")

    import pandas as pd

    # --- Helper: fetch topic metadata and messages ---
    @st.cache_data(ttl=10)
    def _kafka_topic_list(bootstrap: str):
        """Return a sorted list of UrbanPulse topic names."""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            topics = sorted([t for t in consumer.topics() if not t.startswith("__")])
            consumer.close()
            return topics, None
        except Exception as e:
            return [], str(e)

    @st.cache_data(ttl=10)
    def _kafka_topic_info(bootstrap: str, topic: str):
        """Return partition info for a topic."""
        try:
            from kafka import KafkaConsumer, TopicPartition
            consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            partitions = consumer.partitions_for_topic(topic)
            if partitions is None:
                consumer.close()
                return [], None
            info = []
            for p in sorted(partitions):
                tp = TopicPartition(topic, p)
                consumer.assign([tp])
                consumer.seek_to_beginning(tp)
                earliest = consumer.position(tp)
                consumer.seek_to_end(tp)
                latest = consumer.position(tp)
                info.append({
                    "Partition": p,
                    "Earliest Offset": earliest,
                    "Latest Offset": latest,
                    "Messages (approx)": latest - earliest
                })
            consumer.close()
            return info, None
        except Exception as e:
            return [], str(e)

    def _kafka_read_messages(bootstrap: str, topic: str, max_messages: int = 50):
        """Read the latest N messages from all partitions of a topic."""
        try:
            from kafka import KafkaConsumer, TopicPartition
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap,
                auto_offset_reset='latest',
                consumer_timeout_ms=3000,
                value_deserializer=lambda m: m.decode('utf-8', errors='replace'),
                key_deserializer=lambda m: m.decode('utf-8', errors='replace') if m else None,
                request_timeout_ms=5000
            )
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.close()
                return [], None

            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(tps)

            # Seek each partition back by max_messages // num_partitions
            per_part = max(max_messages // len(tps), 1)
            for tp in tps:
                consumer.seek_to_end(tp)
                end = consumer.position(tp)
                start = max(0, end - per_part)
                consumer.seek(tp, start)

            msgs = []
            for msg in consumer:
                record = {
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key,
                    "timestamp": datetime.fromtimestamp(msg.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S') if msg.timestamp and msg.timestamp > 0 else "N/A",
                    "value": msg.value
                }
                msgs.append(record)
                if len(msgs) >= max_messages:
                    break
            consumer.close()
            return msgs, None
        except Exception as e:
            return [], str(e)

    # --- UI ---
    topics, err = _kafka_topic_list(KAFKA_BOOTSTRAP)

    if err:
        st.error(f"❌ Cannot connect to Kafka at `{KAFKA_BOOTSTRAP}`: {err}")
        st.info("Make sure Docker infrastructure is running (Phase 1).")
    elif not topics:
        st.warning("No topics found. Run `cluster_setup.sh` first (Phase 1) to create UrbanPulse topics.")
    else:
        # Topic overview metrics
        st.markdown("### 📋 Cluster Topic Overview")
        urbanpulse_topics = [t for t in topics if t.startswith("urbanpulse.")]
        other_topics = [t for t in topics if not t.startswith("urbanpulse.")]

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Topics", len(topics))
        m2.metric("UrbanPulse Topics", len(urbanpulse_topics))
        m3.metric("Other Topics", len(other_topics))

        st.markdown("---")

        # Topic selector
        col_topic, col_count = st.columns([3, 1])
        with col_topic:
            selected_topic = st.selectbox(
                "Select Topic to Explore:",
                topics,
                format_func=lambda t: f"📨 {t}" if t.startswith("urbanpulse.") else t
            )
        with col_count:
            msg_count = st.number_input("Messages to fetch:", min_value=5, max_value=200, value=25, step=5)

        if selected_topic:
            # Partition info table
            st.markdown(f"### 📊 Partition Details — `{selected_topic}`")
            part_info, part_err = _kafka_topic_info(KAFKA_BOOTSTRAP, selected_topic)
            if part_err:
                st.error(f"Error reading partition info: {part_err}")
            elif part_info:
                part_df = pd.DataFrame(part_info)
                total_msgs = part_df["Messages (approx)"].sum()
                pc1, pc2 = st.columns(2)
                pc1.metric("Partitions", len(part_info))
                pc2.metric("Total Messages (approx)", f"{total_msgs:,}")
                st.dataframe(part_df, use_container_width=True, hide_index=True)
            else:
                st.info("No partition data available.")

            st.markdown("---")

            # Fetch and display messages
            st.markdown(f"### 📬 Latest Messages — `{selected_topic}`")
            fetch_btn = st.button("🔄 Fetch Latest Messages", use_container_width=True, type="primary")

            if fetch_btn:
                with st.spinner(f"Reading latest {msg_count} messages from `{selected_topic}`..."):
                    messages, msg_err = _kafka_read_messages(KAFKA_BOOTSTRAP, selected_topic, msg_count)

                if msg_err:
                    st.error(f"Error reading messages: {msg_err}")
                elif not messages:
                    st.info(f"No messages found in `{selected_topic}`. Make sure producers are running.")
                else:
                    st.success(f"✅ Fetched {len(messages)} messages from `{selected_topic}`")

                    # Summary table with compact view
                    summary_data = []
                    for m in messages:
                        # Try to parse JSON for pretty display
                        try:
                            parsed = json.loads(m['value'])
                            preview = json.dumps(parsed, indent=None)[:120] + ("…" if len(m['value']) > 120 else "")
                        except (json.JSONDecodeError, TypeError):
                            preview = str(m['value'])[:120]
                            parsed = None
                        summary_data.append({
                            "Partition": m['partition'],
                            "Offset": m['offset'],
                            "Key": m['key'] or "—",
                            "Timestamp": m['timestamp'],
                            "Value (Preview)": preview
                        })

                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

                    # Expandable JSON viewer for each message
                    st.markdown("#### 🔍 Detailed Message Inspector")
                    for i, m in enumerate(messages):
                        label = f"Partition {m['partition']} | Offset {m['offset']} | Key: {m['key'] or '—'} | {m['timestamp']}"
                        with st.expander(label, expanded=(i == 0)):
                            try:
                                parsed = json.loads(m['value'])
                                st.json(parsed)
                            except (json.JSONDecodeError, TypeError):
                                st.code(m['value'], language="text")


# ============================================================
# Phase 10: Component Web UIs & Ports Matrix
# ============================================================
elif selected_tab.startswith("10."):
    st.header("🌐 Platform Component Web UIs & Service Ports")
    st.markdown("Direct links to all operational web interfaces, administrative consoles, and storage endpoints running in Docker.")

    st.markdown("""
    | Service Component | Host Port | Direct Web Access Link | Authentication / Credentials | Description |
    | :--- | :--- | :--- | :--- | :--- |
    | **Grafana Dashboards** | `3000` | [http://localhost:3000](http://localhost:3000) | `admin` / `urbanpulse_2026` | Real-time traffic, AQI heatmaps, and bus tracking dashboards |
    | **Apache Flink Dashboard** | `8081` | [http://localhost:8081](http://localhost:8081) | None (Public UI) | PyFlink DataStream job graph, TaskManagers, and checkpoint metrics |
    | **Apache Spark Master UI** | `8080` | [http://localhost:8080](http://localhost:8080) | None (Public UI) | Spark Structured Streaming active queries, stages, and worker nodes |
    | **MinIO Object Console** | `9001` | [http://localhost:9001](http://localhost:9001) | `urbanpulse` / `urbanpulse_2026` | S3 object browser for historical Parquet datasets |
    | **MinIO S3 API Endpoint** | `9000` | `http://localhost:9000` | `urbanpulse` / `urbanpulse_2026` | S3 API endpoint for Spark/Flink object storage writes |
    | **TimescaleDB / PostGIS** | `5432` | `localhost:5432` | `urbanpulse` / `urbanpulse_2026` (`urbanpulse_db`) | PostgreSQL engine for time-series & spatial data |
    | **Kafka Broker 1 (KRaft)** | `9092` | `localhost:9092` | None (PLAINTEXT) | Primary bootstrap broker endpoint for Python producers/consumers |
    """)

    st.subheader("🔑 Access Quick Reference Cards")
    col1, col2 = st.columns(2)
    with col1:
        st.info("""
        ### 📊 Grafana
        - **URL:** [http://localhost:3000](http://localhost:3000)
        - **Username:** `admin`
        - **Password:** `urbanpulse_2026`
        """)
        st.info("""
        ### ⚡ Apache Flink Web UI
        - **URL:** [http://localhost:8081](http://localhost:8081)
        - **Auth:** Open Access
        """)
    with col2:
        st.info("""
        ### 🪣 MinIO Console
        - **URL:** [http://localhost:9001](http://localhost:9001)
        - **Access Key:** `urbanpulse`
        - **Secret Key:** `urbanpulse_2026`
        """)
        st.info("""
        ### 🔥 Apache Spark Master UI
        - **URL:** [http://localhost:8080](http://localhost:8080)
        - **Auth:** Open Access
        """)

    st.markdown("---")
    st.subheader("⚡ Live TimescaleDB & Grafana Ingestion Daemon Controller")
    st.markdown("Run this daemon to continuously stream live Kafka events into TimescaleDB/PostgreSQL so Grafana dashboards automatically update with real-time telemetry.")

    render_script_controller(
        title="TimescaleDB / Grafana Telemetry Ingest Daemon",
        script_rel_path="urbanpulse/timescale_grafana_ingest.py",
        desc="Consumes from `urbanpulse.air_quality`, `traffic_signals`, `bus_gps`, `smart_meters`, and `incidents` and writes structured rows into TimescaleDB tables for Grafana.",
        process_key="timescale_ingest",
        default_cmd=f"{sys.executable} urbanpulse/timescale_grafana_ingest.py",
        script_type="generic"
    )

# ============================================================
# Auto-Refresh Handler
# ============================================================
if auto_refresh:
    # Check if any process is currently running
    any_running = any(pm.is_running(k) for k in pm.processes)
    if any_running:
        time.sleep(5)
        st.rerun()
