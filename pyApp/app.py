import json
import logging
import os
import random
import signal
import sys
import time

from prometheus_client import Counter, Gauge, start_http_server

SERVICE = os.environ.get("SERVICE_NAME", "5g-session-analyzer")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
INTERVAL_MIN = float(os.environ.get("LOG_INTERVAL_MIN", "0.5"))
INTERVAL_MAX = float(os.environ.get("LOG_INTERVAL_MAX", "2.0"))

# Fixed label sets. Random gNodeB IDs would create hundreds of Prometheus series.
SLICES = ("sst-1", "sst-2", "sst-3")
GNBS = ("gNodeB-101", "gNodeB-202", "gNodeB-303")

EVENTS = Counter(
    "session_events_total",
    "Log events emitted by the session analyzer",
    ["level", "slice_id"],
)
ACTIVE_UE = Gauge(
    "active_ue_connections",
    "Active UE connections on a RAN slice",
    ["slice_id", "gnb_id"],
)
LATENCY = Gauge(
    "network_latency_ms",
    "Last observed RAN latency in milliseconds",
    ["slice_id", "gnb_id"],
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
logger = logging.getLogger(SERVICE)
logger.setLevel(logging.INFO)
logger.propagate = False
logger.addHandler(handler)

stop = False

# Salvăm momentul pornirii pentru a ști când să declanșăm incidentul
START_TIME = time.time()

def handle_stop(signum, _frame):
    global stop
    stop = True
    emit("INFO", SLICES[0], GNBS[0], 0, 0, f"shutdown signal {signum}")

def emit(level, slice_id, gnb_id, active_ue, latency_ms, message):
    EVENTS.labels(level=level, slice_id=slice_id).inc()
    ACTIVE_UE.labels(slice_id=slice_id, gnb_id=gnb_id).set(active_ue)
    LATENCY.labels(slice_id=slice_id, gnb_id=gnb_id).set(latency_ms)
    logger.info(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "service": SERVICE,
        "slice_id": slice_id,
        "gnb_id": gnb_id,
        "active_ue_connections": active_ue,
        "latency_ms": latency_ms,
        "message": message,
    }))

def next_event():
    # Fixăm un slice și un gnb specific pentru a concentra erorile într-o singură serie de date
    slice_id = SLICES[0]
    gnb_id = GNBS[0]

    # ---------------------------------------------------------------------------------
    # SIMULARE DE INCIDENT (CHAOS ENGINEERING)
    # ---------------------------------------------------------------------------------
    # În primele 30 de secunde raportăm trafic sănătos (UE=5000, Latență=20ms).
    # Acest pas este necesar ca Prometheus să înregistreze starea `max_over_time > 1000`.
    if time.time() - START_TIME < 30:
        level = "INFO"
        active_ue = 5000
        latency_ms = 20
        message = "[WARMUP] Trafic normal generat pentru baseline-ul Prometheus..."
    else:
        # După 30 de secunde, declanșăm colapsul total pe toate metricele simultan.
        level = "ERROR"         # Declanșează HighApplicationErrorLogs (Rată de eroare 100%)
        active_ue = 50          # Declanșează MassiveUEDrop (A scazut sub 100 din 5000)
        latency_ms = 250        # Declanșează HighRANLatency (Sare peste 150ms)
        message = "[SIMULARE INCIDENT CRITIC] Drop masiv de conexiuni și latență critică!"

    return level, slice_id, gnb_id, active_ue, latency_ms, message

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    start_http_server(METRICS_PORT)
    emit("INFO", SLICES[0], GNBS[0], 0, 0, f"metrics listening on {METRICS_PORT}")
    while not stop:
        emit(*next_event())
        time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))
