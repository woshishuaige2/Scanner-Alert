"""
Structured runtime telemetry for the scanner and trading bot.
Writes JSONL events plus a periodically refreshed JSON state snapshot.
"""
import json
import os
import threading
import time
from datetime import datetime

'''
runtime_events.jsonl: best for post-run diagnosis, tells you what happened and why
runtime_state.json: best for “what is the bot/scanner doing right now?”, current pending entries, positions, top symbols, recent alerts
'''

def _sanitize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


class RuntimeTelemetry:
    def __init__(self, component: str, base_dir: str):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.component = component
        self.component_dir = os.path.join(base_dir, component)
        self.run_dir = os.path.join(self.component_dir, f"{component}_{timestamp}")
        self.events_path = os.path.join(self.run_dir, "runtime_events.jsonl")
        self.state_path = os.path.join(self.run_dir, "runtime_state.json")
        self.meta_path = os.path.join(self.run_dir, "run_metadata.json")
        self.lock = threading.Lock()

        os.makedirs(self.component_dir, exist_ok=True)
        os.makedirs(self.run_dir, exist_ok=True)
        metadata = {
            "component": component,
            "started_at": datetime.now().isoformat(),
            "run_dir": self.run_dir,
        }
        with open(self.meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    def _write_json_snapshot(self, target_path: str, payload: dict, required: bool = True) -> bool:
        temp_path = f"{target_path}.{os.getpid()}.{threading.get_ident()}.tmp"
        last_error = None
        for attempt in range(3):
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                os.replace(temp_path, target_path)
                return True
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.05 * (attempt + 1))
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

        if required and last_error:
            raise last_error
        if last_error:
            print(f"[WARNING] Could not update shared runtime state {target_path}: {last_error}")
        return False

    def log_event(self, event_type: str, **payload):
        event = {
            "timestamp": datetime.now().isoformat(),
            "component": self.component,
            "event_type": event_type,
            **_sanitize(payload),
        }
        with self.lock:
            with open(self.events_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=True) + "\n")

    def write_state(self, state: dict):
        payload = {
            "timestamp": datetime.now().isoformat(),
            "component": self.component,
            "state": _sanitize(state),
        }
        with self.lock:
            self._write_json_snapshot(self.state_path, payload, required=True)
