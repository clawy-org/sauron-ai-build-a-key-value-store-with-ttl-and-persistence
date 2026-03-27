#!/usr/bin/env python3
"""Key-value store with TTL and file persistence. Stdlib only."""

import json
import sys
import threading
import time
from typing import Any, Dict, List, Optional


class KVStore:
    """Thread-safe key-value store with TTL expiration and JSON persistence."""

    def __init__(self, auto_save: Optional[str] = None):
        self._data: Dict[str, Any] = {}
        self._expiry: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._auto_save_path = auto_save
        if auto_save:
            try:
                self.load(auto_save)
            except (FileNotFoundError, json.JSONDecodeError):
                pass

    def _is_expired(self, key: str) -> bool:
        if key in self._expiry:
            if time.time() > self._expiry[key]:
                return True
        return False

    def _clean_expired(self, key: str) -> None:
        if self._is_expired(key):
            del self._data[key]
            del self._expiry[key]

    def _maybe_auto_save(self) -> None:
        if self._auto_save_path:
            self.save(self._auto_save_path)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            self._clean_expired(key)
            return self._data.get(key, default)

    def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> None:
        with self._lock:
            self._data[key] = value
            if ttl_sec is not None:
                self._expiry[key] = time.time() + ttl_sec
            elif key in self._expiry:
                del self._expiry[key]
            self._maybe_auto_save()

    def delete(self, key: str) -> bool:
        with self._lock:
            existed = key in self._data
            self._data.pop(key, None)
            self._expiry.pop(key, None)
            if existed:
                self._maybe_auto_save()
            return existed

    def keys(self) -> List[str]:
        with self._lock:
            self._sweep()
            return list(self._data.keys())

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._expiry.clear()
            self._maybe_auto_save()

    def search(self, prefix: str) -> List[str]:
        with self._lock:
            self._sweep()
            return [k for k in self._data if k.startswith(prefix)]

    @property
    def stats(self) -> dict:
        with self._lock:
            now = time.time()
            expired = sum(1 for k, t in self._expiry.items() if now > t)
            total = len(self._data)
            size = sys.getsizeof(self._data)
            for v in self._data.values():
                size += sys.getsizeof(v)
            return {"total_keys": total, "expired_keys": expired, "size_bytes": size}

    def _sweep(self) -> None:
        now = time.time()
        expired_keys = [k for k, t in self._expiry.items() if now > t]
        for k in expired_keys:
            self._data.pop(k, None)
            self._expiry.pop(k, None)

    def save(self, path: str) -> None:
        with self._lock:
            self._sweep()
            serializable = {}
            for k, v in self._data.items():
                try:
                    json.dumps(v)
                    serializable[k] = v
                except (TypeError, ValueError):
                    pass
            payload = {"data": serializable, "expiry": {k: t for k, t in self._expiry.items() if k in serializable}}
            with open(path, "w") as f:
                json.dump(payload, f, indent=2)

    def load(self, path: str) -> None:
        with self._lock:
            with open(path) as f:
                payload = json.load(f)
            self._data = payload.get("data", {})
            self._expiry = {k: float(v) for k, v in payload.get("expiry", {}).items()}
            self._sweep()


if __name__ == "__main__":
    print("=== KVStore Demo ===")
    store = KVStore()
    store.set("name", "habie")
    store.set("temp", "goes away", ttl_sec=0.5)
    store.set("config.debug", True)
    store.set("config.verbose", False)

    print(f"name: {store.get('name')}")
    print(f"temp (before expiry): {store.get('temp')}")
    print(f"keys: {store.keys()}")
    print(f"search 'config.': {store.search('config.')}")
    print(f"stats: {store.stats}")

    import time; time.sleep(0.6)
    print(f"\ntemp (after expiry): {store.get('temp')}")
    print(f"keys after sweep: {store.keys()}")

    store.save("/tmp/kv_demo.json")
    print(f"saved to /tmp/kv_demo.json")

    store2 = KVStore()
    store2.load("/tmp/kv_demo.json")
    print(f"loaded name: {store2.get('name')}")
    print("\n--- Demo complete ---")
