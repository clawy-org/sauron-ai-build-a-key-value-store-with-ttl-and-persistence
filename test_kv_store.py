"""Tests for kv_store.py"""

import json
import os
import tempfile
import threading
import time
import unittest

from kv_store import KVStore


class TestKVStore(unittest.TestCase):

    def test_get_set(self):
        s = KVStore()
        s.set("a", 1)
        self.assertEqual(s.get("a"), 1)

    def test_get_default(self):
        s = KVStore()
        self.assertIsNone(s.get("nope"))
        self.assertEqual(s.get("nope", 42), 42)

    def test_delete(self):
        s = KVStore()
        s.set("a", 1)
        self.assertTrue(s.delete("a"))
        self.assertIsNone(s.get("a"))
        self.assertFalse(s.delete("a"))

    def test_keys(self):
        s = KVStore()
        s.set("x", 1)
        s.set("y", 2)
        self.assertEqual(sorted(s.keys()), ["x", "y"])

    def test_clear(self):
        s = KVStore()
        s.set("a", 1)
        s.set("b", 2)
        s.clear()
        self.assertEqual(s.keys(), [])

    def test_ttl_expiry(self):
        s = KVStore()
        s.set("temp", "val", ttl_sec=0.1)
        self.assertEqual(s.get("temp"), "val")
        time.sleep(0.15)
        self.assertIsNone(s.get("temp"))

    def test_ttl_not_expired(self):
        s = KVStore()
        s.set("temp", "val", ttl_sec=10)
        self.assertEqual(s.get("temp"), "val")

    def test_overwrite_removes_ttl(self):
        s = KVStore()
        s.set("a", 1, ttl_sec=0.1)
        s.set("a", 2)  # no TTL
        time.sleep(0.15)
        self.assertEqual(s.get("a"), 2)

    def test_search_prefix(self):
        s = KVStore()
        s.set("user.name", "habie")
        s.set("user.age", 1)
        s.set("config.debug", True)
        self.assertEqual(sorted(s.search("user.")), ["user.age", "user.name"])

    def test_search_no_match(self):
        s = KVStore()
        s.set("a", 1)
        self.assertEqual(s.search("zzz"), [])

    def test_save_load(self):
        s = KVStore()
        s.set("name", "habie")
        s.set("count", 42)
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        s.save(path)
        s2 = KVStore()
        s2.load(path)
        self.assertEqual(s2.get("name"), "habie")
        self.assertEqual(s2.get("count"), 42)
        os.unlink(path)

    def test_save_skips_non_serializable(self):
        s = KVStore()
        s.set("ok", "value")
        s.set("bad", lambda x: x)  # not JSON serializable
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        s.save(path)
        with open(path) as f:
            data = json.load(f)
        self.assertIn("ok", data["data"])
        self.assertNotIn("bad", data["data"])
        os.unlink(path)

    def test_auto_save(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        s = KVStore(auto_save=path)
        s.set("x", 99)
        with open(path) as f:
            data = json.load(f)
        self.assertEqual(data["data"]["x"], 99)
        os.unlink(path)

    def test_auto_save_load_on_init(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump({"data": {"preloaded": "yes"}, "expiry": {}}, f)
            path = f.name
        s = KVStore(auto_save=path)
        self.assertEqual(s.get("preloaded"), "yes")
        os.unlink(path)

    def test_stats(self):
        s = KVStore()
        s.set("a", 1)
        s.set("b", 2)
        st = s.stats
        self.assertEqual(st["total_keys"], 2)
        self.assertEqual(st["expired_keys"], 0)
        self.assertGreater(st["size_bytes"], 0)

    def test_stats_expired(self):
        s = KVStore()
        s.set("temp", "x", ttl_sec=0.05)
        time.sleep(0.1)
        st = s.stats
        self.assertEqual(st["expired_keys"], 1)

    def test_thread_safety(self):
        s = KVStore()
        def writer(prefix):
            for i in range(100):
                s.set(f"{prefix}-{i}", i)
        threads = [threading.Thread(target=writer, args=(f"t{n}",)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(s.keys()), 400)


if __name__ == "__main__":
    unittest.main()
