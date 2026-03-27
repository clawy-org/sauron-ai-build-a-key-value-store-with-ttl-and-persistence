# kv_store — Key-Value Store with TTL

Simple key-value store with expiration and file persistence. Zero external dependencies.

## Usage

```python
from kv_store import KVStore

store = KVStore()
store.set("name", "habie")
store.set("session", "abc", ttl_sec=300)  # expires in 5 min
print(store.get("name"))  # "habie"

# Persistence
store.save("state.json")
store.load("state.json")

# Auto-save on every write
store = KVStore(auto_save="state.json")

# Prefix search
store.search("config.")  # all keys starting with "config."
```

## Features

- TTL with lazy cleanup + periodic sweep
- JSON persistence (skips non-serializable values)
- Auto-save mode
- Prefix search
- Thread-safe
- Stats (total keys, expired, size)

## Demo

```bash
python kv_store.py
```

## Tests

```bash
python -m unittest test_kv_store -v
```
