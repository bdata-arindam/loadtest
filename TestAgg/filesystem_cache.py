import os
import json
import hashlib

class FilesystemCache:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, key):
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{key_hash}.json")

    def get(self, key):
        cache_path = self._get_cache_path(key)
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as file:
                return json.load(file)
        return None

    def set(self, key, value):
        cache_path = self._get_cache_path(key)
        with open(cache_path, 'w') as file:
            json.dump(value, file)

    def remove(self, key):
        cache_path = self._get_cache_path(key)
        if os.path.exists(cache_path):
            os.remove(cache_path)

    def clear(self):
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            os.remove(file_path)

class FilesystemCacheManager:
    def __init__(self, config):
        self.use_filesystem_cache = config.get("use_filesystem_cache", False)
        self.cache_dir = config.get("cache_dir", "cache_data")
        self.filesystem_cache = FilesystemCache(self.cache_dir)

    def get(self, key):
        if self.use_filesystem_cache:
            return self.filesystem_cache.get(key)
        return None

    def set(self, key, value):
        if self.use_filesystem_cache:
            self.filesystem_cache.set(key, value)

    def remove(self, key):
        if self.use_filesystem_cache:
            self.filesystem_cache.remove(key)

    def clear(self):
        if self.use_filesystem_cache:
            self.filesystem_cache.clear()