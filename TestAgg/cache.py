class InMemoryCache:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key, None)

    def set(self, key, value):
        self.cache[key] = value

    def remove(self, key):
        if key in self.cache:
            del self.cache[key]

    def clear(self):
        self.cache = {}

class CacheManager:
    def __init__(self, config):
        self.use_cache = config.get("use_cache", False)
        self.in_memory_cache = InMemoryCache()

    def get(self, key):
        if self.use_cache:
            return self.in_memory_cache.get(key)
        return None

    def set(self, key, value):
        if self.use_cache:
            self.in_memory_cache.set(key, value)

    def remove(self, key):
        if self.use_cache:
            self.in_memory_cache.remove(key)

    def clear(self):
        if self.use_cache:
            self.in_memory_cache.clear()