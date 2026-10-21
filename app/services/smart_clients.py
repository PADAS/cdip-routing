import hashlib
import logging
import time
from typing import Dict, List, Tuple

from app import settings

logger = logging.getLogger(__name__)


class _Entry:
    def __init__(self, key: tuple, client):
        self.key = key
        self.client = client
        self.created_at = time.monotonic()
        self.leases = 0


class SmartClientPool:
    """
    Process-wide SMART Connect clients, one per destination configuration.

    Reusing a client keeps its logged-in session (JSESSIONID) across messages
    instead of logging in to SMART Connect for every observation, and bounds how
    many httpx clients (each with its own connection pool and SSL context) exist.
    """

    def __init__(self, *, ttl: int, max_size: int, failure_ttl: int):
        self.ttl = ttl
        self.max_size = max_size
        self.failure_ttl = failure_ttl
        self._entries: Dict[tuple, _Entry] = {}
        self._leased: Dict[int, _Entry] = {}
        self._retired: List[_Entry] = []
        self._failures: Dict[Tuple[tuple, str], float] = {}

    @staticmethod
    def _key(destination_id, api, username, password, version) -> tuple:
        # Any change to the destination's SMART settings must yield a new
        # session; the password is hashed so it never sits in a key or log line.
        password_digest = hashlib.sha256((password or "").encode()).hexdigest()
        return str(destination_id), api, username, password_digest, version

    def acquire(
        self, client_class, *, destination_id, api, username, password, version
    ):
        key = self._key(destination_id, api, username, password, version)
        now = time.monotonic()
        entry = self._entries.get(key)
        if entry and now - entry.created_at >= self.ttl:
            self._retire(entry)
            entry = None
        if not entry:
            for other in list(self._entries.values()):
                if other.key[0] == key[0]:
                    self._retire(other)
            if len(self._entries) >= self.max_size:
                self._retire(min(self._entries.values(), key=lambda e: e.created_at))
            entry = _Entry(
                key,
                client_class(
                    api=api, username=username, password=password, version=version
                ),
            )
            self._entries[key] = entry
        entry.leases += 1
        self._leased[id(entry.client)] = entry
        return entry.client

    async def release(self, client):
        entry = self._leased.get(id(client))
        if entry:
            entry.leases -= 1
        closable = [e for e in self._retired if e.leases <= 0]
        self._retired = [e for e in self._retired if e.leases > 0]
        for retired in closable:
            await self._close(retired)

    def invalidate(self, client):
        """Stop handing out this client, e.g. after its session failed to log in."""
        entry = self._leased.get(id(client))
        if entry and self._entries.get(entry.key) is entry:
            self._retire(entry)

    def record_failure(self, client, ca_uuid: str):
        entry = self._leased.get(id(client))
        if not entry:
            return
        now = time.monotonic()
        self._failures = {k: v for k, v in self._failures.items() if v > now}
        self._failures[(entry.key, str(ca_uuid))] = now + self.failure_ttl
        self.invalidate(client)

    def recent_failure(self, client, ca_uuid: str) -> bool:
        entry = self._leased.get(id(client))
        if not entry:
            return False
        expires_at = self._failures.get((entry.key, str(ca_uuid)))
        return bool(expires_at and expires_at > time.monotonic())

    def _retire(self, entry: _Entry):
        self._entries.pop(entry.key, None)
        self._retired.append(entry)

    async def _close(self, entry: _Entry):
        self._leased.pop(id(entry.client), None)
        try:
            await entry.client.close()
        except Exception:
            logger.warning(
                "Failed closing SMART client for %s", entry.key[1], exc_info=True
            )


smart_clients = SmartClientPool(
    ttl=settings.SMART_CLIENT_TTL,
    max_size=settings.SMART_CLIENT_POOL_MAX_SIZE,
    failure_ttl=settings.SMART_DATAMODEL_FAILURE_TTL,
)
