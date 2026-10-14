"""Minimal Spark History Server REST client (stdlib only, with retries and size limits)."""

from __future__ import annotations

import base64
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .errors import HistoryServerError

log = logging.getLogger(__name__)

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.\-]+$")
DEFAULT_MAX_BYTES = 4 * 1024**3


class HistoryServerClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        basic_auth: tuple[str, str] | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        backoff: float = 1.0,
        max_download_bytes: int = DEFAULT_MAX_BYTES,
    ) -> None:
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise HistoryServerError(f"Invalid History Server URL '{base_url}' (expected http(s)://host:port)")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = max(1, retries)
        self.backoff = backoff
        self.max_download_bytes = max_download_bytes
        self._headers = {"Accept": "application/json", "User-Agent": "spark-telemetry-analyzer"}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        elif basic_auth:
            raw = base64.b64encode(f"{basic_auth[0]}:{basic_auth[1]}".encode()).decode()
            self._headers["Authorization"] = f"Basic {raw}"

    # ---- low level --------------------------------------------------------------------------
    def _open(self, url: str):  # type: ignore[no-untyped-def]
        last: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                req = urllib.request.Request(url, headers=self._headers)
                return urllib.request.urlopen(req, timeout=self.timeout)  # noqa: S310 (scheme validated)
            except urllib.error.HTTPError as exc:
                exc.close()
                if exc.code < 500 and exc.code != 429:
                    raise HistoryServerError(f"History Server returned HTTP {exc.code} for {url}") from exc
                last = exc
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
            if attempt < self.retries:
                delay = self.backoff * 2 ** (attempt - 1)
                log.warning("History Server request failed (%s); retry %d/%d in %.1fs", last, attempt, self.retries - 1, delay)
                time.sleep(delay)
        raise HistoryServerError(f"History Server unreachable after {self.retries} attempt(s): {last}")

    def _get_json(self, path: str, **params: Any) -> Any:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self.base_url}{path}" + (f"?{query}" if query else "")
        with self._open(url) as resp:
            try:
                return json.loads(resp.read().decode("utf-8"))
            except ValueError as exc:
                raise HistoryServerError(f"Non-JSON response from {url}") from exc

    # ---- API --------------------------------------------------------------------------------
    def list_applications(self, *, status: str = "completed", limit: int = 20, min_date: str | None = None) -> list[dict[str, Any]]:
        data = self._get_json("/api/v1/applications", status=status, limit=limit, minDate=min_date)
        if not isinstance(data, list):
            raise HistoryServerError("Unexpected response shape from /api/v1/applications")
        return data

    def download_event_log(self, app_id: str, dest_dir: str | Path, attempt_id: str | None = None) -> Path:
        """Stream the application's event-log zip into ``dest_dir`` and return its path."""
        if not _SAFE_ID.match(app_id) or (attempt_id and not _SAFE_ID.match(attempt_id)):
            raise HistoryServerError(f"Refusing unsafe application/attempt id: {app_id!r}/{attempt_id!r}")
        suffix = f"/{attempt_id}" if attempt_id else ""
        url = f"{self.base_url}/api/v1/applications/{urllib.parse.quote(app_id)}{suffix}/logs"
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        target = dest / f"{app_id}{'_' + attempt_id if attempt_id else ''}.zip"
        tmp = target.with_suffix(".zip.part")
        written = 0
        try:
            with self._open(url) as resp, tmp.open("wb") as out:
                while chunk := resp.read(1 << 20):
                    written += len(chunk)
                    if written > self.max_download_bytes:
                        raise HistoryServerError(f"Event log for {app_id} exceeds {self.max_download_bytes} bytes; aborting")
                    out.write(chunk)
            tmp.replace(target)
        finally:
            tmp.unlink(missing_ok=True)
        return target
