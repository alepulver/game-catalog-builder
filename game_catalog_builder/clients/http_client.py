from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from ..config import REQUEST, RETRY
from ..utils.utilities import (
    RateLimiter,
    network_failures_count,
    raise_on_new_network_failure,
    with_retries,
)


@dataclass
class HTTPJSONClient:
    """
    Small helper to standardize request + retry + rate limiting + stats counting.

    Provider clients pass in their own `requests.Session`, `stats` dict, and the desired
    rate limiter + counter key per endpoint.
    """

    session: requests.Session
    stats: dict[str, Any] | None = None

    def _bump(self, key: str) -> None:
        if self.stats is None:
            return
        self.stats[key] = int(self.stats.get(key, 0) or 0) + 1

    def _bump_ms(self, key: str, elapsed_ms: int) -> None:
        if self.stats is None:
            return
        ms_key = f"{key}_ms"
        self.stats[ms_key] = int(self.stats.get(ms_key, 0) or 0) + int(elapsed_ms)

    @staticmethod
    def format_timing(stats: dict[str, Any] | None, *, key: str) -> str:
        """
        Format request counter for a key tracked via `_bump()`.
        """
        if not stats:
            return f"{key}=0"
        return f"{key}={int(stats.get(key, 0) or 0)}"

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        status_handlers: dict[int, Any] | None = None,
        ratelimiter: RateLimiter | None = None,
        timeout_s: float = REQUEST.timeout_s,
        retries: int = RETRY.retries,
        base_sleep_s: float = RETRY.base_sleep_s,
        counter_key: str = "http_get",
        context: str,
        on_fail_return: Any = None,
    ) -> Any:
        before_net = network_failures_count(self.stats)

        def _request() -> Any:
            if ratelimiter is not None:
                ratelimiter.wait()
            self._bump(counter_key)
            kwargs: dict[str, Any] = {"timeout": timeout_s}
            if params is not None:
                kwargs["params"] = params
            if headers is not None:
                kwargs["headers"] = headers
            t0 = time.perf_counter()
            r = self.session.get(url, **kwargs)
            t1 = time.perf_counter()
            self._bump_ms(counter_key, int(round((t1 - t0) * 1000.0)))
            if status_handlers is not None and r.status_code in status_handlers:
                return status_handlers[r.status_code]
            r.raise_for_status()
            return r.json()

        data = with_retries(
            _request,
            retries=retries,
            base_sleep_s=base_sleep_s,
            on_fail_return=on_fail_return,
            context=context,
            retry_stats=self.stats,
        )
        if data is on_fail_return:
            raise_on_new_network_failure(self.stats, before=before_net, context=context)
        return data

    def post_json(
        self,
        url: str,
        *,
        data: dict[str, Any] | str | None = None,
        json_body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        status_handlers: dict[int, Any] | None = None,
        ratelimiter: RateLimiter | None = None,
        timeout_s: float = REQUEST.timeout_s,
        retries: int = RETRY.retries,
        base_sleep_s: float = RETRY.base_sleep_s,
        counter_key: str = "http_post",
        context: str,
        on_fail_return: Any = None,
    ) -> Any:
        before_net = network_failures_count(self.stats)

        def _request() -> Any:
            if ratelimiter is not None:
                ratelimiter.wait()
            self._bump(counter_key)
            kwargs: dict[str, Any] = {"timeout": timeout_s}
            if params is not None:
                kwargs["params"] = params
            if headers is not None:
                kwargs["headers"] = headers
            if data is not None:
                kwargs["data"] = data
            if json_body is not None:
                kwargs["json"] = json_body
            t0 = time.perf_counter()
            r = self.session.post(url, **kwargs)
            t1 = time.perf_counter()
            self._bump_ms(counter_key, int(round((t1 - t0) * 1000.0)))
            if status_handlers is not None and r.status_code in status_handlers:
                return status_handlers[r.status_code]
            r.raise_for_status()
            return r.json()

        resp = with_retries(
            _request,
            retries=retries,
            base_sleep_s=base_sleep_s,
            on_fail_return=on_fail_return,
            context=context,
            retry_stats=self.stats,
        )
        if resp is on_fail_return:
            raise_on_new_network_failure(self.stats, before=before_net, context=context)
        return resp


@dataclass
class HTTPRequestDefaults:
    ratelimiter: RateLimiter | None = None
    timeout_s: float = REQUEST.timeout_s
    retries: int = RETRY.retries
    base_sleep_s: float = RETRY.base_sleep_s
    headers: dict[str, str] | None = None
    status_handlers: dict[int, Any] | None = None
    counter_key: str = "http_get"
    context_prefix: str | None = None


@dataclass
class ConfiguredHTTPJSONClient:
    """
    Convenience wrapper over HTTPJSONClient that carries default parameters.

    This keeps provider code concise by instantiating a per-endpoint client configured with
    its rate limiter, counter key, retry policy, etc.
    """

    http: HTTPJSONClient
    defaults: HTTPRequestDefaults

    def _ctx(self, context: str) -> str:
        prefix = self.defaults.context_prefix
        if prefix:
            return f"{prefix}{': ' if context else ''}{context}"
        return context

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        status_handlers: dict[int, Any] | None = None,
        counter_key: str | None = None,
        context: str = "",
        on_fail_return: Any = None,
    ) -> Any:
        merged_headers = self.defaults.headers if headers is None else headers
        merged_status = self.defaults.status_handlers if status_handlers is None else status_handlers
        return self.http.get_json(
            url,
            params=params,
            headers=merged_headers,
            status_handlers=merged_status,
            ratelimiter=self.defaults.ratelimiter,
            timeout_s=self.defaults.timeout_s,
            retries=self.defaults.retries,
            base_sleep_s=self.defaults.base_sleep_s,
            counter_key=counter_key or self.defaults.counter_key,
            context=self._ctx(context),
            on_fail_return=on_fail_return,
        )

    def post_json(
        self,
        url: str,
        *,
        data: dict[str, Any] | str | None = None,
        json_body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        status_handlers: dict[int, Any] | None = None,
        counter_key: str | None = None,
        context: str = "",
        on_fail_return: Any = None,
    ) -> Any:
        merged_headers = self.defaults.headers if headers is None else headers
        merged_status = self.defaults.status_handlers if status_handlers is None else status_handlers
        return self.http.post_json(
            url,
            data=data,
            json_body=json_body,
            params=params,
            headers=merged_headers,
            status_handlers=merged_status,
            ratelimiter=self.defaults.ratelimiter,
            timeout_s=self.defaults.timeout_s,
            retries=self.defaults.retries,
            base_sleep_s=self.defaults.base_sleep_s,
            counter_key=counter_key or self.defaults.counter_key,
            context=self._ctx(context),
            on_fail_return=on_fail_return,
        )
