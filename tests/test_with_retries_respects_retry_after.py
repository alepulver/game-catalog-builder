from __future__ import annotations


def test_with_retries_respects_retry_after(monkeypatch):
    import requests

    from game_catalog_builder.utils.utilities import with_retries

    class Resp:
        status_code = 429
        headers = {"Retry-After": "0.02"}

    sleeps: list[float] = []

    def fake_sleep(s: float):
        sleeps.append(float(s))

    monkeypatch.setattr("time.sleep", fake_sleep)

    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            e = requests.exceptions.HTTPError("429")
            e.response = Resp()
            raise e
        return "ok"

    out = with_retries(
        fn,
        retries=2,
        base_sleep_s=0.0,
        jitter_s=0.0,
        retry_on=(requests.exceptions.HTTPError,),
        on_fail_return=None,
        context="test",
    )
    assert out == "ok"
    assert len(sleeps) == 1
    assert sleeps[0] >= 0.02
