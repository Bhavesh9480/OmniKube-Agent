"""Pytest configuration and environment compatibility hooks."""

import httpx
import pytest

# Starlette 0.36.x compatibility fix with HTTPX 0.28+:
# Starlette passes app=... to httpx.Client.__init__, which HTTPX 0.28 deprecated/removed.
_orig_httpx_client_init = httpx.Client.__init__


def _compatible_httpx_client_init(self, *args, **kwargs):
    kwargs.pop("app", None)
    return _orig_httpx_client_init(self, *args, **kwargs)


httpx.Client.__init__ = _compatible_httpx_client_init
