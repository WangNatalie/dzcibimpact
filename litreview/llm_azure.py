"""Azure OpenAI client wrapper for classification and extraction.

Thin layer over the openai SDK's AzureOpenAI client so the rest of the pipeline
calls one function and gets parsed JSON back. Credentials come from the
environment (see config.Settings / .env.example). Designed to fail loudly with
a clear message if creds are missing, and to be resilient to the model wrapping
JSON in prose or code fences.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any, Optional

from .config import SETTINGS

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


class AzureNotConfigured(RuntimeError):
    pass


@lru_cache(maxsize=1)
def _client():
    if not (SETTINGS.azure_openai_endpoint and SETTINGS.azure_openai_api_key
            and SETTINGS.azure_openai_deployment):
        raise AzureNotConfigured(
            "Azure OpenAI not configured. Set AZURE_OPENAI_ENDPOINT, "
            "AZURE_OPENAI_API_KEY, and AZURE_OPENAI_MODEL/DEPLOYMENT "
            "(see litreview/.env.example)."
        )
    from openai import AzureOpenAI
    return AzureOpenAI(
        azure_endpoint=SETTINGS.azure_openai_endpoint,
        api_key=SETTINGS.azure_openai_api_key,
        api_version=SETTINGS.azure_openai_api_version,
    )


def is_configured() -> bool:
    try:
        _client()
        return True
    except AzureNotConfigured:
        return False


def _extract_json(text: str) -> Any:
    """Parse JSON from a model reply that may be fenced or prose-wrapped."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = _FENCE_RE.search(text)
    if m:
        return json.loads(m.group(1))
    # last resort: grab the outermost {...}
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        return json.loads(text[start:end + 1])
    raise ValueError(f"No JSON found in model reply: {text[:200]!r}")


def complete_json(
    system: str,
    user: str,
    *,
    temperature: float = 0.0,
    max_tokens: int = 1500,
    json_mode: bool = True,
) -> Any:
    """Run one chat completion and return parsed JSON."""
    client = _client()
    kwargs: dict[str, Any] = {
        "model": SETTINGS.azure_openai_deployment,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    try:
        resp = client.chat.completions.create(**kwargs)
    except Exception:
        # Some deployments reject response_format; retry without it.
        if json_mode:
            kwargs.pop("response_format", None)
            resp = client.chat.completions.create(**kwargs)
        else:
            raise
    return _extract_json(resp.choices[0].message.content or "")


def complete_text(system: str, user: str, *, temperature: float = 0.0,
                  max_tokens: int = 1500) -> str:
    client = _client()
    resp = client.chat.completions.create(
        model=SETTINGS.azure_openai_deployment,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        temperature=temperature, max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""
