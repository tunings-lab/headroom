"""Pure LiteLLM model-name resolution rules."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LiteLLMModelPrefixRule:
    """Case-insensitive bare-model prefix mapping to a LiteLLM provider key."""

    model_prefix: str
    provider_prefix: str

    def candidate_for(self, model: str) -> str | None:
        if model.lower().startswith(self.model_prefix):
            return f"{self.provider_prefix}{model}"
        return None


# Aliases for models removed from LiteLLM's cost database (retired/renamed).
# Maps old model name -> current LiteLLM key that has equivalent pricing.
MODEL_ALIASES: dict[str, str] = {
    # Claude 3.5 Sonnet retired Feb 2026, pricing same as claude-sonnet-4-20250514
    "claude-3-5-sonnet-20241022": "claude-sonnet-4-20250514",
    "claude-3-5-sonnet-20240620": "claude-sonnet-4-20250514",
    # Claude 3 Sonnet retired. It was a Sonnet-tier model ($3/$15 per 1M
    # in/out) — same price as claude-sonnet-4-20250514 — so alias it there.
    # The old target, claude-3-haiku-20240307 ($0.25/$1.25), is a different
    # (Haiku) tier and underpriced every cost/savings figure ~12x.
    "claude-3-sonnet-20240229": "claude-sonnet-4-20250514",
}


MODEL_PREFIX_RULES: tuple[LiteLLMModelPrefixRule, ...] = (
    LiteLLMModelPrefixRule("claude-", "anthropic/"),
    LiteLLMModelPrefixRule("gpt-", "openai/"),
    LiteLLMModelPrefixRule("o1-", "openai/"),
    LiteLLMModelPrefixRule("o3-", "openai/"),
    LiteLLMModelPrefixRule("o4-", "openai/"),
    LiteLLMModelPrefixRule("gemini-", "google/"),
    LiteLLMModelPrefixRule("minimax-", "minimax/"),
    LiteLLMModelPrefixRule("deepseek-", "deepseek/"),
)


PRICE_LOOKUP_PROVIDER_PREFIXES: tuple[str, ...] = (
    "openai/",
    "anthropic/",
    "google/",
    "mistral/",
    "deepseek/",
    "minimax/",
)


def resolution_candidates(model: str) -> tuple[str, ...]:
    """Return ordered LiteLLM keys to try for cost-per-token resolution."""
    candidates = [model]
    candidates.extend(
        candidate
        for rule in MODEL_PREFIX_RULES
        for candidate in (rule.candidate_for(model),)
        if candidate is not None
    )
    alias = MODEL_ALIASES.get(model)
    if alias:
        candidates.append(alias)
    # Self-hosted models often carry an org-style slash prefix (e.g.
    # "google/gemma-4-12B-it" from a local sglang/vLLM/TGI deployment)
    # that collides with LiteLLM's own provider-prefix parsing: LiteLLM's
    # get_llm_provider() only recognizes prefixes *it* maintains, so
    # "google/" here is misread as "call Google's Gemini API" instead of
    # being the model's literal id, and raises "LLM Provider NOT provided"
    # before pricing lookup even starts. Fall back to treating it as a
    # generic OpenAI-compatible upstream -- correct for anything routed
    # through an --openai-api-url passthrough proxy, which is the only
    # context this resolver runs in. Gated on "/" in model: bare names
    # (e.g. "MiniMax-M3") have no such collision and shouldn't get an
    # extra candidate. No-op unless a matching model_cost entry is
    # registered (see litellm_pricing.py).
    if "/" in model:
        candidates.append(f"openai/{model}")
    return tuple(dict.fromkeys(candidates))


def pricing_lookup_candidates(model: str) -> tuple[str, ...]:
    """Return ordered LiteLLM model_cost keys to try for pricing lookup."""
    candidates = [model]
    candidates.extend(f"{prefix}{model}" for prefix in PRICE_LOOKUP_PROVIDER_PREFIXES)
    alias = MODEL_ALIASES.get(model)
    if alias:
        candidates.append(alias)
    return tuple(dict.fromkeys(candidates))


def resolve_litellm_model_name(
    model: str,
    is_known_model: Callable[[str], bool],
) -> str:
    """Resolve ``model`` to the first candidate accepted by LiteLLM."""
    for candidate in resolution_candidates(model):
        if is_known_model(candidate):
            return candidate
    return model
