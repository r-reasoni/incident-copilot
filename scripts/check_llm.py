"""Send a minimal Gemini request without displaying credentials or output text."""
from __future__ import annotations

import sys

from app.agent.llm import LLMConfigurationError, get_llm
from app.config import get_settings


def main() -> int:
    if get_settings().llm_provider.lower() != "gemini":
        print("FAIL: set LLM_PROVIDER=gemini to check Gemini connectivity")
        return 1
    try:
        response = get_llm().generate(
            "Reply with the single word OK.",
            [{"role": "user", "content": "Reply with the single word OK"}],
        )
    except LLMConfigurationError as exc:
        reason = "Gemini configuration or SDK unavailable"
        if "GEMINI_API_KEY" in str(exc):
            reason = "GEMINI_API_KEY is missing or still a placeholder"
        print(f"FAIL: {reason}")
        return 1
    except Exception as exc:
        print(f"FAIL: request error ({type(exc).__name__})")
        return 1

    if response.strip().upper() == "OK":
        print("PASS")
        return 0
    print("FAIL: response was not the single word OK")
    return 1


if __name__ == "__main__":
    sys.exit(main())