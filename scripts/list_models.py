"""List Gemini models available to the configured key for content generation."""
from __future__ import annotations

import sys

from app.config import get_settings


def main() -> int:
    settings = get_settings()
    if not settings.gemini_api_key or settings.gemini_api_key == "your_key_here":
        print("FAIL: GEMINI_API_KEY is missing or still a placeholder")
        return 1

    try:
        from google import genai

        client = genai.Client(api_key=settings.gemini_api_key)
        available = []
        for model in client.models.list():
            methods = list(getattr(model, "supported_actions", None) or [])
            if any(method.lower() == "generatecontent" for method in methods):
                available.append((model.name or "", methods))
    except Exception as error:
        print(f"FAIL: model listing request error ({type(error).__name__})")
        return 1

    for name, methods in sorted(available):
        print(f"{name}: {', '.join(methods)}")
    if not available:
        print("No available models support generateContent.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())