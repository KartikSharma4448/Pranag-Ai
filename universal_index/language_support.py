from __future__ import annotations

import re

SELF_HEALING_LABEL = "self healing"

_LANG_HINTS_HI = {
    "सामग्री": "material",
    "पदार्थ": "material",
    "उष्मा": "heat",
    "गर्म": "high temperature",
    "स्व उपचार": SELF_HEALING_LABEL,
    "मिट्टी": "soil",
    "सिंचाई": "irrigation",
    "फसल": "crop",
    "मौसम": "weather",
    "samagri": "material",
    "padarth": "material",
    "ushma": "heat",
    "garam": "high temperature",
    "sva upchar": SELF_HEALING_LABEL,
    "mrittika": "soil",
    "sinchai": "irrigation",
    "fasal": "crop",
    "mausam": "weather",
}

_LANG_HINTS_TE = {
    "పదార్థం": "material",
    "ఉష్ణోగ్రత": "temperature",
    "స్వయం చికిత్స": SELF_HEALING_LABEL,
    "మట్టి": "soil",
    "వాతావరణం": "weather",
    "పంట": "crop",
    "నీరు": "irrigation",
    "padartham": "material",
    "vedi": "heat",
    "ushnograta": "temperature",
    "swayam chikitsa": SELF_HEALING_LABEL,
    "mannu": "soil",
    "pari" : "weather",
    "panta": "crop",
    "neellu": "irrigation",
}


def detect_language(text: str) -> str:
    if not text:
        return "en"

    # Devanagari block (Hindi and related languages)
    if re.search(r"[\u0900-\u097F]", text):
        return "hi"

    # Telugu Unicode block
    if re.search(r"[\u0C00-\u0C7F]", text):
        return "te"

    return "en"


def normalize_to_english(text: str) -> tuple[str, str]:
    language = detect_language(text)
    normalized = text.strip()

    if language == "hi":
        normalized = _translate_by_hints(normalized, _LANG_HINTS_HI)
    elif language == "te":
        normalized = _translate_by_hints(normalized, _LANG_HINTS_TE)

    return language, normalized


def _translate_by_hints(text: str, hints: dict[str, str]) -> str:
    lowered = text.lower()
    translated = lowered
    for source_token, target_token in hints.items():
        translated = translated.replace(source_token, target_token)
    return translated
