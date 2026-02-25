"""Per-form-type configuration defining field roles.

Used by the indexer (stage 5) to decide what gets embedded vs stored as metadata.
"""

FORM_CONFIGS = {
    "ת053": {
        "category": "container",
        "narrative_fields": ["TextHofshi", "ReportSubject", "LeloShem"],
        "metadata_fields": ["TaarichDivuach", "SugDivuach"],
        "skip_fields": ["Shem", "Mispar", "TaarichIdkun"],
    },
    "ת121": {
        "category": "container",
        "narrative_fields": ["TextHofshi", "ReportSubject", "LeloShem"],
        "metadata_fields": ["TaarichDivuach"],
        "skip_fields": ["Shem", "Mispar", "TaarichIdkun"],
    },
    "ת087": {
        "category": "structured",
        "narrative_fields": ["ReportSubject"],
        "metadata_fields": ["SugShinuy"],
        "skip_fields": ["Shem", "Mispar"],
    },
    "ת076": {
        "category": "structured",
        "narrative_fields": ["ReportSubject"],
        "metadata_fields": [],
        "skip_fields": ["Shem", "Mispar"],
    },
    "ת081": {
        "category": "structured",
        "narrative_fields": ["ReportSubject"],
        "metadata_fields": ["TaarichKeta", "TaarichTashlum", "SachDividend", "DividendPerShare"],
        "skip_fields": ["Shem", "Mispar"],
    },
    "ת077": {
        "category": "registry",
        "narrative_fields": [],
        "metadata_fields": ["TaarichDivuach"],
        "skip_fields": [],
    },
    "ת460": {
        "category": "mixed",
        "narrative_fields": ["ReportSubject"],
        "metadata_fields": [],
        "skip_fields": ["Shem", "Mispar"],
    },
    "_default": {
        "category": "container",
        "narrative_fields": ["TextHofshi", "ReportSubject"],
        "metadata_fields": [],
        "skip_fields": [],
    },
}


def get_config(form_type: str) -> dict:
    """Get config for a form type, falling back to _default."""
    return FORM_CONFIGS.get(form_type, FORM_CONFIGS["_default"])


def get_category(form_type: str) -> str:
    return get_config(form_type)["category"]
