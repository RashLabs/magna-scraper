from tools.magna_entity_resolver import (
    MagnaEntity,
    normalize_text,
    resolve_company,
    score_entity,
)


def test_normalize_text_strips_punctuation_and_whitespace() -> None:
    assert normalize_text("  בנק  לאומי בע\"מ  ") == "בנק לאומי בע מ"
    assert normalize_text("KENON Holdings, LTD.") == "kenon holdings ltd"


def test_score_prefers_primary_company_over_subsidiary() -> None:
    company = {"name": "לאומי"}
    primary = MagnaEntity(
        entity_id="167",
        name_he='בנק לאומי לישראל בע"מ',
        name_en="BANK LEUMI LE-ISRAEL LTD",
        short_name="לאומי",
        type_codes=("1",),
    )
    subsidiary = MagnaEntity(
        entity_id="2314",
        name_he='ברק לאומי חתמים בע"מ',
        name_en="BARAK LEUMI UNDERWRITING LTD",
        short_name="",
        type_codes=("8",),
    )

    primary_score = score_entity(company, primary, aliases=[])
    subsidiary_score = score_entity(company, subsidiary, aliases=[])
    assert primary_score > subsidiary_score


def test_resolve_company_updates_when_confident() -> None:
    company = {
        "name": "לאומי",
        "magna_id": 12,
        "magna_name": 'אגיש הובלה בין לאומית בע"מ',
    }
    entities = [
        MagnaEntity(
            entity_id="167",
            name_he='בנק לאומי לישראל בע"מ',
            name_en="BANK LEUMI LE-ISRAEL LTD",
            short_name="לאומי",
            type_codes=("1",),
        ),
        MagnaEntity(
            entity_id="2314",
            name_he='ברק לאומי חתמים בע"מ',
            name_en="BARAK LEUMI UNDERWRITING LTD",
            short_name="",
            type_codes=("8",),
        ),
    ]

    decision = resolve_company(
        company=company,
        entities=entities,
        aliases=[],
        min_score=0.65,
        min_margin=0.08,
        min_improvement=0.08,
        top_n=3,
    )

    assert decision.status == "update_id"
    assert decision.update_id is True
    assert decision.best.entity_id == "167"


def test_resolve_company_keeps_review_on_low_margin() -> None:
    company = {"name": "קנון", "magna_id": 2951}
    entities = [
        MagnaEntity(
            entity_id="2951",
            name_he="קנון הולדינגס לימיטד",
            name_en="KENON HOLDINGS LTD",
            short_name="",
            type_codes=("1",),
        ),
        MagnaEntity(
            entity_id="2072",
            name_he="קנזון",
            name_en="KENZON",
            short_name="קנזון",
            type_codes=("1",),
        ),
    ]

    decision = resolve_company(
        company=company,
        entities=entities,
        aliases=[],
        min_score=0.65,
        min_margin=0.08,
        min_improvement=0.08,
        top_n=3,
    )

    assert decision.status in {"keep_current", "review"}
    assert decision.update_id is False


def test_aliases_help_resolve_trade_name() -> None:
    company = {"name": "בזן", "magna_id": 593}
    bazan = MagnaEntity(
        entity_id="2208",
        name_he='בתי זקוק לנפט בע"מ',
        name_en="OIL REFINERIES LTD",
        short_name="",
        type_codes=("1",),
    )
    baran = MagnaEntity(
        entity_id="593",
        name_he='קבוצת ברן בע"מ',
        name_en="BARAN GROUP LTD",
        short_name="ברן",
        type_codes=("1",),
    )

    without_alias = resolve_company(
        company=company,
        entities=[bazan, baran],
        aliases=[],
        min_score=0.65,
        min_margin=0.08,
        min_improvement=0.08,
        top_n=3,
    )
    with_alias = resolve_company(
        company=company,
        entities=[bazan, baran],
        aliases=["בתי זקוק לנפט", "oil refineries", "bazan"],
        min_score=0.65,
        min_margin=0.08,
        min_improvement=0.08,
        top_n=3,
    )

    assert without_alias.best.entity_id != with_alias.best.entity_id
    assert with_alias.best.entity_id == "2208"

