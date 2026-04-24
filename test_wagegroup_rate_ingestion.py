import io

from openpyxl import Workbook

from wagegroup_rates import commit_wagegroup_rate_preview, create_wagegroup_rate_preview


def _workbook_bytes(sheet_name: str, rows: list[list[object]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    for row in rows:
        ws.append(row)
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def test_header_normalization_matches_person_number(tmp_path):
    content = _workbook_bytes(
        "Blad1",
        [
            [" Personeelsnummer ", "Voornaam", "Achternaam", "100 %"],
            ["123", "Jan", "Jansen", "15.2"],
        ],
    )
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    mapping = {item["key"]: item for item in preview["mapping"]}
    assert mapping["person_number"]["sourceType"] == "header"
    assert mapping["person_number"]["confidence"] == "exact"
    assert mapping["rate_100"]["confidence"] == "normalized"


def test_fallback_mapping_is_used(tmp_path):
    rows = [["Personeelsnummer", "Voornaam", "Achternaam"] + [""] * 20]
    row = ["123", "Jan", "Jansen"] + [""] * 13 + [12.5, 13.3, 13.5, 18, 20, 30, ""]
    rows.append(row)
    content = _workbook_bytes("Blad1", rows)
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    mapping = {item["key"]: item for item in preview["mapping"]}
    assert mapping["rate_100"]["sourceType"] == "fallback"
    assert mapping["rate_100"]["column"] == "Q"


def test_sheet_selection_rules():
    otto_bad = _workbook_bytes("Wrong", [["Personeelsnummer", "Voornaam", "Achternaam"]])
    try:
        create_wagegroup_rate_preview(
            content=otto_bad,
            filename="otto.xlsx",
            agency="otto",
            preview_dir=".",
        )
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Blad1" in str(exc)


def test_flex_180_uses_v_fallback(tmp_path):
    rows = [["Loonnummer", "Voornaam", "Achternaam"] + [""] * 22]
    # V (1-indexed 22) => zero-based 21
    row = ["9001", "Lisa", "de Vries"] + [""] * 18 + [18.8, ""]
    rows.append(row)
    content = _workbook_bytes("Blad1", rows)
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="flex.xlsx",
        agency="flexspecialisten",
        preview_dir=str(tmp_path),
    )
    mapping = {item["key"]: item for item in preview["mapping"]}
    assert mapping["rate_180"]["sourceType"] == "fallback"
    assert mapping["rate_180"]["column"] == "V"


def test_unresolved_required_blocks_commit(tmp_path):
    content = _workbook_bytes(
        "Blad1",
        [
            ["Voornaam", "Achternaam", "100%"],
            ["Jan", "Jansen", 12.3],
        ],
    )
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    try:
        commit_wagegroup_rate_preview(
            upload_id=preview["uploadId"],
            agency="otto",
            preview_dir=str(tmp_path),
            output_dir=str(tmp_path),
        )
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Ontbrekende verplichte kolommen" in str(exc)


def test_partial_ingest_counts_and_errors(tmp_path):
    content = _workbook_bytes(
        "Blad1",
        [
            ["Personeelsnummer", "Voornaam", "Achternaam", "100%", "133%"],
            ["101", "Jan", "Jansen", "10.0", "13.3"],
            ["102", "Piet", "", "11.0", "abc"],
            ["103", "Klaas", "Visser", "", ""],
        ],
    )
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    parsed, stats = commit_wagegroup_rate_preview(
        upload_id=preview["uploadId"],
        agency="otto",
        preview_dir=str(tmp_path),
        output_dir=str(tmp_path),
    )
    assert stats["processedRows"] == 3
    assert stats["ingestedRows"] == 1
    assert stats["skippedRows"] == 2
    assert len(parsed.person_rates) == 2
    assert any(err["code"] == "missing_name" for err in stats["errorReportInline"])
    assert any(err["code"] == "no_rates_found" for err in stats["errorReportInline"])


def test_empty_override_entries_do_not_clear_preview_mapping(tmp_path):
    content = _workbook_bytes(
        "Blad1",
        [
            ["Personeelsnummer", "Voornaam", "Achternaam", "100%", "133%"],
            ["101", "Jan", "Jansen", "10.0", "13.3"],
        ],
    )
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    parsed, stats = commit_wagegroup_rate_preview(
        upload_id=preview["uploadId"],
        agency="otto",
        preview_dir=str(tmp_path),
        output_dir=str(tmp_path),
        mapping_override={
            "rate_100": {},
            "rate_133": {"header": "", "column": ""},
        },
    )
    assert stats["ingestedRows"] == 1
    assert stats["skippedRows"] == 0
    assert len(parsed.person_rates) == 2


def test_invalid_override_header_does_not_clear_preview_mapping(tmp_path):
    content = _workbook_bytes(
        "Blad1",
        [
            ["Personeelsnummer", "Voornaam", "Achternaam", "100%", "133%"],
            ["101", "Jan", "Jansen", "10.0", "13.3"],
        ],
    )
    preview = create_wagegroup_rate_preview(
        content=content,
        filename="otto.xlsx",
        agency="otto",
        preview_dir=str(tmp_path),
    )
    parsed, stats = commit_wagegroup_rate_preview(
        upload_id=preview["uploadId"],
        agency="otto",
        preview_dir=str(tmp_path),
        output_dir=str(tmp_path),
        mapping_override={
            "rate_100": {"header": "rate_100"},
            "rate_133": {"header": "rate_133"},
        },
    )
    assert stats["ingestedRows"] == 1
    assert stats["skippedRows"] == 0
    assert len(parsed.person_rates) == 2
    assert any("rate_100" in warning for warning in (stats.get("warnings") or []))
