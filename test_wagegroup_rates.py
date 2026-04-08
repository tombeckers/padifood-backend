import io
import unittest

try:
    from openpyxl import Workbook
except ImportError:  # pragma: no cover - environment dependency
    Workbook = None

from wagegroup_rates import (
    _build_histogram_rows,
    _extract_schaal_tarief,
    _rate_key_from_code_toeslag,
    _rate_key_from_header,
    parse_flex_wagegroup_rate_workbook,
    parse_otto_wagegroup_rate_workbook,
)


class WagegroupRatesTests(unittest.TestCase):
    def test_rate_key_from_header(self):
        self.assertEqual(_rate_key_from_header("100%"), "100")
        self.assertEqual(_rate_key_from_header("1.33"), "133")
        self.assertEqual(_rate_key_from_header("2"), "200")

    def test_extract_schaal_tarief(self):
        schaal, tarief = _extract_schaal_tarief("Productiemdw D4 / Fase C")
        self.assertEqual(schaal, "D4")
        self.assertEqual(tarief, "C")

    def test_rate_key_from_code_toeslag(self):
        self.assertEqual(_rate_key_from_code_toeslag("T133 Dag"), "133")
        self.assertEqual(_rate_key_from_code_toeslag("OW200 Dag"), "200")
        self.assertEqual(_rate_key_from_code_toeslag("Norm uren Dag"), "100")

    def test_histogram_rows(self):
        rows = _build_histogram_rows([0.02, 0.4, 0.7, 1.2, 6.0])
        counts = {row["bucket"]: row["count"] for row in rows}
        self.assertEqual(counts["0.00-0.10"], 1)
        self.assertEqual(counts["0.25-0.50"], 1)
        self.assertEqual(counts["0.50-1.00"], 1)
        self.assertEqual(counts["1.00-2.00"], 1)
        self.assertEqual(counts["5.00+"], 1)

    @unittest.skipIf(Workbook is None, "openpyxl not installed")
    def test_parse_otto_workbook(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "Blad1"
        ws["A1"] = "Personeelsnummer"
        ws["AB1"] = "Fase tarief"
        ws["AC1"] = "ORF"
        ws["AD1"] = "TF"
        ws["AE1"] = "OF"
        ws["AF1"] = "ATV"
        ws["A2"] = "441359"
        ws["AB2"] = "A"
        ws["AC2"] = 10
        ws["AD2"] = 13
        ws["AE2"] = 14
        ws["AF2"] = 20
        buf = io.BytesIO()
        wb.save(buf)

        parsed = parse_otto_wagegroup_rate_workbook(buf.getvalue(), "Padifood tarieven p.p..xlsx")
        self.assertEqual(len(parsed.person_rates), 4)
        self.assertTrue(any(r["rate_key"] == "133" for r in parsed.person_rates))

    @unittest.skipIf(Workbook is None, "openpyxl not installed")
    def test_parse_flex_workbook(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "1 januari 2025"
        ws["A1"] = "Leeftijd"
        ws["B1"] = "Lota naam"
        ws["G1"] = "1"
        ws["I1"] = "1.33"
        ws["A2"] = "alle leeftijden"
        ws["B2"] = "Productiemdw D4 / Fase C"
        ws["G2"] = 20.0
        ws["I2"] = 26.6
        buf = io.BytesIO()
        wb.save(buf)

        parsed = parse_flex_wagegroup_rate_workbook(buf.getvalue(), "Lonen en tarieven 2025.xlsx")
        self.assertEqual(len(parsed.person_rates), 2)
        self.assertTrue(any(r["schaal"] == "D4" for r in parsed.person_rates))


if __name__ == "__main__":
    unittest.main()
