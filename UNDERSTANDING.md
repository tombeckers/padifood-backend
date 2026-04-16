# Padifood Invoice Verification — Full Understanding

## Context

Padifood is a food production company that hires temporary workers through staffing agencies, primarily **OTTO Workforce** (and also **Flexspecialisten**). OTTO sends invoices to Padifood for the hours worked by these employees. Padifood also keeps its own clock-in/clock-out records (kloklijsten). This project verifies that OTTO's invoices are correct by cross-checking hours and rates.

---

## Input Files

### 1. Kloklijst (Timesheets) — Padifood's own records

**Files:** `202549, Kloklijst Padifood, Otto Workforce.xlsx` (weeks 49, 50, 51)

These are Padifood's internal time registrations — what they recorded employees actually worked.

| Column | Meaning |
|---|---|
| `Loonnummers` | Internal payroll number |
| `Personeelsnummer` | Internal personnel ID (different from OTTO's SAP ID) |
| `Datum` | Date |
| `Naam` | Employee name (format: `Lastname Firstname`) |
| `Afd.` | Department (e.g. `PADIFOOD/Wokkeuken`, `DRONTERMEER/Keuken`) |
| `start` / `Eind` | Shift start and end timestamps |
| `Pauze genomen Dag` | Break taken (hours) |
| `Pauze afgetrokken Dag` | Break deducted (hours) |
| `Norm uren Dag` | Normal hours at 1x rate |
| `T133 Dag` | Hours at 1.33x rate (toeslag/surcharge) |
| `T135 Dag` | Hours at 1.35x rate |
| `T200 Dag` | Hours at 2x rate |
| `OW140 Week` | Overtime at 1.4x (weekly overtime) |
| `OW180 Dag` | Overtime at 1.8x (day overtime, typically Saturday) |
| `OW200 Dag` | Overtime at 2x (typically Sunday/holiday) |
| `Effectieve uren Dag` | Total effective hours that day |

**Structure quirks:**
- Employee name only appears on the first row of each block; subsequent rows for the same employee have empty Name/ID fields.
- Each employee block spans 7 day-rows (Mon–Sun) followed by a summary row (empty date, with weekly totals).
- Negative values can appear (e.g. `-3.25` in Norm uren on a Saturday) — this represents reclassification of hours from normal to overtime categories.
- Weeks covered: 49 (Dec 1–7), 50 (Dec 8–14), 51 (Dec 15–21).

There are also **Flexspecialisten** versions of these files for the other staffing agency.

A **correction file** also exists: `202551, Kloklijst Padifood, Otto Workforce- correctie Jankowski.xlsx` — specific correction for employee Jankowski.

### 2. OTTO Tarievenoverzicht — OTTO's agreed rate card per employee

**File:** `OTTO -Padifood tarievenoverzicht per persoon - achternaam voornaam.xlsx`

This is the rate card that OTTO and Padifood agreed upon — the billing rate per employee, per hour type.

| Column | Meaning |
|---|---|
| `Personeelsnummer` | OTTO's SAP ID (matches the invoice) |
| `Achternaam` / `Voornaam` | Last name / First name |
| `Wekenteller` | Week counter (number of weeks worked — determines salary scale) |
| `Schaal` | Salary scale (e.g. `A0`, `B1`, `C2`, `D1`, `E4`) |
| `Uurloon incl. ATV` | Hourly wage including ATV (working time reduction) |
| `Uurloon excl. ATV` | Hourly wage excluding ATV |
| Columns 8–15 (positional) | Billing rates for different multipliers |

**Rate columns (positional, 0-indexed from column 7):**

| Index | Multiplier | Used for |
|---|---|---|
| 0 | 1x | `Norm uren Dag` — normal day hours |
| 1 | 1.33x | `T133 Dag` — 33% surcharge |
| 2 | 1.35x | `T135 Dag` — 35% surcharge |
| 3 | 1.8x (day) | Day-related 1.8x (not used in current invoice codes) |
| 4 | 2x (day) | `T200 Dag` — 100% surcharge |
| 5 | 1.4x (OW) | `OW140 Week` — weekly overtime |
| 6 | 1.8x (OW) | `OW180 Dag` — day overtime (Saturday) |
| 7 | 2x (OW) | `OW200 Dag` — day overtime (Sunday/holiday) |

**Contains 134 employees.** Not all employees in the invoice appear here — 57 are missing from this rate card, suggesting it may be incomplete or outdated.

### 3. Lonen en tarieven — Flexspecialisten rate table (NOT used for OTTO verification)

**File:** `Lonen en tarieven 2025 vanaf 1 April.xlsx` (sheet: `1 januari 2025`)

This is the rate table from **Flexspecialisten** (the other staffing agency), NOT from OTTO. It contains wage tables per salary scale with ORF, marge, and calculated billing rates. **We ignore this file** for OTTO invoice verification — it is only relevant for checking Flexspecialisten invoices.

For reference, Flexspecialisten uses different factors than OTTO:
- Marge: €4.54 (vs OTTO's €2.99)
- Fase A/B: ORF = 1.7058 (vs OTTO's 1.7025/1.7068)
- Fase C: ORF = 1.629 (vs OTTO's 1.6497)

### 4. Padifood specificatie — OTTO's invoice

**File:** `Padifood specificatie.xlsx`

Has two sheets:

#### Sheet: Export Factuur (Invoice Export)
This is the actual invoice from OTTO — line-by-line billing.

| Column | Meaning |
|---|---|
| `SAP ID` | OTTO's employee ID |
| `Naam` | Employee name (format: `Firstname Lastname`) |
| `Uurloon` | Hourly wage incl. ATV |
| `Uurloon zonder ATV` | Hourly wage excl. ATV |
| `Functie toeslag` | Function surcharge (always 0 in current data) |
| `Wekentelling` | Week counter |
| `Fase tarief` | Phase/Fase: `A`, `B`, or `C` |
| `Datum` | Date |
| `Code toeslag` | Hour type code (same as kloklijst column names) |
| `Totaal uren` | Hours for this line |
| `Subtotaal` | Amount billed (hours × rate) |

**Covers:** Week 51 (Dec 15–20, 2025), 111 employees, 910 line items.
**Total invoiced:** ~€139,257.

**Hour type codes on the invoice:**
- `Norm uren Dag` — normal hours (1x)
- `T133 Dag` — 33% surcharge hours
- `T135 Dag` — 35% surcharge hours
- `OW140 Week` — weekly overtime (1.4x)
- `OW180 Dag` — Saturday overtime (1.8x)
- `OW200 Dag` — Sunday/holiday overtime (2x)

#### Sheet: Tarievensheet per persoon — THE primary OTTO rate reference

This is OTTO's own rate calculation sheet — the most authoritative source for verifying invoice rates. It contains per-employee billing rates with all underlying factors.

**Covers:** Week 48 (Nov 24–28, 2025). Although a different period than the invoice (week 51), the rates are stable and **95 of 111 invoiced employees match within 2 cents** — making this the best rate source we have. The remaining 16 are employees not present in this sheet.

| Column (positional) | Meaning |
|---|---|
| 0: `SAP ID` | OTTO's employee ID (matches invoice) |
| 1: `Naam` | Employee name |
| 2: `Uurloon` | Hourly wage incl. ATV |
| 3: `Uurloon zonder ATV` | Hourly wage excl. ATV |
| 5: `Wekentelling` | Week counter |
| 6: `Fase tarief` | `A`, `B`, or `C` |
| 13: `orf` | Overhead Recovery Factor used |
| 14: `TF` | Tarief Factor |
| 15: `OF` | Overwerk Factor |
| 16–17: `ATV` | ATV factors |
| 18: `marge` | Margin per hour |
| 19–24 | Billing rates: `1x, 1.33x, 1.35x, 1.8x, 2x, 3x` |
| 25–28 | Overtime rates: `1.4x, 1.8x, 2x, 3x` |
| 30–34 | Reference: Fase tarief, ORF, TF, OF, ATV |

**Key factors per Fase (from actual data):**

| Fase | ORF | Marge |
|---|---|---|
| A | 1.7025 | €2.99 |
| B | 1.7068 | €2.99 |
| C | 1.6497 | €2.99 |

**Rate formula (verified against data):**
```
Billing rate at 1x = (Uurloon incl. ATV × ORF) + Marge
```

**Comparison with the OTTO rates file:**

| Aspect | Tarievensheet | OTTO -Padifood file |
|---|---|---|
| Employees covered | 111 (from invoice) | 134 |
| Match with invoice | 95/111 within 2ct | 1/111 within 2ct |
| Marge | €2.99 | ~€4.54 (implicit) |
| ORF Fase A | 1.7025 | ~1.7058 |
| ORF Fase C | 1.6497 | ~1.629 |
| Uurloon (e.g. Kutermak) | €17.63 | €17.03 |

The OTTO -Padifood file appears to use **older wage levels** and **different factors** (possibly from a different period or calculation method). The Tarievensheet is the correct reference for current invoices.

---

## ID and Name Matching

A critical challenge: the files use **different ID systems** and **different name formats**.

| File | ID field | Name format |
|---|---|---|
| Kloklijst | `Personeelsnummer` (internal, e.g. `30050`) | `Lastname Firstname` |
| OTTO Tarievenoverzicht | `Personeelsnummer` (SAP ID, e.g. `374801`) | `Achternaam` + `Voornaam` (separate columns) |
| Invoice (Export Factuur) | `SAP ID` (e.g. `374801`) | `Firstname Lastname` |

**Matching strategy:**
- Invoice ↔ OTTO Rates: matched by `SAP ID` = `Personeelsnummer` (54 direct matches; 57 not in rate card)
- Invoice ↔ Kloklijst: matched by **normalized name** (lowercase, sorted words) — 109 of 111 match
- 2 unmatched: `Sandra Owczarczak - Wawrzyniak` (hyphenated name) and `Krystyna Pasiali`

---

## Verification Checks

### Check 1: Hours Comparison (Invoice vs Kloklijst)

For each employee, date, and hour type, compare:
- What OTTO billed (invoice `Totaal uren`)
- What Padifood recorded (kloklijst hour columns)

**Common discrepancy patterns:**
- Hours reclassified between categories (e.g. Norm→OW on weekends, shown as negative Norm + positive OW in kloklijst)
- Sunday (Dec 21) hours appear in kloklijst but not in invoice (week boundary difference)
- Small rounding differences

### Check 2: Rate Comparison (Invoice vs OTTO Tarievensheet)

For each employee and hour type, compare:
- The effective rate on the invoice (`Subtotaal / Totaal uren`)
- The expected rate from the **Tarievensheet per persoon** (primary source, in the Padifood specificatie file)
- Fallback: the **OTTO -Padifood tarievenoverzicht** file (secondary, older rates)

**Two OTTO rate sources exist:**

1. **Tarievensheet per persoon** (in the invoice file) — matches 95/111 employees within 2 cents. This uses SAP ID for matching, covers employees from week 48, and has accurate ORF/marge values.

2. **OTTO -Padifood tarievenoverzicht** (standalone file) — older rates that are systematically ~€1 lower. Only 1/111 matches the invoice. Useful as a fallback for the 16 employees not in the Tarievensheet, and contains the `Schaal` (salary scale) per employee which the Tarievensheet does not.

**The script should prefer the Tarievensheet, falling back to the OTTO file.**

### Check 3: Factor Validation (not yet automated)

Verify that the rate factors used by OTTO are correct:
- Is the correct `Fase` (A/B/C) applied based on the `Wekenteller`?
- Is the ORF correct for that Fase? (A=1.7025, B=1.7068, C=1.6497)
- Is the billing rate correctly calculated: `(Uurloon incl. ATV × ORF) + Marge`?

**Fase rules:**
- Fase A: early phase — ORF = 1.7025, Marge = €2.99
- Fase B: mid phase — ORF = 1.7068, Marge = €2.99
- Fase C: long-tenured — ORF = 1.6497, Marge = €2.99

### Check 4: Salary Scale Validation (not yet automated)

The `Schaal` (A0, A1, A2, B0, etc.) from the OTTO -Padifood file is based on experience/tenure. The number after the letter indicates periodic wage increases. Verify:
- Does the `Wekenteller` justify the current scale?
- Has the employee been upgraded correctly over time?

---

## The Verification Script

**File:** `check_invoice.py`

### What it does
1. **Loads the OTTO rate card** — builds a lookup by SAP ID and normalized name
2. **Loads the kloklijst** — parses the block-structured timesheet into `{name → {date → {hour_type → hours}}}`
3. **Loads the invoice** — extracts line items and aggregates hours per employee/date/type
4. **Compares hours** — finds mismatches between invoice and kloklijst
5. **Compares rates** — checks invoice rates against OTTO rate card
6. **Builds employee summary** — totals per employee with flags
7. **Writes Excel report** to `output/verification_report.xlsx`

### Output: `output/verification_report.xlsx`

| Sheet | Content |
|---|---|
| **Employee Summary** | Per-employee overview: SAP ID, name, scale, invoice hours, kloklijst hours, difference (red if ≠0), total amount, flags for missing data |
| **Hour Discrepancies** | Each row = one specific employee + date + hour type where hours don't match |
| **Rate Discrepancies** | Each row = one employee + hour type where the billed rate differs from the agreed rate, or employee not found in rate card |

### How to run
```bash
uv run python check_invoice.py
```

### Configuration
All file paths are defined as constants at the top of the script. To check a different week, update `KLOKLIJST_FILE` and `INVOICE_FILE`.

---

## Known Limitations and TODOs

1. **Script uses the OTTO -Padifood file for rate comparison** — the script currently compares against the older/less accurate OTTO rates file. It should be updated to use the **Tarievensheet per persoon** as the primary rate source (matches 95/111 within 2ct) and fall back to the OTTO file only for employees not in the Tarievensheet.

2. **16 employees missing from Tarievensheet** — these appear on the invoice but not in the Tarievensheet (which covers week 48). The OTTO -Padifood file covers more employees (134) but with outdated rates. For these 16, the OTTO file can still flag large deviations.

3. **Factor validation not automated** — the script does not yet verify that OTTO's ORF, Marge, and Fase are correct. This could be added by recalculating: `(Uurloon × ORF) + Marge` and comparing to the rate.

4. **Name matching is imperfect** — 2 of 111 employees don't match between invoice and kloklijst due to hyphens/special characters. These need manual review.

5. **Sunday hours** — the kloklijst includes Sunday (Dec 21) which falls in the next billing week. These generate false discrepancies. Filtering to the invoice date range (Mon–Sat) would clean this up.

6. **Correction file** — `202551, Kloklijst Padifood, Otto Workforce- correctie Jankowski.xlsx` is not yet incorporated. Jankowski's hours should be adjusted based on this file.

7. **Only week 51 is checked** — the script currently only compares the invoice (week 51) against the week 51 kloklijst. Weeks 49 and 50 could be checked once their invoices are available.

8. **Lonen en tarieven file is from Flexspecialisten** — this was initially thought to be the CAO table but is actually the Flexspecialisten rate card. It is not relevant for OTTO verification.

---

## Glossary

| Term | Meaning |
|---|---|
| **ATV / ADV** | Arbeidstijdverkorting / Arbeidsduurvermindering — working time reduction (e.g. 38hr→36hr week), paid as part of hourly wage |
| **CAO** | Collectieve Arbeidsovereenkomst — collective labor agreement |
| **Fase A/B/C** | Employment phase under Dutch flexible labor law (ABU/NBBU). Fase A = first 78 weeks, B = 78–130, C = 130+ |
| **Kloklijst** | Clock list — time registration sheet |
| **Marge** | Margin — staffing agency's flat fee per hour. OTTO uses €2.99, Flexspecialisten uses €4.54 |
| **Norm uren** | Normal hours at base rate |
| **ORF** | Overhead Recovery Factor — covers social charges, pension, holiday pay, sickness reserves |
| **OW** | Overwerk — overtime |
| **Schaal** | Salary scale (A0 through E4+) |
| **T133 / T135 / T200** | Toeslag — surcharge at 133%, 135%, 200% of base rate |
| **Wekenteller** | Week counter — total weeks of employment, determines scale progression |
