# Padifood Backend

running on port 8001 via systemctl

`systemctl --user status padifood-backend`
`journalctl --user -u padifood-backend -f`

to inspect

## Wagegroup API

All wagegroup endpoints require `X-API-Key`.

Set variables:

`export BACKEND_URL="http://localhost:8001"`
`export API_KEY="your-api-key"`

Get current people:

`curl -sS -X GET "$BACKEND_URL/wagegroups" -H "X-API-Key: $API_KEY"`

Clear wagegroups (all providers):

`curl -sS -X DELETE "$BACKEND_URL/wagegroups" -H "X-API-Key: $API_KEY"`

Fallback clear endpoint:

`curl -sS -X POST "$BACKEND_URL/wagegroups/clear" -H "X-API-Key: $API_KEY"`

Upsert a single person (canonical ID first; name fallback auto-generated when omitted):

`curl -sS -X POST "$BACKEND_URL/update_wage_person" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"provider":"otto","personNumber":"123456","name":"John Doe","wagegroup":"WG3","verified":true}'`

Bulk upload from Excel (header-detected):

- supported header pairs include:
  - `Achternaam voornaam` + `Loongroep`
  - `Name`/`Naam` + `Wagegroup`
- for Otto, names like `Ailoaei 16444322 Emanuel-Florin` are parsed and matched against verified identifier mappings; canonical person numbers are stored when found

`curl -sS -X POST "$BACKEND_URL/update_wages" -H "X-API-Key: $API_KEY" -F "file=@/absolute/path/to/wagegroups.xlsx"`

Backfill DB wagegroups from legacy CSV + Otto identifier mapping candidates:

`curl -sS -X POST "$BACKEND_URL/wagegroups/backfill_from_csv" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"week":"202550","provider":"otto"}'`

Analyze Otto wagegroups for a week (compares invoice `fase_tarief` against DB wagegroups):

`curl -sS -X POST "$BACKEND_URL/otto_wagegroups/analyze" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"week":"202550","includeMismatches":true,"maxItems":500}'`

Verify Otto runtime coverage/uniqueness gate:

`curl -sS -X POST "$BACKEND_URL/otto_wagegroups/verify_coverage" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"week":"202550"}'`

## Upload Validation + Name Verification API

`POST /upload` now returns provider-specific results in `providers`.

Upload files (2 Excel files: one kloklijst and one factuur/specificatie):

`curl -sS -X POST "$BACKEND_URL/upload" -H "X-API-Key: $API_KEY" -F "files=@/absolute/path/to/202551 Kloklijst Padifood Otto Workforce.xlsx" -F "files=@/absolute/path/to/202551 Padifood specificatie.xlsx"`

Response shape:

- `providers.otto` / `providers.flex` (depending on uploaded files)
- per provider:
  - `emailBody`
  - `outputFileWeek`
  - `outputFileDay`
  - `similarPeople`
  - `exactPersonMatchCount`
  - Otto only:
    - `wagegroupOutputFile`
    - `wagegroupSummary`

Confirm suggested pairs:

`curl -sS -X POST "$BACKEND_URL/verify_name_pairs" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"week":"202551","decisions":[{"kloklijstName":"Jan Jansen","factuurName":"Jansen Jan","samePerson":true},{"kloklijstName":"Piet de Vries","factuurName":"P de Vries","samePerson":false}]}'`

`POST /verify_name_pairs` response:

- `providers.otto` / `providers.flex` with:
  - `emailBody`
  - `outputFileWeek`
  - `exactPersonMatchCount`
  - Otto only:
    - `wagegroupOutputFile`
    - `wagegroupSummary`

Notes:

- decisions are persisted globally in `verified_name_pairs.csv` and reused in future validations
- `samePerson=false` also prevents repeated fuzzy suggestions for that pair
- for Otto, upload validation now also compares `InvoiceLine.fase_tarief` against DB `person_wagegroups` and includes loongroep mismatches in `emailBody`

## Otto Identifier Mapping API

`POST /otto_identifier_mapping/build` builds Otto candidate mappings from:

- `kloklijst.agency='otto'` (`Loonnummers`, `Naam`)
- `invoice_lines` (`SAP ID`, `Naam`)

Request body:

`{"week":"202550","persist":false,"requireFullCoverage":false,"writeCsv":true,"includeCandidates":false}`

Response includes:

- `stats` (coverage and match-type counts)
- `uniquenessConflicts`
- `csvBackupPath` (backup CSV at `output/otto_identifier_mapping_backup.csv` when enabled)
- `persistResult.insertedMappings` (when `persist=true`)

Notes:

- Otto mapping is identifier-first (`Loonnummers -> SAP ID`) with name fallback in validation
- Flex remains on current name-based behavior until a valid identifier/source bridge exists for Flex-specific invoice matching
