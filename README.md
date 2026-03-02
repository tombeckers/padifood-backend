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

Upsert a single person:

`curl -sS -X POST "$BACKEND_URL/update_wage_person" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"name":"John Doe","wagegroup":"WG3"}'`

Bulk upload from Excel (`col1=name`, `col2=wagegroup`):

`curl -sS -X POST "$BACKEND_URL/update_wages" -H "X-API-Key: $API_KEY" -F "file=@/absolute/path/to/wagegroups.xlsx"`

## Upload Validation + Name Verification API

`POST /upload` now also returns `similarPeople`: near-equal name pairs (`>90` fuzzy match) between kloklijst and factuur names.

Upload files (2 Excel files: one kloklijst and one factuur/specificatie):

`curl -sS -X POST "$BACKEND_URL/upload" -H "X-API-Key: $API_KEY" -F "files=@/absolute/path/to/202551 Kloklijst Padifood Otto Workforce.xlsx" -F "files=@/absolute/path/to/202551 Padifood specificatie.xlsx"`

Response shape:

- `emailBody`: email text after validation
- `outputFileWeek`: generated weekly CSV output path
- `outputFileDay`: generated daily CSV output path
- `similarPeople`: list of `[kloklijstName, factuurName]` pairs for user verification (empty list when none)

Confirm suggested pairs:

`curl -sS -X POST "$BACKEND_URL/verify_name_pairs" -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" -d '{"week":"202551","decisions":[{"kloklijstName":"Jan Jansen","factuurName":"Jansen Jan","samePerson":true},{"kloklijstName":"Piet de Vries","factuurName":"P de Vries","samePerson":false}]}'`

`POST /verify_name_pairs` response:

- `emailBody`
- `outputFileWeek`

Notes:

- decisions are persisted globally in `verified_name_pairs.csv` and reused in future validations
- `samePerson=false` also prevents repeated fuzzy suggestions for that pair