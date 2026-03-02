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