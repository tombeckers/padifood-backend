"""Padifood Invoice Verification Agent

Conversational CLI that wraps the local FastAPI backend.
Verifies OTTO Workforce and Flexspecialisten staffing invoices against
Padifood's own kloklijst (timesheet) data.

Usage:
    uv run python agent.py
    uv run python agent.py --url http://localhost:8001
"""

import csv
import json
import os
import sys
from pathlib import Path

import anthropic
import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _load_env() -> dict[str, str]:
    env_path = Path(__file__).parent / ".env"
    config: dict[str, str] = {}
    if env_path.exists():
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    config[key.strip()] = val.strip().strip('"').strip("'")
    return config

_env = _load_env()
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8001")
BACKEND_API_KEY = _env.get("BACKEND_API_KEY") or os.environ.get("BACKEND_API_KEY", "")

# ---------------------------------------------------------------------------
# Tool implementations  (sync httpx calls to the local FastAPI backend)
# ---------------------------------------------------------------------------

def _headers() -> dict[str, str]:
    return {"X-API-Key": BACKEND_API_KEY}


def _upload_invoice_files(
    otto_kloklijst_path: str | None = None,
    otto_factuur_path: str | None = None,
    flex_kloklijst_path: str | None = None,
    flex_pdf_paths: list[str] | None = None,
) -> dict:
    """Upload any combination of OTTO and/or Flexspecialisten files."""
    paths: list[tuple[str, str]] = []  # (path, mime_type)

    for p in [otto_kloklijst_path, otto_factuur_path, flex_kloklijst_path]:
        if p:
            paths.append((p, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
    for p in (flex_pdf_paths or []):
        if p:
            paths.append((p, "application/pdf"))

    if not paths:
        return {"error": "No files provided."}

    # Check all files exist before opening
    for p, _ in paths:
        if not Path(p).exists():
            return {"error": f"File not found: {p}"}

    file_handles = []
    try:
        multipart_files = []
        for p, mime in paths:
            fh = open(p, "rb")
            file_handles.append(fh)
            multipart_files.append(("files", (Path(p).name, fh, mime)))

        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                f"{BACKEND_URL}/upload",
                headers=_headers(),
                files=multipart_files,
            )
    finally:
        for fh in file_handles:
            fh.close()

    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    return resp.json()


def _verify_name_pairs(week: str, decisions: list[dict], agency: str = "otto") -> dict:
    payload = {"week": week, "agency": agency, "decisions": decisions}
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            f"{BACKEND_URL}/verify_name_pairs",
            headers={**_headers(), "Content-Type": "application/json"},
            content=json.dumps(payload),
        )
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    return resp.json()


def _get_wagegroups() -> list[dict] | dict:
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(f"{BACKEND_URL}/wagegroups", headers=_headers())
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    return resp.json()


def _update_wage_person(name: str, wagegroup: str) -> dict:
    payload = {"name": name, "wagegroup": wagegroup}
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{BACKEND_URL}/update_wage_person",
            headers={**_headers(), "Content-Type": "application/json"},
            content=json.dumps(payload),
        )
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    return resp.json()


def _read_output_file(file_path: str, max_rows: int = 50) -> dict:
    p = Path(file_path)
    if not p.exists():
        return {"error": f"File not found: {file_path}"}
    try:
        rows = []
        with open(p, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                rows.append(dict(row))
        return {"headers": headers, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"error": str(e)}


def _execute_tool(name: str, tool_input: dict) -> str:
    if name == "upload_invoice_files":
        result = _upload_invoice_files(
            otto_kloklijst_path=tool_input.get("otto_kloklijst_path"),
            otto_factuur_path=tool_input.get("otto_factuur_path"),
            flex_kloklijst_path=tool_input.get("flex_kloklijst_path"),
            flex_pdf_paths=tool_input.get("flex_pdf_paths"),
        )
    elif name == "verify_name_pairs":
        result = _verify_name_pairs(
            tool_input["week"],
            tool_input["decisions"],
            tool_input.get("agency", "otto"),
        )
    elif name == "get_wagegroups":
        result = _get_wagegroups()
    elif name == "update_wage_person":
        result = _update_wage_person(tool_input["name"], tool_input["wagegroup"])
    elif name == "read_output_file":
        result = _read_output_file(
            tool_input["file_path"],
            tool_input.get("max_rows", 50),
        )
    else:
        result = {"error": f"Unknown tool: {name}"}

    return json.dumps(result, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "upload_invoice_files",
        "description": (
            "Upload invoice files for validation. Supports OTTO (xlsx), Flexspecialisten (PDF), "
            "or both agencies in one call. At least one complete pair must be provided: "
            "OTTO needs otto_kloklijst_path + otto_factuur_path; "
            "Flex needs flex_kloklijst_path + one or more flex_pdf_paths. "
            "Returns a 'results' list — one entry per agency — each containing: "
            "emailBody (Dutch discrepancy summary), outputFileWeek, outputFileDay, "
            "and similarPeople (name pairs needing confirmation)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "otto_kloklijst_path": {
                    "type": "string",
                    "description": "Path to the OTTO kloklijst .xlsx file (filename must contain 'Otto' and 'Kloklijst')",
                },
                "otto_factuur_path": {
                    "type": "string",
                    "description": "Path to the OTTO factuur/specificatie .xlsx file",
                },
                "flex_kloklijst_path": {
                    "type": "string",
                    "description": "Path to the Flexspecialisten kloklijst .xlsx file (filename must contain 'Flexspecialisten' and 'Kloklijst')",
                },
                "flex_pdf_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "One or more paths to Flexspecialisten invoice PDF files. Multiple PDFs for the same week are automatically merged — correction invoices (later date) override earlier ones for overlapping employees.",
                },
            },
        },
    },
    {
        "name": "verify_name_pairs",
        "description": (
            "Confirm or deny fuzzy name match suggestions returned by upload_invoice_files. "
            "Call this when similarPeople contains pairs that need confirmation. "
            "samePerson=true means the two names refer to the same employee (merge hours). "
            "samePerson=false means they are different people (keep separate)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "week": {
                    "type": "string",
                    "description": "Week number in YYYYww format, e.g. '202551'",
                },
                "agency": {
                    "type": "string",
                    "enum": ["otto", "flexspecialisten"],
                    "description": "Which agency's validation to re-run after confirming pairs",
                },
                "decisions": {
                    "type": "array",
                    "description": "List of name pair decisions",
                    "items": {
                        "type": "object",
                        "properties": {
                            "kloklijstName": {"type": "string", "description": "Name as it appears in the kloklijst"},
                            "factuurName": {"type": "string", "description": "Name as it appears in the factuur/invoice"},
                            "samePerson": {"type": "boolean", "description": "True if these names refer to the same employee"},
                        },
                        "required": ["kloklijstName", "factuurName", "samePerson"],
                    },
                },
            },
            "required": ["week", "agency", "decisions"],
        },
    },
    {
        "name": "get_wagegroups",
        "description": "Retrieve the list of all employees and their wage groups from the backend.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "update_wage_person",
        "description": "Add or update a single employee's wage group.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Employee full name"},
                "wagegroup": {"type": "string", "description": "Wage group identifier"},
            },
            "required": ["name", "wagegroup"],
        },
    },
    {
        "name": "read_output_file",
        "description": (
            "Read the contents of a validation output CSV file. "
            "Use the outputFileWeek or outputFileDay path from upload_invoice_files results."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the output CSV file",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Maximum number of rows to return (default 50)",
                },
            },
            "required": ["file_path"],
        },
    },
]

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a helpful assistant for Padifood's invoice verification workflow.

Your job is to help verify staffing agency invoices against Padifood's own kloklijst (timesheet) data.
Two agencies are supported: OTTO Workforce (Excel invoices) and Flexspecialisten (PDF invoices).

## What you can do
- Upload files for validation via `upload_invoice_files` — OTTO, Flex, or both in one call
- Interpret validation results per agency: explain VERSCHIL, ALLEEN IN FACTUUR, ALLEEN IN KLOKLIJST
- Help confirm ambiguous name matches via `verify_name_pairs` — always ask user to confirm before calling
- Show and update employee wage groups
- Read output CSV files to dig into specific discrepancies

## File types
- **OTTO**: kloklijst (.xlsx, filename contains 'Otto' + 'Kloklijst') + factuur (.xlsx, contains 'factuur' or 'specificatie')
- **Flex**: kloklijst (.xlsx, filename contains 'Flexspecialisten' + 'Kloklijst') + one or more invoice PDFs
- Multiple Flex PDFs for the same week = the backend automatically uses the later-dated one for any overlapping employees (correction invoices)

## Response format
`upload_invoice_files` returns `{"results": [...]}` — one entry per agency validated:
```
{
  "agency": "otto" | "flexspecialisten",
  "emailBody": "...",          # Dutch discrepancy summary
  "outputFileWeek": "...",     # path to weekly aggregated CSV
  "outputFileDay": "...",      # path to daily breakdown CSV (empty for Flex — PDFs have no per-day data)
  "similarPeople": [...]       # name pairs needing confirmation
}
```

## Key domain knowledge
- Names are matched by normalizing: lowercase + sort words alphabetically, so "Dawid Kutermak" == "Kutermak Dawid"
- OTTO invoice uses SAP IDs; kloklijst uses Personeelsnummer — matched by name normalization
- Flex PDF invoice strips "De heer"/"Mevrouw" title prefixes before matching
- Hour types (both agencies): Norm uren Dag (1×), T133 Dag (1.33×), T135 Dag (1.35×), T200 Dag (2×), OW140 Week (1.4×), OW180 Dag (1.8×), OW200 Dag (2×)
- Flex PDF uursoort mapping: "Normale uren 100%" → Norm uren Dag, "Toeslag uren 133%" → T133 Dag, "Toeslag uren 135%" → T135 Dag, "Overuren 140%" → OW140 Week, "Overuren 180%" → OW180 Dag, "Overuren 200%" → OW200 Dag
- Negative hours in kloklijst = reclassification (e.g. Norm→OW on Saturday), this is normal
- Flex daily output is always empty — PDFs only have weekly totals, so only the weekly CSV is meaningful for Flex

## Name pair workflow
When `similarPeople` is returned, always present the pairs to the user in a clear table and ask for confirmation before calling `verify_name_pairs`. Include `agency` in the call so the correct validation is re-run.

## Style
- Be concise and practical
- Summarize discrepancies by category and count before listing details
- When both agencies are validated, present results per agency clearly
- Respond in the same language the user writes in (Dutch or English)
"""

# ---------------------------------------------------------------------------
# Main conversation loop
# ---------------------------------------------------------------------------

def run_agent():
    client = anthropic.Anthropic()
    messages: list[dict] = []

    print("Padifood Invoice Verification Agent")
    print("Type 'quit' or 'exit' to stop.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if user_input.lower() in {"quit", "exit", "q"}:
            print("Goodbye.")
            break
        if not user_input:
            continue

        messages.append({"role": "user", "content": user_input})

        # Agentic loop: keep calling until no more tool use
        while True:
            print("Agent: ", end="", flush=True)

            # Stream response
            full_content: list[dict] = []
            text_buffer = ""

            with client.messages.stream(
                model="claude-opus-4-6",
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            ) as stream:
                for event in stream:
                    if event.type == "content_block_start":
                        block = event.content_block
                        if block.type == "text":
                            pass  # will stream via deltas
                        elif block.type == "tool_use":
                            if text_buffer:
                                print()  # newline before tool call
                            print(f"\n[Calling tool: {block.name}]", flush=True)
                    elif event.type == "content_block_delta":
                        delta = event.delta
                        if delta.type == "text_delta":
                            print(delta.text, end="", flush=True)
                            text_buffer += delta.text

                response = stream.get_final_message()

            print()  # newline after response

            # Build content list from response for message history
            raw_content = response.content
            messages.append({"role": "assistant", "content": raw_content})

            # Check if done
            if response.stop_reason != "tool_use":
                break

            # Execute tool calls
            tool_results = []
            for block in raw_content:
                if block.type != "tool_use":
                    continue
                print(f"[Running {block.name}({json.dumps(block.input, ensure_ascii=False)[:120]}...)]", flush=True)
                result = _execute_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            messages.append({"role": "user", "content": tool_results})


if __name__ == "__main__":
    if "--url" in sys.argv:
        idx = sys.argv.index("--url")
        if idx + 1 < len(sys.argv):
            BACKEND_URL = sys.argv[idx + 1]

    if not BACKEND_API_KEY:
        print("Warning: BACKEND_API_KEY not set in .env — API calls will be rejected.\n")

    run_agent()
