import openpyxl, csv, os, re
from datetime import datetime, date

input_dir = 'input'
output_dir = 'formatted_input'


def get_week_prefix(wb):
    """
    Scan sheets for a 'Datum' column and return a 'YYYYww ' prefix derived from
    the first data value found. Returns '' if no Datum column is found.
    Dutch week numbering == ISO 8601, so isocalendar() is correct.
    """
    for sn in wb.sheetnames:
        ws = wb[sn]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        header = [str(c).strip() if c is not None else '' for c in rows[0]]
        if 'Datum' not in header:
            continue
        col_idx = header.index('Datum')
        for row in rows[1:]:
            val = row[col_idx] if col_idx < len(row) else None
            if val is None or val == '':
                continue
            if isinstance(val, (datetime, date)):
                d = val
            else:
                try:
                    d = datetime.strptime(str(val).strip()[:10], '%Y-%m-%d')
                except ValueError:
                    continue
            iso = d.isocalendar()  # (year, week, weekday)
            return f"{iso[0]}{iso[1]:02d} "
    return ''


for fname in os.listdir(input_dir):
    if fname.startswith('~$') or not fname.endswith('.xlsx'):
        continue
    fpath = os.path.join(input_dir, fname)
    base = re.sub(r'\.xlsx$', '', fname)
    safe_base = base.replace(',', '').replace('  ', ' ').strip()
    try:
        wb = openpyxl.load_workbook(fpath, data_only=True)
        # Only prepend the prefix if the filename doesn't already start with one (YYYYww )
        prefix = '' if re.match(r'^\d{6} ', safe_base) else get_week_prefix(wb)
        # Generic sheet names that don't add useful information to the filename
        GENERIC_SHEET_NAMES = {'sheet', 'sheet1', 'blad1', 'blad'}
        for sn in wb.sheetnames:
            ws = wb[sn]
            safe_sn = sn.replace('/', '-').replace('\\', '-').strip()
            is_generic = safe_sn.lower() in GENERIC_SHEET_NAMES
            if len(wb.sheetnames) > 1 and not is_generic:
                out_name = f'{prefix}{safe_base} - {safe_sn}.csv'
            else:
                out_name = f'{prefix}{safe_base}.csv'
            out_path = os.path.join(output_dir, out_name)
            with open(out_path, 'w', newline='', encoding='utf-8-sig') as csvf:
                writer = csv.writer(csvf)
                for row in ws.iter_rows(values_only=True):
                    writer.writerow([v if v is not None else '' for v in row])
            print(f'Created: {out_name}')
    except Exception as e:
        print(f'ERROR with {fname}: {e}')
