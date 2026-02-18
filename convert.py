import openpyxl, csv, os, re

input_dir = 'input'
output_dir = 'formatted_input'

for fname in os.listdir(input_dir):
    if fname.startswith('~$') or not fname.endswith('.xlsx'):
        continue
    fpath = os.path.join(input_dir, fname)
    base = re.sub(r'\.xlsx$', '', fname)
    safe_base = base.replace(',', '').replace('  ', ' ').strip()
    try:
        wb = openpyxl.load_workbook(fpath, data_only=True)
        for sn in wb.sheetnames:
            ws = wb[sn]
            safe_sn = sn.replace('/', '-').replace('\\', '-').strip()
            if len(wb.sheetnames) > 1:
                out_name = f'{safe_base} - {safe_sn}.csv'
            else:
                out_name = f'{safe_base}.csv'
            out_path = os.path.join(output_dir, out_name)
            with open(out_path, 'w', newline='', encoding='utf-8-sig') as csvf:
                writer = csv.writer(csvf)
                for row in ws.iter_rows(values_only=True):
                    writer.writerow([v if v is not None else '' for v in row])
            print(f'Created: {out_name}')
    except Exception as e:
        print(f'ERROR with {fname}: {e}')
