import os
import pandas as pd
import warnings
from itertools import count, product

warnings.filterwarnings('ignore')

# ==================== Deployment ====================
input_folder = r"0Replace with the original data location"
output_root = r"1Data extraction"
# ================================================

os.makedirs(output_root, exist_ok=True)

# ========= Indicator =========
metric_cols = [
    'AXDL', 'BXDL', 'CXDL',
    'AXDY', 'BXDY', 'CXDY',
    'YGGL', 'AXYGGL', 'BXYGGL', 'CXYGGL',
    'WGGL', 'AXWGGL', 'BXWGGL', 'CXWGGL'
]

required_cols = ['BYQJH', 'SJSJ'] + metric_cols

# ========= Desensitization =========
line_map = {}
line_counter = count(1)
byq_map = {}
BYQ_ALPHABETS = [f"{i[0]}" for i in product('ABCDEFGHIJKLMNOPQRSTUVWXYZ', repeat=1)]

all_generated_csvs = []


def parse_sjsj(time_str):
    try:
        time_str = str(time_str).strip()
        if not time_str or time_str.lower() == 'nan':
            return pd.NaT
        parts = time_str.replace('-', '/').split(' ')
        if len(parts) != 2:
            return pd.NaT
        date_part, time_part = parts
        try:
            y, m, d = date_part.split('/')
            y, m, d = y, m.zfill(2), d.zfill(2)
            time_dt_str = f"{y}-{m}-{d} {time_part}"
        except ValueError:
            time_dt_str = time_str.replace('/', '-')
        return pd.to_datetime(time_dt_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except:
        return pd.NaT


def get_daily_96_points_template(date):
    start = pd.to_datetime(f'{date} 00:00:00')
    end = pd.to_datetime(f'{date} 23:45:00')
    full_day_series = pd.date_range(start=start, end=end, freq='15min')
    template_df = pd.DataFrame(full_day_series, columns=['SJSJ_Full'])
    return template_df


# ========= Iterate through each route CSV file =========
for filename in os.listdir(input_folder):
    if not filename.lower().endswith('.csv'):
        continue

    # --- 1. Line desensitization ---
    if filename not in line_map:
        line_map[filename] = str(next(line_counter))
    line_code = line_map[filename]

    line_output_folder = os.path.join(output_root, line_code)
    os.makedirs(line_output_folder, exist_ok=True)

    file_path = os.path.join(input_folder, filename)
    print(f"\nProcessing line files: {filename} -> Route Code {line_code}")

    try:
        df = pd.read_csv(file_path, encoding='utf-8', dtype=str, on_bad_lines='skip', low_memory=False)
        print(f"  Number of rows in the original data: {len(df)}")
    except Exception as e:
        print(f"  Read operation failed: {e}")
        continue

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"  Skip: Missing column {missing_cols}")
        continue

    # ========= 2. Analysis of SJSJ =========
    df['SJSJ_parsed'] = df['SJSJ'].apply(parse_sjsj)
    df = df[df['SJSJ_parsed'].notna()].copy()
    if df.empty:
        print(f"  Skip: All SJSJ parsing failed")
        continue

    df['SJSJ'] = df['SJSJ_parsed']
    df = df.drop(columns=['SJSJ_parsed'])

    # ========= 3. Deduplication =========
    df['__hash'] = df.apply(lambda row: (row['SJSJ'],) + tuple(row[col] for col in metric_cols), axis=1)
    df = df.drop_duplicates(subset=['__hash'], keep='first')
    df = df.drop(columns=['__hash'])

    if df.empty:
        print(f"  Skip: Empty after deduplication")
        continue

    # ========= 4. Add date string =========
    df['date_str'] = df['SJSJ'].dt.strftime('%Y-%m-%d')

    # ========= 5. Check if BYQJH is empty and group =========
    df['BYQJH'] = df['BYQJH'].astype(str).str.strip()
    df = df[df['BYQJH'] != '']
    byqjh_groups = df.groupby('BYQJH')

    # --- 6. Transformer Desensitization and Batch Writing ---
    byq_code_counter = 0
    line_byq_map = {}

    for byqjh_orig, group in byqjh_groups:

        # --- Transformer Desensitization ---
        if byqjh_orig not in line_byq_map:
            if byq_code_counter >= len(BYQ_ALPHABETS):
                byq_code = f"Z{byq_code_counter - len(BYQ_ALPHABETS) + 1}"
            else:
                byq_code = BYQ_ALPHABETS[byq_code_counter]
            line_byq_map[byqjh_orig] = byq_code
            byq_code_counter += 1

        byq_code = line_byq_map[byqjh_orig]

        byq_folder = os.path.join(line_output_folder, byq_code)
        os.makedirs(byq_folder, exist_ok=True)

        # Processing logic grouped by day
        for date, day_group in group.groupby(group['SJSJ'].dt.date):
            date_str = day_group['date_str'].iloc[0]
            template_df = get_daily_96_points_template(date_str)

            for metric in metric_cols:
                metric_df = day_group[['SJSJ', metric]].rename(columns={metric: 'Metric_Value'}).copy()
                metric_df['Metric_Value'] = pd.to_numeric(metric_df['Metric_Value'], errors='coerce')
                merged_df = template_df.merge(
                    metric_df,
                    left_on='SJSJ_Full',
                    right_on='SJSJ',
                    how='left'
                ).drop(columns=['SJSJ'])

                values = merged_df['Metric_Value'].fillna('').tolist()

                if len(values) != 96:
                    continue

                row = [byq_code, date_str] + values
                csv_file = os.path.join(byq_folder, f"{metric}.csv")
                all_generated_csvs.append(csv_file)
                write_header = not os.path.exists(csv_file)

                header_names = ['BYQJH_Code', 'Date'] + [f"Value_{i + 1}" for i in range(96)]
                row_df = pd.DataFrame([row], columns=header_names)
                row_df.to_csv(csv_file, mode='a', index=False, header=write_header, encoding='utf-8-sig')

        print(f"  Transformers generated: {byqjh_orig} -> Code Name {byq_code}")

    print(f"Route File {filename} Processing complete!")


# --- 7. Post-processing ---
def post_process_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig', header=0)
        if not df.empty and df.columns[0] == 'BYQJH_Code':
            df = df.iloc[:, 1:]
            df.to_csv(file_path, mode='w', index=False, header=True, encoding='utf-8-sig')
            return True
        else:
            return False
    except Exception as e:
        print(f"  Post-processing files {file_path} Failure: {e}")
        return False


print("\n" + "=" * 50)

print("=" * 50)
print(f"All route files have been processed. Post-processing of {len(all_generated_csvs)} files is now beginning...")

unique_csvs = list(set(all_generated_csvs))
post_processed_count = 0

for i, csv_file in enumerate(unique_csvs):
    if post_process_csv(csv_file):
        post_processed_count += 1
    if (i + 1) % 100 == 0:
        print(f"  Post-processing completed for {i + 1} out of {len(unique_csvs)} files....")

print(f"Post-processing complete! A total of {post_processed_count} files have been modified.")
print("=" * 50)
print(f"Final Results Directory: {output_root}")