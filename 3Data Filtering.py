import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# ================= Configuration Area =================
INPUT_DIR = r"2Data Preprocessing"

OUTPUT_DIR = r"3Data Filtering"

MIN_VALID_DAYS = x   #Here, x requires the user to modify a value.

ENABLE_OUTLIER_CLEANING = True

# ===========================================

def get_new_transformer_name(index):

    if index < 26:
        return chr(ord('A') + index)
    else:
        return f"Trans_{index}"


def clean_outliers_iqr(df):

    df_clean = df.copy()

    value_cols = df_clean.columns[1:]

    cleaned_count = 0

    for col in value_cols:
        series = df_clean[col]

        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - y * IQR  #Here, y requires the user to modify a value.
        upper_bound = Q3 + y * IQR  #Here, y requires the user to modify a value identical to that in the preceding line.

        outlier_mask = (series < lower_bound) | (series > upper_bound)

        if outlier_mask.any():
            cleaned_count += outlier_mask.sum()
            df_clean.loc[outlier_mask, col] = np.nan
            df_clean[col] = df_clean[col].interpolate(method='linear', limit_direction='both')

    return df_clean, cleaned_count


def process_single_transformer_data(trans_path):

    files = [f for f in os.listdir(trans_path) if f.endswith('.csv')]
    if not files:
        return None

    data_dict = {}
    valid_dates_list = []

    for file in files:
        file_path = os.path.join(trans_path, file)
        try:
            df = pd.read_csv(file_path, encoding='gbk')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='utf-8')

        # 1. Standardization Date
        date_col_name = df.columns[0]
        try:
            df[date_col_name] = pd.to_datetime(df[date_col_name]).dt.strftime('%Y/%m/%d')
        except Exception:
            return None

        # 2. Convert 0 and null values to NaN
        data_cols = df.columns[1:]
        for col in data_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df[data_cols] = df[data_cols].replace([0, 0.0], np.nan)

        # 3. Determination of Effective Date
        missing_counts = df[data_cols].isna().sum(axis=1)
        valid_mask = missing_counts < len(data_cols)
        current_valid_dates = set(df.loc[valid_mask, date_col_name].values)
        valid_dates_list.append(current_valid_dates)
        data_dict[file] = df

    if not valid_dates_list:
        return None

    # 4. Take the intersection
    common_dates = set.intersection(*valid_dates_list)

    if not common_dates:
        return None

    # 5. Number of days to filter
    if len(common_dates) < MIN_VALID_DAYS:
        return None

    # 6. Generate final data
    cleaned_data = {}
    for file, df in data_dict.items():
        date_col_name = df.columns[0]
        filtered_df = df[df[date_col_name].isin(common_dates)].copy()
        filtered_df = filtered_df.sort_values(by=date_col_name)

        filtered_df = filtered_df.reset_index(drop=True)

        cleaned_data[file] = filtered_df

    return cleaned_data


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # --- 1. Read the original mapping table ---
    raw_line_folders = [f for f in os.listdir(INPUT_DIR) if os.path.isdir(os.path.join(INPUT_DIR, f))]
    sorted_line_folders = sorted(raw_line_folders, key=lambda x: int(x) if x.isdigit() else x)

    valid_line_count = 1

    # --- 2. Traverse the route ---
    for old_line_code in sorted_line_folders:
        line_path = os.path.join(INPUT_DIR, old_line_code)
        print(f"Scanning intermediate state circuit: {old_line_code} ...")

        valid_transformers_buffer = []
        trans_folders = sorted(os.listdir(line_path))

        # --- 3. Traversal Transformer ---
        for old_trans_code in trans_folders:
            trans_path = os.path.join(line_path, old_trans_code)
            if not os.path.isdir(trans_path):
                continue

            cleaned_data_map = process_single_transformer_data(trans_path)

            if cleaned_data_map:
                valid_transformers_buffer.append((old_trans_code, cleaned_data_map))
            else:
                print(f"  [X] Transformer removal: {old_trans_code} (insufficient data)")

        # --- 4. Keep valid routes ---
        if valid_transformers_buffer:
            new_line_code = str(valid_line_count)
            output_line_path = os.path.join(OUTPUT_DIR, new_line_code)
            os.makedirs(output_line_path, exist_ok=True)

            print(f"  [√] Line retention: {old_line_code} -> Final number {new_line_code}")

            valid_trans_count = 0

            for old_trans_code, data_map in valid_transformers_buffer:
                new_trans_code = get_new_transformer_name(valid_trans_count)

                # --- Outlier Handling ---
                output_trans_path = os.path.join(output_line_path, new_trans_code)
                os.makedirs(output_trans_path, exist_ok=True)

                total_cleaned_points = 0

                for file_name, df in data_map.items():
                    if ENABLE_OUTLIER_CLEANING:
                        df, count = clean_outliers_iqr(df)
                        total_cleaned_points += count
                    # <-------------------->

                    save_path = os.path.join(output_trans_path, file_name)
                    df.to_csv(save_path, index=False, encoding='gbk')

                if ENABLE_OUTLIER_CLEANING and total_cleaned_points > 0:
                    print(f"    - Transformer saved: {new_trans_code} (Cleaned abnormal points: {total_cleaned_points} points))")
                else:
                    print(f"    - Save Transformer: {new_trans_code}")

                valid_trans_count += 1

            valid_line_count += 1
        else:
            print(f"  [!] Line removal: {old_line_code} (No valid data)")

    # --- 5. Save the final mapping table ---
    print("\n" + "=" * 50)

    print(f"Number of original route folders: {len(sorted_line_folders)}")
    print(f"Number of remaining active lines: {valid_line_count - 1}")
    print("=" * 50)


if __name__ == "__main__":
    main()