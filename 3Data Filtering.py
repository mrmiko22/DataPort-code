import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


INPUT_DIR = r"2Data Preprocessing"
OUTPUT_DIR = r"3Data Filtering"

VALUE_PREFIX = "Value_"
MISSING_TOKENS = [" ", "", "NULL", "null", "None", "none", "NaN", "nan"]


def require_private_float(env_name):
    value = os.getenv(env_name)
    if value is None:
        raise RuntimeError(
            f"Private configuration {env_name} is required. "
            "Set it outside the public repository before running this script."
        )
    return float(value)


def require_private_int(env_name):
    value = os.getenv(env_name)
    if value is None:
        raise RuntimeError(
            f"Private configuration {env_name} is required. "
            "Set it outside the public repository before running this script."
        )
    return int(value)


def require_private_bool(env_name):
    value = os.getenv(env_name)
    if value is None:
        raise RuntimeError(
            f"Private configuration {env_name} is required. "
            "Set it outside the public repository before running this script."
        )
    return value.strip().lower() in {"1", "true", "yes", "y"}


def load_private_config():
    return {
        "min_valid_days": require_private_int("TYMPSSMD_MIN_VALID_DAYS"),
        "iqr_multiplier": require_private_float("TYMPSSMD_IQR_MULTIPLIER"),
        "long_gap_min_points": require_private_int("TYMPSSMD_LONG_GAP_MIN_POINTS"),
        "zero_as_missing": require_private_bool("TYMPSSMD_ZERO_AS_MISSING"),
    }


def get_value_columns(df):
    return [col for col in df.columns if str(col).startswith(VALUE_PREFIX)]


def read_csv_with_fallback(path):
    try:
        return pd.read_csv(path, encoding="gbk")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")


def get_new_transformer_name(index):
    if index < 26:
        return chr(ord("A") + index)
    return f"Trans_{index}"


def normalize_missing_values(df, value_cols, zero_as_missing):
    df = df.replace(MISSING_TOKENS, np.nan)
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if zero_as_missing:
        df[value_cols] = df[value_cols].replace(0, np.nan)
    return df


def contiguous_nan_runs(mask):
    runs = []
    start = None
    for idx, is_missing in enumerate(mask):
        if is_missing and start is None:
            start = idx
        elif not is_missing and start is not None:
            runs.append((start, idx))
            start = None
    if start is not None:
        runs.append((start, len(mask)))
    return runs


def adjacent_day_profile_fill(df, value_cols, original_missing_mask, long_gap_min_points):
    repaired = df.copy()
    values = repaired[value_cols].to_numpy(dtype=float)
    missing_mask = repaired[value_cols].isna().to_numpy()
    original_mask = original_missing_mask[value_cols].to_numpy()

    for row_idx in range(values.shape[0]):
        for start, end in contiguous_nan_runs(original_mask[row_idx]):
            if end - start < long_gap_min_points:
                continue

            target_cols = list(range(start, end))
            candidates = []
            if row_idx > 0:
                candidates.append(values[row_idx - 1, target_cols])
            if row_idx + 1 < values.shape[0]:
                candidates.append(values[row_idx + 1, target_cols])
            if not candidates:
                continue

            profile = np.nanmean(np.vstack(candidates), axis=0)
            for offset, col_idx in enumerate(target_cols):
                if missing_mask[row_idx, col_idx] and not np.isnan(profile[offset]):
                    values[row_idx, col_idx] = profile[offset]
                    missing_mask[row_idx, col_idx] = False

    repaired[value_cols] = values
    return repaired


def repair_missing_values(df, value_cols, config):
    original_missing_mask = df[value_cols].isna()

    work = df.copy()
    numeric = work[value_cols].copy()
    original_cols = list(numeric.columns)
    numeric.columns = range(len(original_cols))

    try:
        numeric = numeric.interpolate(method="pchip", axis=1, limit_area="inside")
    except Exception:
        numeric = numeric.interpolate(method="linear", axis=1, limit_area="inside")

    numeric = numeric.ffill(axis=1).bfill(axis=1)
    numeric.columns = original_cols
    work[value_cols] = numeric

    work = adjacent_day_profile_fill(
        work,
        value_cols,
        original_missing_mask,
        config["long_gap_min_points"],
    )
    work[value_cols] = work[value_cols].interpolate(
        method="linear",
        axis=0,
        limit_direction="both",
    )
    return work


def clean_outliers_iqr(df, config):
    df_clean = df.copy()
    value_cols = get_value_columns(df_clean)
    cleaned_count = 0

    for col in value_cols:
        series = pd.to_numeric(df_clean[col], errors="coerce")
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue

        lower_bound = q1 - config["iqr_multiplier"] * iqr
        upper_bound = q3 + config["iqr_multiplier"] * iqr
        outlier_mask = (series < lower_bound) | (series > upper_bound)

        if outlier_mask.any():
            cleaned_count += int(outlier_mask.sum())
            df_clean.loc[outlier_mask, col] = np.nan

    if cleaned_count:
        df_clean = repair_missing_values(df_clean, value_cols, config)

    return df_clean, cleaned_count


def process_single_transformer_data(trans_path, config):
    files = [f for f in os.listdir(trans_path) if f.endswith(".csv")]
    if not files:
        return None

    data_dict = {}
    valid_dates_list = []

    for filename in files:
        file_path = os.path.join(trans_path, filename)
        df = read_csv_with_fallback(file_path)

        date_col_name = df.columns[0]
        try:
            df[date_col_name] = pd.to_datetime(df[date_col_name]).dt.strftime("%Y/%m/%d")
        except Exception:
            return None

        value_cols = get_value_columns(df)
        if not value_cols:
            return None

        df = normalize_missing_values(df, value_cols, config["zero_as_missing"])

        missing_counts = df[value_cols].isna().sum(axis=1)
        valid_mask = missing_counts < len(value_cols)
        valid_dates_list.append(set(df.loc[valid_mask, date_col_name].values))
        data_dict[filename] = df

    if not valid_dates_list:
        return None

    common_dates = set.intersection(*valid_dates_list)
    if len(common_dates) < config["min_valid_days"]:
        return None

    cleaned_data = {}
    for filename, df in data_dict.items():
        date_col_name = df.columns[0]
        filtered_df = df[df[date_col_name].isin(common_dates)].copy()
        filtered_df = filtered_df.sort_values(by=date_col_name).reset_index(drop=True)
        filtered_df, _ = clean_outliers_iqr(filtered_df, config)
        cleaned_data[filename] = filtered_df

    return cleaned_data


def main():
    config = load_private_config()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    raw_line_folders = [
        f for f in os.listdir(INPUT_DIR)
        if os.path.isdir(os.path.join(INPUT_DIR, f))
    ]
    sorted_line_folders = sorted(
        raw_line_folders,
        key=lambda item: int(item) if item.isdigit() else item,
    )

    valid_line_count = 1

    for old_line_code in sorted_line_folders:
        line_path = os.path.join(INPUT_DIR, old_line_code)
        print(f"Scanning feeder folder: {old_line_code}")

        valid_transformers_buffer = []
        trans_folders = sorted(os.listdir(line_path))

        for old_trans_code in trans_folders:
            trans_path = os.path.join(line_path, old_trans_code)
            if not os.path.isdir(trans_path):
                continue

            cleaned_data_map = process_single_transformer_data(trans_path, config)
            if cleaned_data_map:
                valid_transformers_buffer.append((old_trans_code, cleaned_data_map))
            else:
                print(f"  [X] Transformer removed: {old_trans_code}")

        if not valid_transformers_buffer:
            print(f"  [!] Feeder removed: {old_line_code}")
            continue

        new_line_code = str(valid_line_count)
        output_line_path = os.path.join(OUTPUT_DIR, new_line_code)
        os.makedirs(output_line_path, exist_ok=True)
        print(f"  [OK] Feeder retained: {old_line_code} -> {new_line_code}")

        for valid_trans_count, (old_trans_code, data_map) in enumerate(valid_transformers_buffer):
            new_trans_code = get_new_transformer_name(valid_trans_count)
            output_trans_path = os.path.join(output_line_path, new_trans_code)
            os.makedirs(output_trans_path, exist_ok=True)

            for file_name, df in data_map.items():
                save_path = os.path.join(output_trans_path, file_name)
                df.to_csv(save_path, index=False, encoding="gbk")

            print(f"    - Transformer saved: {old_trans_code} -> {new_trans_code}")

        valid_line_count += 1

    print("=" * 50)
    print(f"Number of original feeder folders: {len(sorted_line_folders)}")
    print(f"Number of retained feeder folders: {valid_line_count - 1}")
    print("=" * 50)


if __name__ == "__main__":
    main()
