import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


INPUT_ROOT = r"1Data extraction"
OUTPUT_ROOT = r"2Data Preprocessing"

VALUE_PREFIX = "Value_"
MISSING_TOKENS = [" ", "", "NULL", "null", "None", "none", "NaN", "nan"]


def log(msg):
    print(f"[INFO] {msg}")


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
        "daily_missing_ratio_threshold": require_private_float(
            "TYMPSSMD_DAILY_MISSING_RATIO_THRESHOLD"
        ),
        "long_gap_min_points": require_private_int(
            "TYMPSSMD_LONG_GAP_MIN_POINTS"
        ),
        "zero_as_missing": require_private_bool(
            "TYMPSSMD_ZERO_AS_MISSING"
        ),
    }


def get_value_columns(df):
    return [col for col in df.columns if str(col).startswith(VALUE_PREFIX)]


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

    log("Perform shape preserving intraday interpolation")
    try:
        numeric = numeric.interpolate(method="pchip", axis=1, limit_area="inside")
    except Exception as exc:
        log(f"PCHIP interpolation failed; falling back to linear interpolation: {exc}")
        numeric = numeric.interpolate(method="linear", axis=1, limit_area="inside")

    log("Fill boundary gaps within each retained day")
    numeric = numeric.ffill(axis=1).bfill(axis=1)

    numeric.columns = original_cols
    work[value_cols] = numeric

    log("Apply adjacent day historical profile imputation for longer contiguous gaps")
    work = adjacent_day_profile_fill(
        work,
        value_cols,
        original_missing_mask,
        config["long_gap_min_points"],
    )

    log("Apply inter day interpolation for remaining local gaps")
    work[value_cols] = work[value_cols].interpolate(
        method="linear",
        axis=0,
        limit_direction="both",
    )

    return work


def preprocess_dataframe(df, config):
    log("Commencing preprocessing of this file")

    value_cols = get_value_columns(df)
    if not value_cols:
        raise ValueError("No Value_* columns were found in the input file.")

    df = normalize_missing_values(df, value_cols, config["zero_as_missing"])

    log("Remove days whose missing ratio exceeds the private threshold")
    missing_ratio = df[value_cols].isna().mean(axis=1)
    df = df.loc[missing_ratio <= config["daily_missing_ratio_threshold"]].copy()

    if df.empty:
        return df

    df = repair_missing_values(df, value_cols, config)
    df = df.infer_objects(copy=False)
    df[value_cols] = df[value_cols].astype(float).round(4)

    return df


def process_single_file(in_path, out_path, config):
    log(f"Read CSV: {in_path}")
    df = pd.read_csv(in_path, encoding="utf-8", low_memory=False)
    df = preprocess_dataframe(df, config)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    log(f"Write CSV: {out_path}")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")


def process_all_files(input_root, output_root):
    config = load_private_config()
    log("Begin batch preprocessing")

    for dirpath, _, filenames in os.walk(input_root):
        for filename in filenames:
            if not filename.lower().endswith(".csv"):
                continue

            in_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(dirpath, input_root)
            out_dir = os.path.join(output_root, rel_path)
            out_path = os.path.join(out_dir, filename)

            log("=" * 80)
            log(f"Processing file: {filename}")
            try:
                process_single_file(in_path, out_path, config)
                log(f"File processed successfully: {filename}")
            except Exception as exc:
                log(f"An error occurred while processing {filename}: {exc}")

    log("All documents have been processed.")


if __name__ == "__main__":
    process_all_files(INPUT_ROOT, OUTPUT_ROOT)
