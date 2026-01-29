import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


# ==============================================
# Logarithmic function
# ==============================================
def log(msg):
    print(f"[INFO] {msg}")


# ==============================================
# Core Logic of Data Preprocessing
# ==============================================
def preprocess_dataframe(df):
    log("Commencing pre-processing of power system data")

    # 1. Foundation clearance
    df = df.replace([" ", "", "NULL", "null", "None"], np.nan)

    date_col = df.columns[0]
    data_cols = df.columns[1:]

    df[data_cols] = df[data_cols].apply(pd.to_numeric, errors='coerce').replace(0, np.nan)

    # ---------------------------
    # 1) Remove rows with significant data loss
    # ---------------------------
    missing_ratio = df[data_cols].isna().mean(axis=1)
    df = df.loc[missing_ratio <= x].copy()  #Here, x requires the user to modify a value within the range [0, 1].

    if df.empty:
        log("Warning: All lines in this file have a missing rate exceeding x per cent; skipping.")
        return df

    # ---------------------------
    # 2) Longitudinal filling
    # ---------------------------
    log("执行纵向均值填充（基于时刻特征）")
    df[data_cols] = df[data_cols].fillna(df[data_cols].mean())

    # ---------------------------
    # 3) Horizontal interpolation
    # ---------------------------
    log("Perform horizontal linear interpolation")
    df[data_cols] = df[data_cols].interpolate(method="linear", axis=1, limit_direction="both")

    # ---------------------------
    # 4) Ultimately, the safety net
    # ---------------------------
    if df[data_cols].isna().any().any():
        log("Perform global default filling")
        global_mean = df[data_cols].stack().mean()
        df[data_cols] = df[data_cols].fillna(global_mean)

    # ---------------------------
    # 5) Formatting
    # ---------------------------
    df[data_cols] = df[data_cols].astype(float).round(4)

    return df

# ==============================================
# Processing a single CSV file
# ==============================================
def process_single_file(in_path, out_path):
    log(f"Reading CSV: {in_path}")
    try:

        df = pd.read_csv(in_path, encoding="utf-8", low_memory=False, dtype=str)

        if df.empty:
            log(f"Skipping empty file: {in_path}")
            return

        df = preprocess_dataframe(df)

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        log(f"Write CSV: {out_path}")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

    except Exception as e:
        log(f"Error reading/processing {in_path}: {e}")
        raise e


# ==============================================
# Main Process: Maintain Directory Structure
# ==============================================
def process_all_files(input_root, output_root):
    log("Begin batch processing all files")

    if not os.path.exists(input_root):
        log(f"Input folder not found: {input_root}")
        return

    success_count = 0
    fail_count = 0

    for dirpath, dirnames, filenames in os.walk(input_root):
        for filename in filenames:
            if not filename.lower().endswith(".csv"):
                continue

            in_path = os.path.join(dirpath, filename)

            rel_path = os.path.relpath(dirpath, input_root)
            out_dir = os.path.join(output_root, rel_path)
            out_path = os.path.join(out_dir, filename)

            log("=" * 80)
            log(f"Processing: {filename}")
            try:
                process_single_file(in_path, out_path)
                log(f"Success: {filename}")
                success_count += 1
            except Exception as e:
                log(f"Failed: {filename} -> {e}")
                fail_count += 1

    log("=" * 80)
    log(f"All files processed. Success: {success_count}, Failed: {fail_count}")


# ==============================================
# Entrance
# ==============================================
if __name__ == "__main__":
    input_root = r"1Data extraction"
    output_root = r"2Data Preprocessing"

    process_all_files(input_root, output_root)
