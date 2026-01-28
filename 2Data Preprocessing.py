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
    log("Begin preprocessing this file")

    df = df.replace([" ", "", "NULL", "null", "None"], np.nan)


    for col in df.columns:

        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            continue

    temp = df.replace(0, np.nan)

    # ---------------------------
    # 3) Delete rows with missing values exceeding x%
    # ---------------------------
    log("Detect rows where 0 or NaN values exceed x% of the entire row.")

    missing_ratio = temp.isna().mean(axis=1)
    df = df.loc[missing_ratio <= x].copy()  #Here, x requires the user to modify a value within the range [0, 1].

    df = df.replace(0, np.nan)

    # ---------------------------
    # 4) Interpolation + Completion Logic
    # ---------------------------
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    if len(numeric_cols) > 0:
        log(f"Processing {len(numeric_cols)} numeric columns for interpolation...")

        df[numeric_cols] = df[numeric_cols].interpolate(method="linear", limit_direction="both")

        log("Perform forward padding + backward padding")
        df[numeric_cols] = df[numeric_cols].ffill().bfill()

        for col in numeric_cols:
            if df[col].isna().any():
                median_val = df[col].median()
                if pd.notna(median_val):
                    df[col] = df[col].fillna(median_val)
                    log(f"Column {col} imputed with median: {median_val}")
                else:
                    df[col] = df[col].fillna(0)

    # ---------------------------
    # 5) Type inference & Formatting
    # ---------------------------
    df = df.infer_objects(copy=False)

    if len(numeric_cols) > 0:
        log("All values are formatted as floats and rounded to 4 decimal places.")

        df[numeric_cols] = df[numeric_cols].apply(lambda x: x.astype(float).round(4))

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