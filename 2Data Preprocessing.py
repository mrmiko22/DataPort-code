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
    log("Commencing preprocessing of this file")

    df = df.replace([" ", "", "NULL", "null", "None"], np.nan)

    temp = df.replace(0, np.nan)

    # ---------------------------
    # 1) Remove rows where the proportion of missing values exceeds x
    # ---------------------------
    log("Detect rows where the number of 0s or NaNs exceeds x")
    missing_ratio = temp.isna().mean(axis=1)
    df = df.loc[missing_ratio <= x].copy()

    df = df.replace(0, np.nan)

    # ---------------------------
    # 2) Interpolation + Completion Logic
    # ---------------------------
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    if len(numeric_cols) > 0:
        log("Implementing a multi-level backfilling strategy (Optimized for Power Systems)...")

        # =========================================================
        # CRITICAL UPDATE: Fix PCHIP column name error & Prevent tail divergence
        # =========================================================

        # 1. Create a subset and rename columns to integers (0, 1, 2...)
        # This is necessary because PCHIP requires numeric indices to calculate distances.
        df_subset = df[numeric_cols].copy()
        original_cols = df_subset.columns
        df_subset.columns = range(len(df_subset.columns))

        # 2. Level 1: Lateral PCHIP Interpolation (Inside Area Only)
        # limit_area='inside' prevents the "tail divergence" issue (e.g., -54.701)
        # by ensuring we only interpolate between existing points, not extrapolate edges.
        log("1. Perform lateral PCHIP conformal interpolation (Inside area only)")
        try:
            df_subset = df_subset.interpolate(method='pchip', axis=1, limit_area='inside')
        except Exception as e:
            log(f"PCHIP failed (fallback to Linear): {e}")
            df_subset = df_subset.interpolate(method='linear', axis=1, limit_area='inside')

        # 3. Handle Edge Cases (Rows starting or ending with NaNs)
        # Use flat filling (Forward/Backward Fill) for the tails to be safe.
        df_subset = df_subset.ffill(axis=1).bfill(axis=1)

        # 4. Restore original column names and update dataframe
        df_subset.columns = original_cols
        df[numeric_cols] = df_subset

        # ---------------------------------------------------------

        # 5. Level 2: Vertical Linear Interpolation
        # Uses trends from adjacent days to fill remaining gaps.
        log("2. Perform vertical linear interpolation")
        df[numeric_cols] = df[numeric_cols].interpolate(method='linear', axis=0, limit_direction='both')

        # 6. Level 3: Full-range Boundary Filling
        log("3. Perform full-range boundary filling")
        df[numeric_cols] = df[numeric_cols].ffill(axis=0).bfill(axis=0)

        # 7. Level 4: Global Median Fallback
        if df[numeric_cols].isna().any().any():
            log("Detected persistent missing values; imputed using the global median.")
            for col in numeric_cols:
                if df[col].isna().any():
                    median_val = df[col].median()
                    fill_val = 0 if pd.isna(median_val) else median_val
                    df[col] = df[col].fillna(fill_val)

    # ---------------------------
    # 3) Type inference avoids FutureWarning
    # ---------------------------
    df = df.infer_objects(copy=False)

    # ---------------------------
    # 4) Final format: All values retained to 4 decimal places
    # ---------------------------
    if len(numeric_cols) > 0:
        log("All values are formatted as floats, retaining four decimal places.")
        df[numeric_cols] = df[numeric_cols].astype(float).round(4)

    return df


# ==============================================
# Processing a single CSV file
# ==============================================
def process_single_file(in_path, out_path):
    log(f"Read CSV：{in_path}")
    df = pd.read_csv(in_path, encoding="utf-8", low_memory=False)

    df = preprocess_dataframe(df)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    log(f"Write CSV：{out_path}")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")


# ==============================================
# Main
# ==============================================
def process_all_files(input_root, output_root):
    log("Begin batch processing all files")

    for dirpath, dirnames, filenames in os.walk(input_root):
        for filename in filenames:
            if not filename.lower().endswith(".csv"):
                continue

            in_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(dirpath, input_root)
            out_dir = os.path.join(output_root, rel_path)
            out_path = os.path.join(out_dir, filename)

            log("=" * 80)
            log(f"Commencing processing of the document：{filename}")
            try:
                process_single_file(in_path, out_path)
                log(f"File processing successful：{filename}")
            except Exception as e:
                log(f"An error occurred while processing the file {filename}:{e}")

    log("All documents have been processed.")


# ==============================================
# Entrance
# ==============================================
if __name__ == "__main__":
    input_root = r"1Data extraction"
    output_root = r"2Data Preprocessing"

    process_all_files(input_root, output_root)
