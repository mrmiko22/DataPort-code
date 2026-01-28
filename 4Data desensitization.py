import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# ==================== Deployment Zone ====================
input_root = r"3Data Filtering"
output_root = r"4Data desensitization"

os.makedirs(output_root, exist_ok=True)

DATE_COL = "Date"
VALUE_COLS = [f"Value_{i+1}" for i in range(96)]

# === Classification of noise intensity (based on physical characteristics) ===
NOISE_CURRENT_FACTOR = x  #Here, x requires the user to modify a value.
NOISE_VOLTAGE_FACTOR = y  #Here, y requires the user to modify a value.
NOISE_POWER_FACTOR = z  #Here, z requires the user to modify a value.

# === Listing mode ===
CURRENT_KEYS = ['XDL']
VOLTAGE_KEYS = ['XDY']
POWER_KEYS = ['GGL']
# ===============================================


def classify_column(col_name: str):
    if any(k in col_name.upper() for k in CURRENT_KEYS):
        return "current"
    if any(k in col_name.upper() for k in VOLTAGE_KEYS):
        return "voltage"
    if any(k in col_name.upper() for k in POWER_KEYS):
        return "power"
    return "unknown"


# ==================== Global Metrics Collection ====================
def collect_global_stats(input_root):
    print("========== Phase 1: Collection of Global Statistics ==========")

    current_vals = []
    voltage_vals = []
    power_vals = []

    for line in os.listdir(input_root):
        p1 = os.path.join(input_root, line)
        if not os.path.isdir(p1):
            continue
        print(f"[Level 1 Directory] {line}")

        for byq in os.listdir(p1):
            p2 = os.path.join(p1, byq)
            if not os.path.isdir(p2):
                continue
            print(f"  [Secondary directory] {byq}")

            for file in os.listdir(p2):
                if not file.endswith(".csv"):
                    continue
                print(f"    [Read the file] {file}（Statistical phase）")

                df = pd.read_csv(os.path.join(p2, file), encoding="utf-8-sig")
                vals = df[VALUE_COLS].replace("", np.nan).astype(float)

                ctype = classify_column(file)
                if ctype == "current":
                    current_vals.append(vals.stack().dropna().values)
                elif ctype == "voltage":
                    voltage_vals.append(vals.stack().dropna().values)
                elif ctype == "power":
                    power_vals.append(vals.stack().dropna().values)

    def calc_std(x):
        return np.std(np.concatenate(x)) if x else 1.0

    print("Global statistics collection completed。")

    return calc_std(current_vals), calc_std(voltage_vals), calc_std(power_vals)


# ========== Calculate the global standard deviation for three categories ==========
STD_CURRENT, STD_VOLTAGE, STD_POWER = collect_global_stats(input_root)

SIGMA_CURRENT = STD_CURRENT * NOISE_CURRENT_FACTOR
SIGMA_VOLTAGE = STD_VOLTAGE * NOISE_VOLTAGE_FACTOR
SIGMA_POWER = STD_POWER * NOISE_POWER_FACTOR

print("\nCalculate the standard deviation of the noise：")
print(f"  Current noise σ = {SIGMA_CURRENT:.6f}")
print(f"  voltage noise σ = {SIGMA_VOLTAGE:.6f}")
print(f"  Power noise σ = {SIGMA_POWER:.6f}\n")


# ==================== Noise addition function ====================
def apply_noise(df, file_type):
    vals = df[VALUE_COLS].astype(float)

    if file_type == "current":
        noise = np.random.normal(0, SIGMA_CURRENT, vals.shape)
        new_vals = np.maximum(vals + noise, 0)

    elif file_type == "voltage":
        noise = np.random.normal(0, SIGMA_VOLTAGE, vals.shape)
        new_vals = vals + noise

    elif file_type == "power":
        noise = np.random.normal(0, SIGMA_POWER, vals.shape)
        new_vals = vals * (1 + noise)

    else:
        noise = np.random.normal(0, STD_VOLTAGE * 0.01, vals.shape)
        new_vals = vals + noise

    df[VALUE_COLS] = np.round(new_vals, 4)
    return df


# ==================== Calculate the total number of files ====================
print("Counting the number of files awaiting processing...")

total_files = 0
for a in os.listdir(input_root):
    p1 = os.path.join(input_root, a)
    if not os.path.isdir(p1):
        continue
    for b in os.listdir(p1):
        p2 = os.path.join(p1, b)
        if not os.path.isdir(p2):
            continue
        for f in os.listdir(p2):
            if f.endswith(".csv"):
                total_files += 1

print(f"A total of {total_files} files need to be processed.\n")


# ==================== Batch data de-identification ====================
print("========== Stage 2: Commencing Batch Data De-identification ==========")

processed = 0

for line in os.listdir(input_root):
    p1 = os.path.join(input_root, line)
    if not os.path.isdir(p1):
        continue
    print(f"[Level 1 Directory] {line}")

    for byq in os.listdir(p1):
        p2 = os.path.join(p1, byq)
        if not os.path.isdir(p2):
            continue
        print(f"  [Secondary directory] {byq}")

        out_dir = os.path.join(output_root, line, byq)
        os.makedirs(out_dir, exist_ok=True)

        for file in os.listdir(p2):
            if not file.endswith(".csv"):
                continue

            processed += 1
            print(f"    [Processed {processed}/{total_files}] files:{file}")

            file_type = classify_column(file)

            df = pd.read_csv(os.path.join(p2, file), encoding="utf-8-sig")
            df = apply_noise(df, file_type)

            df.to_csv(os.path.join(out_dir, file),
                      index=False,
                      encoding="utf-8-sig")

print("\n========== All files have been processed. ==========")
