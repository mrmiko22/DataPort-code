import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


INPUT_ROOT = r"3Data Filtering"
OUTPUT_ROOT = r"4Data desensitization"

VALUE_COLS = [f"Value_{i + 1}" for i in range(96)]

CURRENT_KEYS = ["XDL"]
VOLTAGE_KEYS = ["XDY"]
POWER_KEYS = ["GGL"]

ACTIVE_TOTAL_KEY = "YGGL"
ACTIVE_PHASE_KEYS = ["AXYGGL", "BXYGGL", "CXYGGL"]
REACTIVE_TOTAL_KEY = "WGGL"
REACTIVE_PHASE_KEYS = ["AXWGGL", "BXWGGL", "CXWGGL"]


def require_private_float(env_name):
    value = os.getenv(env_name)
    if value is None:
        raise RuntimeError(
            f"Private configuration {env_name} is required. "
            "Set it outside the public repository before running this script."
        )
    return float(value)


def load_private_config():
    return {
        "noise_current_factor": require_private_float("TYMPSSMD_NOISE_CURRENT_FACTOR"),
        "noise_voltage_factor": require_private_float("TYMPSSMD_NOISE_VOLTAGE_FACTOR"),
        "noise_power_factor": require_private_float("TYMPSSMD_NOISE_POWER_FACTOR"),
        "active_consistency_threshold": require_private_float(
            "TYMPSSMD_ACTIVE_CONSISTENCY_THRESHOLD"
        ),
        "reactive_consistency_threshold": require_private_float(
            "TYMPSSMD_REACTIVE_CONSISTENCY_THRESHOLD"
        ),
        "bounded_correction_fraction": require_private_float(
            "TYMPSSMD_BOUNDED_CORRECTION_FRACTION"
        ),
    }


def classify_column(file_name):
    upper_name = file_name.upper()
    if any(key in upper_name for key in CURRENT_KEYS):
        return "current"
    if any(key in upper_name for key in VOLTAGE_KEYS):
        return "voltage"
    if any(key in upper_name for key in POWER_KEYS):
        return "power"
    return "unknown"


def collect_global_stats(input_root):
    current_vals = []
    voltage_vals = []
    power_vals = []

    for feeder in os.listdir(input_root):
        feeder_path = os.path.join(input_root, feeder)
        if not os.path.isdir(feeder_path):
            continue

        for transformer in os.listdir(feeder_path):
            transformer_path = os.path.join(feeder_path, transformer)
            if not os.path.isdir(transformer_path):
                continue

            for file_name in os.listdir(transformer_path):
                if not file_name.endswith(".csv"):
                    continue

                path = os.path.join(transformer_path, file_name)
                df = pd.read_csv(path, encoding="utf-8-sig")
                vals = df[VALUE_COLS].replace("", np.nan).astype(float)
                flattened = vals.stack().dropna().values

                file_type = classify_column(file_name)
                if file_type == "current":
                    current_vals.append(flattened)
                elif file_type == "voltage":
                    voltage_vals.append(flattened)
                elif file_type == "power":
                    power_vals.append(flattened)

    def calc_std(parts):
        if not parts:
            return 1.0
        return float(np.std(np.concatenate(parts)))

    return calc_std(current_vals), calc_std(voltage_vals), calc_std(power_vals)


def apply_noise(df, file_type, sigmas, rng):
    vals = df[VALUE_COLS].astype(float)
    sigma_current, sigma_voltage, sigma_power = sigmas

    if file_type == "current":
        noise = rng.normal(0, sigma_current, vals.shape)
        new_vals = np.maximum(vals + noise, 0)
    elif file_type == "voltage":
        noise = rng.normal(0, sigma_voltage, vals.shape)
        new_vals = vals + noise
    elif file_type == "power":
        noise = rng.normal(0, sigma_power, vals.shape)
        new_vals = vals * (1 + noise)
    else:
        noise = rng.normal(0, sigma_voltage, vals.shape)
        new_vals = vals + noise

    df[VALUE_COLS] = np.round(new_vals, 4)
    return df


def find_file(data_map, key):
    key = key.upper()
    for file_name, df in data_map.items():
        base_name = os.path.splitext(file_name)[0].upper()
        if base_name == key:
            return file_name, df
    return None, None


def bounded_consistency_correction(total_df, phase_dfs, threshold, correction_fraction):
    total = total_df[VALUE_COLS].astype(float)
    phases = [df[VALUE_COLS].astype(float) for df in phase_dfs]
    phase_sum = phases[0] + phases[1] + phases[2]
    residual = total - phase_sum

    correction = residual / 3.0
    phase_scale = phase_sum.abs() / 3.0
    bound = (phase_scale * correction_fraction).replace(0, np.nan)
    correction_array = correction.to_numpy(dtype=float)
    bound_array = bound.to_numpy(dtype=float)
    clipped_array = np.where(
        np.isnan(bound_array),
        0.0,
        np.minimum(np.maximum(correction_array, -bound_array), bound_array),
    )
    clipped = pd.DataFrame(clipped_array, index=correction.index, columns=correction.columns)

    needs_correction = residual.abs() > threshold
    for phase_df in phase_dfs:
        values = phase_df[VALUE_COLS].astype(float)
        values = values.where(~needs_correction, values + clipped)
        phase_df[VALUE_COLS] = np.round(values, 4)


def apply_physical_consistency_checks(data_map, config):
    _, active_total = find_file(data_map, ACTIVE_TOTAL_KEY)
    active_phase_dfs = [find_file(data_map, key)[1] for key in ACTIVE_PHASE_KEYS]

    if active_total is not None and all(df is not None for df in active_phase_dfs):
        bounded_consistency_correction(
            active_total,
            active_phase_dfs,
            config["active_consistency_threshold"],
            config["bounded_correction_fraction"],
        )

    _, reactive_total = find_file(data_map, REACTIVE_TOTAL_KEY)
    reactive_phase_dfs = [find_file(data_map, key)[1] for key in REACTIVE_PHASE_KEYS]

    if reactive_total is not None and all(df is not None for df in reactive_phase_dfs):
        bounded_consistency_correction(
            reactive_total,
            reactive_phase_dfs,
            config["reactive_consistency_threshold"],
            config["bounded_correction_fraction"],
        )


def process_transformer_folder(input_path, output_path, sigmas, config, rng):
    os.makedirs(output_path, exist_ok=True)
    data_map = {}

    for file_name in os.listdir(input_path):
        if not file_name.endswith(".csv"):
            continue
        file_type = classify_column(file_name)
        df = pd.read_csv(os.path.join(input_path, file_name), encoding="utf-8-sig")
        data_map[file_name] = apply_noise(df, file_type, sigmas, rng)

    apply_physical_consistency_checks(data_map, config)

    for file_name, df in data_map.items():
        df.to_csv(
            os.path.join(output_path, file_name),
            index=False,
            encoding="utf-8-sig",
        )


def main():
    config = load_private_config()
    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    std_current, std_voltage, std_power = collect_global_stats(INPUT_ROOT)
    sigmas = (
        std_current * config["noise_current_factor"],
        std_voltage * config["noise_voltage_factor"],
        std_power * config["noise_power_factor"],
    )

    print("Global statistics collected and private noise scales initialized.")
    print("Exact private coefficients and thresholds are not printed by this script.")

    rng = np.random.default_rng()

    for feeder in os.listdir(INPUT_ROOT):
        feeder_path = os.path.join(INPUT_ROOT, feeder)
        if not os.path.isdir(feeder_path):
            continue

        for transformer in os.listdir(feeder_path):
            transformer_path = os.path.join(feeder_path, transformer)
            if not os.path.isdir(transformer_path):
                continue

            out_dir = os.path.join(OUTPUT_ROOT, feeder, transformer)
            process_transformer_folder(transformer_path, out_dir, sigmas, config, rng)

    print("All files have been processed.")


if __name__ == "__main__":
    main()
