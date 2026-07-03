import json
import os
import warnings
from itertools import count, product

import pandas as pd

warnings.filterwarnings("ignore")


OUTPUT_ROOT = r"1Data extraction"

INTERNAL_METRIC_ORDER = [
    "AXDL", "BXDL", "CXDL",
    "AXDY", "BXDY", "CXDY",
    "YGGL", "AXYGGL", "BXYGGL", "CXYGGL",
    "WGGL", "AXWGGL", "BXWGGL", "CXWGGL",
]

BYQ_ALPHABETS = [item[0] for item in product("ABCDEFGHIJKLMNOPQRSTUVWXYZ", repeat=1)]


def require_private_text(env_name):
    value = os.getenv(env_name)
    if not value:
        raise RuntimeError(
            f"Private configuration {env_name} is required. "
            "Set it outside the public repository before running this script."
        )
    return value


def load_private_config():
    metric_map = json.loads(require_private_text("TYMPSSMD_RAW_METRIC_COLUMN_MAP"))
    missing_keys = [key for key in INTERNAL_METRIC_ORDER if key not in metric_map]
    if missing_keys:
        raise RuntimeError(
            "The private raw metric mapping is incomplete for internal keys: "
            + ", ".join(missing_keys)
        )

    return {
        "input_folder": require_private_text("TYMPSSMD_RAW_INPUT_DIR"),
        "time_column": require_private_text("TYMPSSMD_RAW_TIME_COLUMN"),
        "transformer_column": require_private_text("TYMPSSMD_RAW_TRANSFORMER_COLUMN"),
        "metric_map": metric_map,
    }


def parse_timestamp(time_value):
    try:
        time_str = str(time_value).strip()
        if not time_str or time_str.lower() == "nan":
            return pd.NaT
        normalized = time_str.replace("-", "/")
        parts = normalized.split(" ")
        if len(parts) != 2:
            return pd.NaT
        date_part, time_part = parts
        try:
            year, month, day = date_part.split("/")
            time_dt_str = f"{year}-{month.zfill(2)}-{day.zfill(2)} {time_part}"
        except ValueError:
            time_dt_str = time_str.replace("/", "-")
        return pd.to_datetime(
            time_dt_str,
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
        )
    except Exception:
        return pd.NaT


def get_daily_96_points_template(date_value):
    start = pd.to_datetime(f"{date_value} 00:00:00")
    end = pd.to_datetime(f"{date_value} 23:45:00")
    full_day_series = pd.date_range(start=start, end=end, freq="15min")
    return pd.DataFrame(full_day_series, columns=["timestamp_full"])


def get_transformer_code(counter_value):
    if counter_value >= len(BYQ_ALPHABETS):
        return f"Trans_{counter_value - len(BYQ_ALPHABETS) + 1}"
    return BYQ_ALPHABETS[counter_value]


def post_process_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig", header=0)
        if not df.empty and df.columns[0] == "transformer_code":
            df = df.iloc[:, 1:]
            df.to_csv(file_path, mode="w", index=False, header=True, encoding="utf-8-sig")
            return True
    except Exception as exc:
        print(f"Post-processing failed for {file_path}: {exc}")
    return False


def process_raw_file(file_path, feeder_code, config, generated_csvs):
    raw_time_col = config["time_column"]
    raw_transformer_col = config["transformer_column"]
    metric_map = config["metric_map"]

    try:
        df = pd.read_csv(
            file_path,
            encoding="utf-8",
            dtype=str,
            on_bad_lines="skip",
            low_memory=False,
        )
    except Exception as exc:
        print(f"Read operation failed for one raw feeder file: {exc}")
        return

    required_raw_cols = [raw_transformer_col, raw_time_col] + [
        metric_map[key] for key in INTERNAL_METRIC_ORDER
    ]
    missing_cols = [col for col in required_raw_cols if col not in df.columns]
    if missing_cols:
        print("Skip one raw feeder file because required private columns are missing.")
        return

    df["parsed_timestamp"] = df[raw_time_col].apply(parse_timestamp)
    df = df[df["parsed_timestamp"].notna()].copy()
    if df.empty:
        print("Skip one raw feeder file because all timestamps failed to parse.")
        return

    df[raw_transformer_col] = df[raw_transformer_col].astype(str).str.strip()
    df = df[df[raw_transformer_col] != ""]
    if df.empty:
        print("Skip one raw feeder file because transformer identifiers are empty.")
        return

    hash_cols = ["parsed_timestamp"] + [metric_map[key] for key in INTERNAL_METRIC_ORDER]
    df = df.drop_duplicates(subset=hash_cols, keep="first")
    df["date_str"] = df["parsed_timestamp"].dt.strftime("%Y-%m-%d")

    feeder_output_folder = os.path.join(OUTPUT_ROOT, feeder_code)
    os.makedirs(feeder_output_folder, exist_ok=True)

    transformer_code_counter = 0
    transformer_map = {}

    for raw_transformer_id, group in df.groupby(raw_transformer_col):
        if raw_transformer_id not in transformer_map:
            transformer_map[raw_transformer_id] = get_transformer_code(transformer_code_counter)
            transformer_code_counter += 1

        transformer_code = transformer_map[raw_transformer_id]
        transformer_folder = os.path.join(feeder_output_folder, transformer_code)
        os.makedirs(transformer_folder, exist_ok=True)

        for date_value, day_group in group.groupby(group["parsed_timestamp"].dt.date):
            date_str = day_group["date_str"].iloc[0]
            template_df = get_daily_96_points_template(date_str)

            for internal_metric in INTERNAL_METRIC_ORDER:
                raw_metric_col = metric_map[internal_metric]
                metric_df = day_group[["parsed_timestamp", raw_metric_col]].rename(
                    columns={raw_metric_col: "metric_value"}
                )
                metric_df["metric_value"] = pd.to_numeric(
                    metric_df["metric_value"],
                    errors="coerce",
                )
                merged_df = template_df.merge(
                    metric_df,
                    left_on="timestamp_full",
                    right_on="parsed_timestamp",
                    how="left",
                ).drop(columns=["parsed_timestamp"])

                values = merged_df["metric_value"].fillna("").tolist()
                if len(values) != 96:
                    continue

                csv_file = os.path.join(transformer_folder, f"{internal_metric}.csv")
                generated_csvs.append(csv_file)
                write_header = not os.path.exists(csv_file)
                row = [transformer_code, date_str] + values
                header_names = ["transformer_code", "Date"] + [
                    f"Value_{idx + 1}" for idx in range(96)
                ]
                row_df = pd.DataFrame([row], columns=header_names)
                row_df.to_csv(
                    csv_file,
                    mode="a",
                    index=False,
                    header=write_header,
                    encoding="utf-8-sig",
                )


def main():
    config = load_private_config()
    input_folder = config["input_folder"]
    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    generated_csvs = []
    feeder_counter = count(1)

    for filename in os.listdir(input_folder):
        if not filename.lower().endswith(".csv"):
            continue
        feeder_code = str(next(feeder_counter))
        process_raw_file(
            os.path.join(input_folder, filename),
            feeder_code,
            config,
            generated_csvs,
        )

    for csv_file in set(generated_csvs):
        post_process_csv(csv_file)

    print(f"Data extraction complete. Output directory: {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
