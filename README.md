# DataPort-code

This repository provides the public data processing workflow used to construct the
TY-MPSSMD dataset. The scripts describe the processing logic, directory
organization, missing value handling, filtering, desensitization, and file name
standardization steps.

Private thresholds and perturbation coefficients are not stored in this
repository. They must be supplied through a private execution environment before
running the scripts. Do not commit private configuration files, raw data,
intermediate data, or generated dataset folders to this repository.

## Scripts

`1Data extraction.py` extracts transformer level daily time series from raw
feeder files, standardizes each day into 96 sampling positions, and replaces
feeder and transformer identifiers with desensitized folder codes. Raw input
paths and raw source column names are supplied through private environment
variables and are not stored in the public code.

`2Data Preprocessing.py` performs missing value normalization, daily missing
ratio filtering, shape preserving intraday interpolation, boundary filling,
adjacent day historical profile imputation for longer contiguous gaps, and
inter day interpolation. A small number of residual missing values can remain
after preprocessing and should be retained as explicit missing values.

`3Data Filtering.py` aligns timestamps across the 14 electrical quantities for
each transformer, removes transformers with insufficient retained days, detects
outliers using the interquartile range method, marks outliers as missing values,
and repairs them through the same local imputation workflow.

`4Data desensitization.py` applies differentiated perturbation to current,
voltage, and power variables based on global statistical features. After
perturbation, it performs consistency checks between recorded total quantities
and the corresponding three phase sums. When a residual exceeds the private
acceptance threshold, bounded consistency correction is applied before export.

`5Data Name Change.py` replaces technical measurement codes in file names with
readable electrical quantity names.

## Private Runtime Configuration

The following environment variables are required by the public scripts. Their
values are intentionally not included in this repository.

- `TYMPSSMD_DAILY_MISSING_RATIO_THRESHOLD`
- `TYMPSSMD_LONG_GAP_MIN_POINTS`
- `TYMPSSMD_ZERO_AS_MISSING`
- `TYMPSSMD_RAW_INPUT_DIR`
- `TYMPSSMD_RAW_TIME_COLUMN`
- `TYMPSSMD_RAW_TRANSFORMER_COLUMN`
- `TYMPSSMD_RAW_METRIC_COLUMN_MAP`
- `TYMPSSMD_MIN_VALID_DAYS`
- `TYMPSSMD_IQR_MULTIPLIER`
- `TYMPSSMD_NOISE_CURRENT_FACTOR`
- `TYMPSSMD_NOISE_VOLTAGE_FACTOR`
- `TYMPSSMD_NOISE_POWER_FACTOR`
- `TYMPSSMD_ACTIVE_CONSISTENCY_THRESHOLD`
- `TYMPSSMD_REACTIVE_CONSISTENCY_THRESHOLD`
- `TYMPSSMD_BOUNDED_CORRECTION_FRACTION`

These variables control private filtering thresholds, zero value treatment,
outlier detection settings, perturbation strengths, and physical consistency
acceptance limits. They should be stored outside the repository, for example in
a private environment configuration or a secure execution system.
