# DataPort-code

`1Data extraction.py` extracts transformer level daily time series from raw feeder files, standardizes each day into 96 sampling positions, and replaces feeder and transformer identifiers with desensitized folder codes. Raw input paths and raw source column names are supplied through private environment variables and are not stored in the public code.

`2Data Preprocessing.py` performs missing value normalization, daily missing ratio filtering, shape preserving intraday interpolation, boundary filling,
adjacent day historical profile imputation for longer contiguous gaps, and inter day interpolation. A small number of residual missing values can remain after preprocessing and should be retained as explicit missing values.

`3Data Filtering.py` aligns timestamps across the 14 electrical quantities for each transformer, removes transformers with insufficient retained days, detects outliers using the interquartile range method, marks outliers as missing values, and repairs them through the same local imputation workflow.

`4Data desensitization.py` applies differentiated perturbation to current, voltage, and power variables based on global statistical features. After perturbation, it performs consistency checks between recorded total quantities and the corresponding three phase sums. When a residual exceeds the private acceptance threshold, bounded consistency correction is applied before export.

`5Data Name Change.py` replaces technical measurement codes in file names with readable electrical quantity names.

***The specific .py files are stored in the master folder.***
