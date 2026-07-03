# DataPort-code

2Data Preprocessing.py does not disclose the numerical threshold used for missing-value processing. The daily missing-value threshold and long-gap handling rules are loaded from private runtime environment variables and must be configured locally before execution.

3Data Filtering.py does not disclose the numerical thresholds used for transformer filtering or anomaly detection. The minimum valid-day threshold and the IQR-based anomaly detection multiplier are loaded from private runtime environment variables. The same IQR multiplier is applied consistently to the lower and upper anomaly bounds.

4Data desensitization.py does not disclose the numerical noise intensities or physical-consistency correction thresholds. The differentiated noise intensities for current, voltage, and power variables, together with the consistency-check and bounded-correction parameters, are loaded from private runtime environment variables. These values are intentionally excluded from the public repository to prevent leakage of sensitive data-processing parameters.

***The specific .py files are stored in the master folder.***
