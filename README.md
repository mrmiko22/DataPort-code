1Data extraction.py is mainly used for cleaning, desensitizing, and standardizing the format of multivariate measurement data in the electric power system, and finally saving the output in a directory hierarchy of "line code/transformer code/index name. csv".

2Data Preprocessing.py is mainly used for batch cleaning and preprocessing CSV data files, and finally outputting standardized data with 4 decimal places to a new location according to the original directory structure.

3Data Filtering. py is mainly used for batch cleaning, filtering, and reassembling hierarchical time-series data. It performs timestamp alignment and outlier processing on CSV files, and finally reassembles the qualified data after cleaning according to standardized naming rules and outputs it to a new directory.

4Data desensitization. py is used to desensitize power data, classify data (current, voltage, power) according to file names, and calculate their respective global standard deviations; Subsequently, Gaussian noise that conforms to physical characteristics is generated based on these statistical values and user preset coefficients, and overlaid onto the original data. Finally, the processed files are output to a new folder while maintaining the original directory structure.

Replace specific technical code (such as AXDL) in the file name with the corresponding electrical and physical quantity description (such as Phase A current) in 5Data Name Change. py.
