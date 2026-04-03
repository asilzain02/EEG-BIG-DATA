# EEG-BIG-DATA

Complete implementation assets for:

**Scalable Big Data Analytics for Epileptic Seizure Detection using Hadoop**

## Included files

- `IMPLEMENTATION_GUIDE.md` — full step-by-step setup, MapReduce pipeline, commands, architecture, viva Q&A.
- `src/main/java/org/eeg/EEGFeatureExtraction.java` — Hadoop MapReduce Java job (Mapper + Reducer + Driver).
- `scripts/edf_to_csv.py` — converts `.edf` EEG recordings to Hadoop-ready CSV (`timestamp,channel,value`).
- `scripts/visualize_eeg_features.py` — optional Python visualization for Hadoop output.
