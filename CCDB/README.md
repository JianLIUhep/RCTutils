# CCDB Objects Based on RCT Flags

This repository contains utilities to create and manage CCDB objects based on RCT flags. It includes functionality to process CSV files with RCT flags, encode them into bitmasks, upload them to CCDB, and retrieve the stored objects.

---
## Flag-to-Bit Mapping

The flag-to-bit mapping is defined in the `process_and_upload.C` file. It assigns specific bits in a 32-bit word for each detector, representing its data quality status. Each detector can have one or more flags, including both general and specific ones. The `"Bad"` flag is used as a general indicator for issue, but it may be triggered by different underlying reasons depending on the detector.

### Map Definition Location

The mapping is implemented in the `detailedBitMapping` variable, declared as follows in `process_and_upload.C`:

```cpp
std::map<std::string, std::map<std::string, int>> detailedBitMapping = {
    {"CPV", { {"Bad", 0}, {"Invalid", 0} }},
    {"EMC", { {"Bad", 1}, {"NoDetectorData", 1}, {"BadEMCalorimetry", 1}, {"LimitedAcceptanceMCReproducible", 2} }},
    {"FDD", { {"Bad", 3}, {"Invalid", 3}, {"NoDetectorData", 3} }},
    {"FT0", { {"Bad", 4}, {"UnknownQuality", 4}, {"Unknown", 4} }},
    {"FV0", { {"Bad", 5} }},
    {"HMP", { {"Bad", 6}, {"NoDetectorData", 6} }},
    {"ITS", { {"Bad", 7}, {"UnknownQuality", 7}, {"BadTracking", 7}, {"LimitedAcceptanceMCReproducible", 8} }},
    {"MCH", { {"Bad", 9}, {"BadTracking", 9}, {"NoDetectorData", 9}, {"Unknown", 9}, {"LimitedAcceptanceMCReproducible", 10} }},
    {"MFT", { {"Bad", 11}, {"BadTracking", 11}, {"LimitedAcceptanceMCReproducible", 12} }},
    {"MID", { {"Bad", 13}, {"BadTracking", 13}, {"LimitedAcceptanceMCReproducible", 14} }},
    {"PHS", { {"Bad", 15}, {"Invalid", 15} }},
    {"TOF", { {"Bad", 16}, {"NoDetectorData", 16}, {"BadPID", 16}, {"LimitedAcceptanceMCReproducible", 17} }},
    {"TPC", { {"Bad", 18}, {"BadTracking", 18}, {"BadPID", 19}, {"LimitedAcceptanceMCNotReproducible", 18}, {"LimitedAcceptanceMCReproducible", 20} }},
    {"TRD", { {"Bad", 21}, {"BadTracking", 21} }},
    {"ZDC", { {"Bad", 22}, {"UnknownQuality", 22}, {"Unknown", 22}, {"NoDetectorData", 22} }}
};
```
### Explanation of the Map

#### **GOOD:**
- The corresponding bit in the encoded-word is not set (remains 0).
#### **"Bad" (General Issue):**
- `"Bad"` is the default flag for general issues and may have different reasons based on the detector.
- For example, in `"EMC"`, it can be due to `"NoDetectorData"` or `"BadEMCalorimetry"`, both of which trigger **bit 1**.

#### **Specific Flags:**
- Certain conditions have unique flags and bits. For example:
  - `"LimitedAcceptanceMCReproducible"` in `"EMC"` triggers **bit 2**.
  - `"BadPID"` in `"TPC"` triggers **bit 19**, separate from the general `"Bad"`.

#### **Detector-Specific Behavior:**
- Each detector may have unique issue modes. For example:
  - `"ITS"` treats `"BadTracking"` and `"UnknownQuality"` as reasons for `"Bad"` (**bit 7**).
  - `"TPC"` handles `"BadTracking"`, `"BadPID"`, and `"LimitedAcceptanceMCNotReproducible"` with distinct bits (**18**, **19**, **20**).

### Explanation of the Encoded Flag Format

The encoded flag format represents the data quality status of multiple detectors using a 32-bit integer. Each bit in the bitmask corresponds to a specific detector or condition.

#### Format:
- **Bit Position**: Each bit in the 32-bit integer maps to a specific detector or condition.
- **Bit Status**:
  - `1`: Indicates a "Bad" status or a specific issue for the corresponding detector.
  - `0`: Indicates a "Good" status with no issues detected for the corresponding detector.
- **Time Range**: Each bitmask is valid for a specific time range within the run.
  - **Start Timestamp (`from`)**: Indicates when this bitmask becomes valid.
  - **End Timestamp (`to`)**: Implicitly defined as the start timestamp of the next entry in the vector or the end of the run for the last entry.

#### Example:
For a given time range:
- **Start Timestamp (`from`)**: `1724039848000`.
- **Bitmask**: `0b00000000000000001000000000000010`.

This bitmask indicates:
- **EMC** (bit 1): Bad quality.
- **PHS** (bit 15): Bad quality.
- All other detectors are in "Good" status (`0`).

If there is another entry:
- **Next Timestamp (`from`)**: `1724040959000`.
- The time range for the first entry is implicitly `1724039848000` to `1724040959000`.


### CCDB Object Format

The encoded flags are stored in the CCDB as a vector of timestamped bitmasks, providing a detailed and chronological record of detector quality.

#### Structure:
The CCDB object is a `std::map<uint64_t, uint32_t>`, where:
- **`uint64_t`**: Represents the start timestamp (`from`) of a time range.
- **`uint32_t`**: Represents the 32-bit encoded flag for that time range.

#### Time Range Interpretation:
- Each pair only stores the **start timestamp (`from`)**.
- The **end timestamp (`to`)** is implicitly defined as:
  - The next timestamp in the vector for all but the last entry.
  - The end of the run for the final entry.

#### Example:
For a specific run:
```cpp
std::map<uint64_t, uint32_t> encodedFlags = {
    {1724039848000, 0b00000000000000001000000000000010}, // Time range: 1724039848000 to 1724040959000
    {1724040959000, 0b00000000000000000000000000000000}, // Time range: 1724040959000 to end of run
};
```
---
## Contents

- **process_and_upload.C**: 
  - Processes RCT flag data from a CSV file.
  - Encodes the flags into bitmasks and uploads them to the CCDB.

- **read_encoded_flags.C**: 
  - Reads and decodes the encoded flags from CCDB for a given run number and metadata.

- **dict_ccdb.so, dict_ccdb_rdict.pcm**: 
  - ROOT dictionary files for CCDB interactions, required to handle RCT CCDB objects.

- **README.md**: 
  - Documentation for the repository.

---

## Instructions

### Prerequisites

- Ensure you have access to the ALICE CCDB infrastructure (`http://alice-ccdb.cern.ch`).
- The `dict_ccdb.so and dict_ccdb_rdict.pcm` files must be in the working directory to enable CCDB object handling.
- Create the dictionary file  1455  rootcling -f dict_ccdb.cxx -c dict_ccdb.h
- O2/ROOT framework should be installed and configured.
- If the pre-created dictionary files do not work, the dictionary files can be created with your ROOT: 
```
rootcling -f dict_ccdb.cxx -c dict_ccdb.h
g++ -shared -fPIC dict_ccdb.cxx -o dict_ccdb.so $(root-config --cflags --libs)
```
### 1. Encoding and Uploading RCT Flags to CCDB

#### Input:
- A CSV file containing RCT flags for various detectors. The file can be created following [this instruction](https://github.com/JianLIUhep/RCTutils/tree/main?tab=readme-ov-file#export-runs-from-rct-in-bookkeeping).

#### Command:
Treat "Not available" as Bad:
```
root -b -q 'process_and_upload.C("path/to/input.csv", "passName", "periodName", "versionNumber",  "YourCCDBPath")'
```
Treat "Not available" as Good:
```
root -b -q 'process_and_upload.C("path/to/input.csv", "passName", "periodName", "versionNumber",  "YourCCDBPath", true)'
```

### 2. Reading Encoded Flags from CCDB

#### Input:
- Metadata: Run number, pass name and period name
#### Command: 
``` 
root -b -q 'read_encoded_flags.C(runNumber, "passName", "periodName", versionNumber, "YourCCDBPath")'
```
#### Example: 
``` 
root -b -q 'read_encoded_flags.C(555651, "apass1_skimmed", "LHC24am", 0,  "Users/j/jian/RCT")'
```
### 3. Error Handling
- Unexpected Flags:
detected unexpected flags are treated as "Bad" and logged in a file named unexpected_flags_YYYYMMDD_HHMMSS.log.



