## Producing Runlists  
- `./mk-runlist.sh LHC23zzo_apass3 LHC23zzo apass3`
## Download merged QC root files from ML
### Usage
`./download.sh <year> <period> <pass> [outdir] [filename] [mode]`
- year: data year, for example 2026
- period: period name, for example LHC26ac
- pass: pass name, for example cpass0
- outdir: local output directory, default: ./qc_downloads
- filename: target file name to search, default: QC_fullrun.root
- mode: handling of existing local files
 - skip (default): keep existing files and skip download
- overwrite: remove existing files and re-download
- Example `./download.sh 2026 LHC26ac apass1 ./qc_downloads QC_fullrun.root skip`
