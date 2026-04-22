#!/usr/bin/env bash
set -euo pipefail

YEAR="${1:-2022}"
PERIOD="${2:-LHC22f}"
PASS="${3:-apass3}"
OUTDIR="${4:-./qc_downloads}"
FILENAME="${5:-QC_fullrun.root}"
MODE="${6:-skip}"   # allowed: skip, overwrite

BASE="/alice/data/${YEAR}/${PERIOD}"

mkdir -p "${OUTDIR}"

SUCCESS_LOG="${OUTDIR}/download_success.log"
MISSING_LOG="${OUTDIR}/missing_files.log"
ERROR_LOG="${OUTDIR}/download_errors.log"
MULTIPLE_LOG="${OUTDIR}/multiple_matches.log"
RUNLIST_LOG="${OUTDIR}/run_list.log"

: > "${SUCCESS_LOG}"
: > "${MISSING_LOG}"
: > "${ERROR_LOG}"
: > "${MULTIPLE_LOG}"
: > "${RUNLIST_LOG}"

if [[ "${MODE}" != "skip" && "${MODE}" != "overwrite" ]]; then
    echo "ERROR: invalid mode '${MODE}'"
    echo "Allowed values: skip, overwrite"
    exit 1
fi

echo "Starting QC download: year=${YEAR}, period=${PERIOD}, pass=${PASS}, mode=${MODE}"

command -v alien_find >/dev/null 2>&1 || { echo "ERROR: alien_find not found"; exit 1; }
command -v alien_ls   >/dev/null 2>&1 || { echo "ERROR: alien_ls not found"; exit 1; }
command -v alien_cp   >/dev/null 2>&1 || { echo "ERROR: alien_cp not found"; exit 1; }

if ! alien_ls "${BASE}" >/dev/null 2>&1; then
    echo "ERROR: base path not accessible: ${BASE}"
    exit 1
fi

# Build run list directly from immediate children of the period directory
alien_ls "${BASE}" 2>> "${ERROR_LOG}" \
    | grep -E '^[0-9]+/?$' \
    | sed 's:/$::' \
    | sort -u > "${RUNLIST_LOG}" || true

NRUNS=$(wc -l < "${RUNLIST_LOG}" || echo 0)

if [[ "${NRUNS}" -eq 0 ]]; then
    echo "ERROR: no runs found under ${BASE}"
    exit 1
fi

echo "Found ${NRUNS} runs under ${BASE}"

n_ok=0
n_missing=0
n_multiple=0
n_failed=0
n_skip=0
n_overwrite=0
n_no_pass=0

while IFS= read -r run; do
    run_pass_dir="${BASE}/${run}/${PASS}"
    local_dir="${OUTDIR}/${PERIOD}/${run}/${PASS}"
    local_file="${local_dir}/${FILENAME}"

    mkdir -p "${local_dir}"

    if ! alien_ls "${run_pass_dir}" >/dev/null 2>&1; then
        echo "${run} MISSING_PASS_DIR ${run_pass_dir}" >> "${MISSING_LOG}"
        ((n_no_pass+=1))
        echo "[${run}] missing pass dir"
        continue
    fi

    TMP_MATCHES=$(mktemp)
    alien_find "${run_pass_dir}" "${FILENAME}" > "${TMP_MATCHES}" 2>> "${ERROR_LOG}" || true
    NMATCH=$(wc -l < "${TMP_MATCHES}" || echo 0)

    if [[ "${NMATCH}" -eq 0 ]]; then
        echo "${run} MISSING_FILE ${run_pass_dir}" >> "${MISSING_LOG}"
        rm -f "${TMP_MATCHES}"
        ((n_missing+=1))
        echo "[${run}] missing file"
        continue
    fi

    if [[ "${NMATCH}" -gt 1 ]]; then
        {
            echo "${run} MULTIPLE_MATCHES ${run_pass_dir}"
            cat "${TMP_MATCHES}"
        } >> "${MULTIPLE_LOG}"
        rm -f "${TMP_MATCHES}"
        ((n_multiple+=1))
        echo "[${run}] multiple matches"
        continue
    fi

    alien_file=$(head -n 1 "${TMP_MATCHES}")
    rm -f "${TMP_MATCHES}"

    if [[ -f "${local_file}" ]]; then
        if [[ "${MODE}" == "skip" ]]; then
            echo "${run} SKIP ${local_file}" >> "${SUCCESS_LOG}"
            ((n_skip+=1))
            echo "[${run}] already exists, skipped"
            continue
        else
            rm -f "${local_file}"
            ((n_overwrite+=1))
            echo "[${run}] overwriting existing file"
        fi
    fi

    if alien_cp "alien://${alien_file}" "file://${local_file}" >> "${SUCCESS_LOG}" 2>> "${ERROR_LOG}"; then
        echo "${run} OK ${alien_file}" >> "${SUCCESS_LOG}"
        ((n_ok+=1))
        echo "[${run}] downloaded"
    else
        echo "${run} DOWNLOAD_FAILED ${alien_file}" >> "${ERROR_LOG}"
        rm -f "${local_file}"
        ((n_failed+=1))
        echo "[${run}] download failed"
    fi

done < "${RUNLIST_LOG}"

echo "Done"
echo "  total runs under period : ${NRUNS}"
echo "  downloaded              : ${n_ok}"
echo "  skipped                 : ${n_skip}"
echo "  overwritten targets     : ${n_overwrite}"
echo "  missing pass dir        : ${n_no_pass}"
echo "  missing file            : ${n_missing}"
echo "  multiple matches        : ${n_multiple}"
echo "  failed                  : ${n_failed}"
echo "Logs:"
echo "  ${RUNLIST_LOG}"
echo "  ${SUCCESS_LOG}"
echo "  ${MISSING_LOG}"
echo "  ${MULTIPLE_LOG}"
echo "  ${ERROR_LOG}"
