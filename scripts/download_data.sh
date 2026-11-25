#!/usr/bin/env bash
set -euo pipefail

REQUIRED_DATASETS=(
  "clients"
  "projects"
  "survey_questions"
  "employees"
  "timesheets"
  "fx_rates"
  "invoices"
  "expenses"
  "crm_events"
  "employee_surveys"
)

URL=""
S3_BUCKET_ARG=""
S3_PREFIX_ARG="${S3_PREFIX:-landing}"
ENDPOINT_URL_ARG=""
DEST="./data/landing"
VERIFY_ONLY=false
FORCE=false

usage() {
  cat <<'EOF'
Download and verify the CorpTech Enterprise Lakehouse landing dataset.

Usage:
  download_data.sh [OPTIONS]

Options:
  --url <URL>              HTTP/HTTPS archive URL (.zip, .tar.gz, .tgz).
                           Defaults to $DATASET_DOWNLOAD_URL if set.
  --s3-bucket <BUCKET>     S3 bucket name or s3://bucket/path URI to sync from.
                           Defaults to $S3_BUCKET if set.
  --s3-prefix <PREFIX>     Key prefix inside S3 bucket (default: "landing" or $S3_PREFIX).
  --endpoint-url <URL>     Custom S3/MinIO endpoint URL.
                           Defaults to $S3_ENDPOINT_URL if set.
  --dest <DIR>             Destination directory (default: ./data/landing).
  --verify-only            Skip download and only verify checksums + 10 folders.
  --force                  Re-download even if destination already contains all 10 folders.
  -h, --help               Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      URL="${2:?Missing value for --url}"
      shift 2
      ;;
    --s3-bucket)
      S3_BUCKET_ARG="${2:?Missing value for --s3-bucket}"
      shift 2
      ;;
    --s3-prefix)
      S3_PREFIX_ARG="${2:-}"
      shift 2
      ;;
    --endpoint-url)
      ENDPOINT_URL_ARG="${2:?Missing value for --endpoint-url}"
      shift 2
      ;;
    --dest)
      DEST="${2:?Missing value for --dest}"
      shift 2
      ;;
    --verify-only)
      VERIFY_ONLY=true
      shift
      ;;
    --force)
      FORCE=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Fall back to environment variables only when neither CLI source flag was passed.
if [[ -z "$URL" && -z "$S3_BUCKET_ARG" ]]; then
  URL="${DATASET_DOWNLOAD_URL:-}"
  S3_BUCKET_ARG="${S3_BUCKET:-}"
fi

if [[ -z "$ENDPOINT_URL_ARG" ]]; then
  ENDPOINT_URL_ARG="${S3_ENDPOINT_URL:-}"
fi

all_folders_populated() {
  local target_dir="$1"
  for ds in "${REQUIRED_DATASETS[@]}"; do
    local ds_dir="${target_dir}/${ds}"
    if [[ ! -d "$ds_dir" ]]; then
      return 1
    fi
    if [[ -z "$(find "$ds_dir" -type f -print -quit 2>/dev/null)" ]]; then
      return 1
    fi
  done
  return 0
}

verify_manifest_if_present() {
  local target_dir="$1"
  local parent_dir
  parent_dir="$(dirname "$target_dir")"
  local manifest=""
  local manifest_dir=""

  if [[ -f "${target_dir}/MANIFEST.sha256" ]]; then
    manifest="${target_dir}/MANIFEST.sha256"
    manifest_dir="$target_dir"
  elif [[ -f "${parent_dir}/MANIFEST.sha256" ]]; then
    manifest="${parent_dir}/MANIFEST.sha256"
    manifest_dir="$parent_dir"
  else
    echo "INFO: No MANIFEST.sha256 found; skipping checksum verification."
    return 0
  fi

  echo "Verifying SHA-256 checksums using ${manifest}..."
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$manifest_dir" && sha256sum -c "$(basename "$manifest")")
  elif command -v shasum >/dev/null 2>&1; then
    (cd "$manifest_dir" && shasum -a 256 -c "$(basename "$manifest")")
  else
    echo "ERROR: Neither sha256sum nor shasum is installed to verify ${manifest}." >&2
    return 1
  fi
}

verify_folders() {
  local target_dir="$1"
  local missing=0
  echo ""
  echo "Validating landing zone folders in ${target_dir}:"
  for ds in "${REQUIRED_DATASETS[@]}"; do
    local ds_dir="${target_dir}/${ds}"
    if [[ ! -d "$ds_dir" ]]; then
      echo "  [MISSING] ${ds}/"
      missing=1
    elif [[ -z "$(find "$ds_dir" -type f -print -quit 2>/dev/null)" ]]; then
      echo "  [EMPTY]   ${ds}/"
      missing=1
    else
      local count
      count="$(find "$ds_dir" -type f | wc -l | tr -d ' ')"
      echo "  [OK]      ${ds}/ (${count} files)"
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    echo ""
    echo "ERROR: Landing zone validation failed. Expected all 10 non-empty dataset folders." >&2
    return 1
  fi
  echo ""
  echo "SUCCESS: All 10 required datasets are present and verified."
}

extract_http_archive() {
  local archive_path="$1"
  local extract_dir="$2"
  local clean_url="${URL%%\?*}"
  clean_url="${clean_url%%#*}"

  case "$clean_url" in
    *.tar.gz|*.tgz)
      tar -xzf "$archive_path" -C "$extract_dir"
      ;;
    *.zip)
      if command -v unzip >/dev/null 2>&1; then
        unzip -oq "$archive_path" -d "$extract_dir"
      else
        python3 -m zipfile -e "$archive_path" "$extract_dir"
      fi
      ;;
    *)
      if command -v unzip >/dev/null 2>&1 && unzip -tq "$archive_path" >/dev/null 2>&1; then
        unzip -oq "$archive_path" -d "$extract_dir"
      else
        tar -xzf "$archive_path" -C "$extract_dir"
      fi
      ;;
  esac
}

if [[ "$VERIFY_ONLY" == false ]]; then
  if [[ "$FORCE" == false ]] && all_folders_populated "$DEST"; then
    echo "INFO: All 10 dataset folders already exist in ${DEST}. Use --force to re-download."
  else
    if [[ -n "$URL" && -n "$S3_BUCKET_ARG" ]]; then
      echo "ERROR: Specify either --url or --s3-bucket, not both." >&2
      exit 1
    fi
    if [[ -z "$URL" && -z "$S3_BUCKET_ARG" ]]; then
      echo "ERROR: No download source provided." >&2
      echo "Provide --url <DATASET_DOWNLOAD_URL> or --s3-bucket <BUCKET>, or pass --verify-only." >&2
      exit 1
    fi

    mkdir -p "$DEST"

    if [[ -n "$URL" ]]; then
      if ! command -v curl >/dev/null 2>&1; then
        echo "ERROR: curl is required to download HTTP archives." >&2
        exit 1
      fi
      TMP_DIR="$(mktemp -d)"
      trap 'rm -rf "$TMP_DIR"' EXIT
      ARCHIVE_FILE="${TMP_DIR}/landing_archive"
      EXTRACT_DIR="${TMP_DIR}/extracted"
      mkdir -p "$EXTRACT_DIR"

      echo "Downloading dataset archive from ${URL}..."
      curl -fL "$URL" -o "$ARCHIVE_FILE"

      echo "Extracting archive..."
      extract_http_archive "$ARCHIVE_FILE" "$EXTRACT_DIR"

      # Handle archives wrapped in landing/ or data/landing/.
      SOURCE_ROOT="$EXTRACT_DIR"
      if [[ -d "${EXTRACT_DIR}/landing/clients" ]]; then
        SOURCE_ROOT="${EXTRACT_DIR}/landing"
      elif [[ -d "${EXTRACT_DIR}/data/landing/clients" ]]; then
        SOURCE_ROOT="${EXTRACT_DIR}/data/landing"
      fi

      cp -R "${SOURCE_ROOT}/." "$DEST/"
      if [[ "$SOURCE_ROOT" != "$EXTRACT_DIR" && -f "${EXTRACT_DIR}/MANIFEST.sha256" && ! -f "${DEST}/MANIFEST.sha256" ]]; then
        cp "${EXTRACT_DIR}/MANIFEST.sha256" "$(dirname "$DEST")/MANIFEST.sha256"
      fi
    else
      if ! command -v aws >/dev/null 2>&1; then
        echo "ERROR: aws CLI is required for --s3-bucket sync." >&2
        exit 1
      fi
      if [[ "$S3_BUCKET_ARG" == s3://* ]]; then
        S3_URI="${S3_BUCKET_ARG%/}"
      elif [[ -n "$S3_PREFIX_ARG" ]]; then
        S3_URI="s3://${S3_BUCKET_ARG%/}/${S3_PREFIX_ARG#/}"
      else
        S3_URI="s3://${S3_BUCKET_ARG%/}"
      fi

      AWS_ARGS=(s3 sync "$S3_URI" "$DEST")
      if [[ -n "$ENDPOINT_URL_ARG" ]]; then
        AWS_ARGS+=(--endpoint-url "$ENDPOINT_URL_ARG")
      fi
      echo "Syncing dataset from ${S3_URI} to ${DEST}..."
      aws "${AWS_ARGS[@]}"
    fi
  fi
fi

if [[ ! -d "$DEST" ]]; then
  echo "ERROR: Destination directory does not exist: ${DEST}" >&2
  exit 1
fi

verify_manifest_if_present "$DEST"
verify_folders "$DEST"
