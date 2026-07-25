#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# download_dump.sh — Resumable Wikidata JSON dump downloader
#
# Downloads the weekly Wikidata entity dump (latest-all.json.gz) with
# automatic resume on interruption, checksum verification, and progress
# reporting.  Designed for the wiki_db bootstrap pipeline.
#
# Usage:
#   ./scripts/download_dump.sh [--output <PATH>] [--force] [--quiet]
#
# Options:
#   --output <PATH>   Target file path (default: ./latest-all.json.gz)
#   --force           Re-download even if a complete valid dump exists
#   --quiet           Suppress progress output
#   --help            Show this help text and exit
#
# Resume behaviour:
#   If the output file already exists, curl attempts HTTP range-request
#   resume (curl -C -).  On success, only the remaining portion is
#   transferred.  After completion, the MD5 checksum is verified against
#   the upstream checksum file.  If verification fails, the incomplete
#   file is removed and the user is prompted to retry with --force.
#
# Signal handling:
#   SIGINT / SIGTERM leave the partial file in place for future resume.
#   A second Ctrl-C during checksum or finalisation exits immediately.
# ---------------------------------------------------------------------------
set -euo pipefail

# ---------- Configuration ------------------------------------------------
DUMP_URL="https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz"
MD5_URL="${DUMP_URL}.md5"
USER_AGENT="wiki_db/0.1.0 (resumable-download-script; mailto:user@example.com)"
OUTPUT_FILE="latest-all.json.gz"
FORCE_DOWNLOAD=false
QUIET=false

# ---------- Argument parsing ---------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
	--output)
		shift
		OUTPUT_FILE="$1"
		;;
	--force)
		FORCE_DOWNLOAD=true
		;;
	--quiet)
		QUIET=true
		;;
	--help | -h)
		sed -n '3,26p' "$0" | sed 's/^# //; s/^#$//'
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		echo "Usage: $0 [--output <PATH>] [--force] [--quiet]" >&2
		exit 1
		;;
	esac
	shift
done

# ---------- Helpers ------------------------------------------------------
info() { [[ "$QUIET" == false ]] && echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*" >&2; }
error() { echo "[ERROR] $*" >&2; }

# ---------- Signal handling ----------------------------------------------
PARTIAL_CLEANUP=true
cleanup() {
	if [[ "$PARTIAL_CLEANUP" == true ]]; then
		info "Interrupted. Partial file left at '${OUTPUT_FILE}' for resume."
	fi
	exit 1
}
trap cleanup SIGINT SIGTERM

# ---------- Pre-flight checks --------------------------------------------
for cmd in curl md5sum; do
	if ! command -v "$cmd" &>/dev/null; then
		error "Required command not found: $cmd"
		exit 1
	fi
done

# ---------- Download -----------------------------------------------------
DOWNLOAD_NEEDED=true

if [[ -f "$OUTPUT_FILE" ]]; then
	if [[ "$FORCE_DOWNLOAD" == true ]]; then
		info "File exists; --force set — downloading again."
		rm -f "$OUTPUT_FILE"
	else
		info "File exists: $OUTPUT_FILE"
		info "Tentative size: $(stat --printf='%s' "$OUTPUT_FILE" 2>/dev/null | numfmt --to=iec 2>/dev/null || echo 'unknown')"
		info "Verifying current file integrity before deciding..."
		if gzip -t "$OUTPUT_FILE" 2>/dev/null; then
			info "Existing file is a valid gzip archive."
			# Check the MD5 to see if it's the complete dump
			STORED_MD5=$(curl -sS -A "$USER_AGENT" "$MD5_URL" 2>/dev/null | awk '{print $1}') || true
			if [[ -n "$STORED_MD5" ]]; then
				LOCAL_MD5=$(md5sum "$OUTPUT_FILE" | awk '{print $1}')
				if [[ "$LOCAL_MD5" == "$STORED_MD5" ]]; then
					info "MD5 checksum matches — dump is already fully downloaded."
					DOWNLOAD_NEEDED=false
				else
					warn "MD5 mismatch — file is incomplete or corrupt. Resuming..."
				fi
			else
				# Can't fetch checksum; trust gzip validity and skip
				info "Could not fetch upstream MD5 — skipping checksum verification."
				info "Assuming existing valid gzip is complete. Use --force to re-download."
				DOWNLOAD_NEEDED=false
			fi
		else
			warn "Existing file is not a valid gzip — will attempt resume."
		fi
	fi
fi

if [[ "$DOWNLOAD_NEEDED" == true ]]; then
	info "Downloading Wikidata entity dump..."
	info "  URL:    $DUMP_URL"
	info "  Output: $OUTPUT_FILE"
	[[ "$QUIET" == false ]] && info "  (This is a ~35 GB file; expect 10–30 minutes on a fast connection)"

	# Progress flag: --progress-bar in terminals, --silent if quiet
	PROGRESS_FLAG="--progress-bar"
	[[ "$QUIET" == true ]] && PROGRESS_FLAG="--silent"

	# Turn off partial-cleanup so a Ctrl-C leaves the file for resume
	PARTIAL_CLEANUP=false

	curl -C - -o "$OUTPUT_FILE" \
		-A "$USER_AGENT" \
		-L \
		--retry 3 \
		--retry-delay 5 \
		"$PROGRESS_FLAG" \
		"$DUMP_URL" || {
		# curl exit status
		EXIT_CODE=$?
		if [[ $EXIT_CODE -eq 18 ]]; then
			# exit 18 = partial transfer — normal for Ctrl-C or transient error
			info "Partial transfer (curl exit $EXIT_CODE). File left for resume."
		else
			warn "Download failed (curl exit $EXIT_CODE). File left for resume."
		fi
		exit $EXIT_CODE
	}

	PARTIAL_CLEANUP=true
	info "Download complete."
fi

# ---------- Checksum verification ----------------------------------------
info "Fetching MD5 checksum..."
STORED_MD5=$(curl -sS -A "$USER_AGENT" "$MD5_URL" 2>/dev/null | awk '{print $1}') || {
	warn "Could not fetch MD5 checksum from upstream — skipping verification."
	warn "You can manually verify: md5sum '$OUTPUT_FILE'"
	echo ""
	info "Dump ready: $OUTPUT_FILE"
	exit 0
}

info "Verifying MD5 checksum..."
LOCAL_MD5=$(md5sum "$OUTPUT_FILE" | awk '{print $1}')

if [[ "$LOCAL_MD5" != "$STORED_MD5" ]]; then
	error "MD5 MISMATCH!"
	error "  Expected: $STORED_MD5"
	error "  Got:      $LOCAL_MD5"
	error "The file is corrupt. Remove it and re-run with --force:"
	error "  rm '$OUTPUT_FILE' && $0 --force"
	exit 1
fi

info "MD5 checksum OK (${LOCAL_MD5})."

# ---------- Final integrity check ----------------------------------------
info "Running final gzip integrity test..."
if gzip -t "$OUTPUT_FILE" 2>/dev/null; then
	info "gzip integrity check passed."
else
	error "gzip integrity test FAILED — file is corrupt after checksum match."
	error "This is unusual. Remove and re-download:"
	error "  rm '$OUTPUT_FILE' && $0 --force"
	exit 1
fi

# ---------- Summary ------------------------------------------------------
FINAL_SIZE=$(stat --printf='%s' "$OUTPUT_FILE" 2>/dev/null | numfmt --to=iec 2>/dev/null || echo "unknown")
echo ""
info "═══════════════════════════════════════════════════════════════"
info "  Dump downloaded successfully."
info "  File:  $OUTPUT_FILE"
info "  Size:  $FINAL_SIZE"
info "  MD5:   $LOCAL_MD5"
info "═══════════════════════════════════════════════════════════════"
echo ""
info "Next step:  cargo run --release -- bootstrap --dump '$OUTPUT_FILE'"
