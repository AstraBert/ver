#!/usr/bin/env bash
set -euo pipefail

REPO="AstraBert/ver"
BIN="ver"

INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${VERSION:-latest}"      # e.g. VERSION=v0.1.0 to pin

err() { echo "error: $*" >&2; exit 1; }
need() { command -v "$1" > /dev/null 2>&1 || err "'$1' is required but not installed"; }

need curl
need tar

# Detect target triple (must match the matrix in binaries.yml)
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)  os_part="unknown-linux-gnu" ;;
  Darwin) os_part="apple-darwin" ;;
  *) err "unsupported OS: $os (Windows users: download the .zip from the releases page)" ;;
esac
case "$arch" in
  x86_64|amd64)  arch_part="x86_64" ;;
  arm64|aarch64) arch_part="aarch64" ;;
  *) err "unsupported architecture: $arch" ;;
esac
target="${arch_part}-${os_part}"

# Resolve the version. Following the /releases/latest redirect avoids the API
# rate limit. Note: it ignores prereleases, so pin VERSION for those.
if [[ "$VERSION" == "latest" ]]; then
  url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest")"
  VERSION="${url##*/}"
  [[ "$VERSION" == v* ]] || err "could not determine latest version (got '$VERSION')"
fi

stage="${BIN}-${VERSION}-${target}"
asset="${stage}.tar.gz"
base="https://github.com/${REPO}/releases/download/${VERSION}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

echo "Downloading ${BIN} ${VERSION} for ${target}..."
curl -fsSL -O "${base}/${asset}" || err "download failed: ${base}/${asset}"
curl -fsSL -O "${base}/${asset}.sha256" || err "checksum download failed"

# Verify checksum
if command -v sha256sum > /dev/null; then
  sha256sum -c "${asset}.sha256" > /dev/null || err "checksum verification failed"
elif command -v shasum > /dev/null; then
  shasum -a 256 -c "${asset}.sha256" > /dev/null || err "checksum verification failed"
else
  err "need sha256sum or shasum to verify the download"
fi

tar xzf "$asset"

mkdir -p "$INSTALL_DIR"
install -m 755 "${stage}/${BIN}" "${INSTALL_DIR}/${BIN}"

echo "Installed ${BIN} ${VERSION} to ${INSTALL_DIR}/${BIN}"
case ":$PATH:" in
  *":${INSTALL_DIR}:"*) ;;
  *) echo "Note: ${INSTALL_DIR} is not on your PATH. Add this to your shell profile:"
     echo "  export PATH=\"${INSTALL_DIR}:\$PATH\"" ;;
esac
