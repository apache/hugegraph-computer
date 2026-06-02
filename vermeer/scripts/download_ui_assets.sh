#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
UI_DIR="$PROJECT_ROOT/ui"
LIB_DIR="$PROJECT_ROOT/ui/ui/lib"

# Glyphicons source (GitHub raw) - pinned to specific commit for reproducible builds
GLYPHICONS_COMMIT="f7b1a17bbe64308d1d8b2b4bb2ba8a0ea621b377"
GLYPHICONS_BASE="https://raw.githubusercontent.com/Darkseal/bootstrap4-glyphicons/${GLYPHICONS_COMMIT}/bootstrap4-glyphicons"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Download a single file from URL to target path
download_file() {
    local url=$1
    local target=$2

    mkdir -p "$(dirname "$target")"
    if ! curl -sL -f "$url" -o "$target"; then
        log_error "Failed to download: $url"
        return 1
    fi
}

# Main function
main() {
    log_info "Downloading UI assets..."

    # Check npm and node
    if ! command -v npm &> /dev/null; then
        log_error "npm is required but not installed. Please install Node.js/npm first."
        exit 1
    fi
    if ! command -v node &> /dev/null; then
        log_error "node is required but not installed. Please install Node.js first."
        exit 1
    fi

    # Step 1: npm install (jQuery + Bootstrap)
    log_info "Installing npm dependencies (jQuery, Bootstrap)..."
    cd "$UI_DIR"
    npm install --no-audit --no-fund
    cd "$PROJECT_ROOT"

    # Read actual versions from package.json (source of truth)
    JQUERY_VERSION=$(node -p "require('$UI_DIR/package.json').dependencies.jquery")
    BOOTSTRAP_VERSION=$(node -p "require('$UI_DIR/package.json').dependencies.bootstrap")

    # Step 2: Create lib directory structure
    log_info "Copying files to ui/ui/lib/..."
    mkdir -p "$LIB_DIR"

    # Copy jQuery (npm names it jquery.min.js, rename to match version)
    cp "$UI_DIR/node_modules/jquery/dist/jquery.min.js" "$LIB_DIR/jquery-${JQUERY_VERSION}.min.js"

    # Copy Bootstrap CSS
    mkdir -p "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap.min.css" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap.min.css.map" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap-grid.min.css" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap-grid.min.css.map" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap-reboot.min.css" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"
    cp "$UI_DIR/node_modules/bootstrap/dist/css/bootstrap-reboot.min.css.map" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/css/"

    # Copy Bootstrap JS
    mkdir -p "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/js"
    cp "$UI_DIR/node_modules/bootstrap/dist/js/bootstrap.min.js" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/js/"
    cp "$UI_DIR/node_modules/bootstrap/dist/js/bootstrap.min.js.map" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/js/"
    cp "$UI_DIR/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/js/"
    cp "$UI_DIR/node_modules/bootstrap/dist/js/bootstrap.bundle.min.js.map" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/js/"

    # Copy Bootstrap LICENSE
    cp "$UI_DIR/node_modules/bootstrap/LICENSE" "$LIB_DIR/bootstrap-${BOOTSTRAP_VERSION}-dist/"

    # Create jquery-license (MIT)
    cat > "$LIB_DIR/jquery-license" << 'JQLICENSE'
Copyright JS Foundation and other contributors, https://js.foundation/

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
JQLICENSE

    # Step 3: Download Bootstrap4 Glyphicons from GitHub
    log_info "Downloading Bootstrap4 Glyphicons from GitHub..."
    download_file "$GLYPHICONS_BASE/css/bootstrap-glyphicons.min.css" \
        "$LIB_DIR/bootstrap4-glyphicons/css/bootstrap-glyphicons.min.css"

    # Download fontawesome fonts
    for font in fa-brands-400 fa-regular-400 fa-solid-900; do
        for ext in eot svg ttf woff woff2; do
            download_file "$GLYPHICONS_BASE/fonts/fontawesome/${font}.${ext}" \
                "$LIB_DIR/bootstrap4-glyphicons/fonts/fontawesome/${font}.${ext}"
        done
    done

    # Download glyphicons fonts
    for ext in eot svg ttf woff woff2; do
        download_file "$GLYPHICONS_BASE/fonts/glyphicons/glyphicons-halflings-regular.${ext}" \
            "$LIB_DIR/bootstrap4-glyphicons/fonts/glyphicons/glyphicons-halflings-regular.${ext}"
    done

    # Download maps
    download_file "$GLYPHICONS_BASE/maps/glyphicons-fontawesome.less" \
        "$LIB_DIR/bootstrap4-glyphicons/maps/glyphicons-fontawesome.less"
    download_file "$GLYPHICONS_BASE/maps/glyphicons-fontawesome.min.css" \
        "$LIB_DIR/bootstrap4-glyphicons/maps/glyphicons-fontawesome.min.css"

    # Download bootstrap4-glyphicons LICENSE for release metadata
    GLYPHICONS_LICENSE_URL="https://raw.githubusercontent.com/Darkseal/bootstrap4-glyphicons/${GLYPHICONS_COMMIT}/LICENSE"
    if ! curl -sL -f "$GLYPHICONS_LICENSE_URL" -o "$LIB_DIR/bootstrap4-glyphicons/LICENSE" 2>/dev/null; then
        log_warn "Could not download bootstrap4-glyphicons LICENSE file (repo may not have one)"
    fi

    # Stamp file for incremental builds
    touch "$LIB_DIR/.downloaded"

    log_info "All UI assets downloaded successfully!"
    log_info ""
    log_info "Downloaded to: $LIB_DIR"
    log_info "  - jQuery ${JQUERY_VERSION}"
    log_info "  - Bootstrap ${BOOTSTRAP_VERSION} (CSS + JS + source maps)"
    log_info "  - Bootstrap4 Glyphicons (CSS + fonts + maps)"
}

main "$@"
