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

.PHONY: all init download-binaries download-ui-assets generate-assets build clean help

# Default target
all: generate-assets build

# Initialize project (first time setup)
init: download-binaries download-ui-assets
	@echo "Installing Go dependencies..."
	go mod download
	@echo "Project initialized successfully!"

# Download binary dependencies (supervisord, protoc)
download-binaries:
	@echo "Downloading binary dependencies..."
	@./scripts/download_binaries.sh || (echo "Failed to download binaries" && exit 1)

# Download UI assets (jQuery, Bootstrap, Glyphicons)
download-ui-assets:
	@if [ ! -f "ui/ui/lib/.downloaded" ]; then \
		echo "Downloading UI assets..."; \
		./scripts/download_ui_assets.sh || (echo "Failed to download UI assets" && exit 1); \
	else \
		echo "UI assets already exist, skipping download."; \
	fi

# Generate assets (vfsdata.go for web UI)
generate-assets: download-ui-assets
	@echo "Generating assets..."
	@cd asset && go generate || (echo "Failed to generate assets" && exit 1)
	@echo "Assets generated successfully!"

# Build vermeer binary
build:
	@echo "Building vermeer..."
	@go build -o vermeer
	@echo "Build completed: ./vermeer"

# Build for specific platform
build-linux-amd64:
	@echo "Building for linux/amd64..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o vermeer

build-linux-arm64:
	@echo "Building for linux/arm64..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o vermeer

# Clean generated files and binaries
clean:
	@echo "Cleaning..."
	@rm -f vermeer
	@rm -f asset/assets_vfsdata.go
	@echo "Clean completed!"

# Clean including downloaded binaries
clean-all: clean
	@echo "Cleaning downloaded binaries..."
	@rm -rf tools/supervisord/*/supervisord
	@rm -rf tools/protoc/*/protoc
	@rm -rf tools/protoc/*/bin
	@rm -rf tools/protoc/*/include
	@echo "Cleaning downloaded UI assets..."
	@rm -rf ui/node_modules ui/package-lock.json
	@rm -f ui/ui/lib/.downloaded ui/ui/lib/jquery-license
	@rm -rf ui/ui/lib/bootstrap-*-dist ui/ui/lib/bootstrap4-glyphicons
	@rm -f ui/ui/lib/jquery-*.min.js
	@echo "All clean completed!"

# Help
help:
	@echo "Vermeer Build System"
	@echo ""
	@echo "Usage:"
	@echo "  make init              - First time setup (download binaries + UI assets + go mod download)"
	@echo "  make download-binaries - Download supervisord and protoc binaries for your platform"
	@echo "  make download-ui-assets - Download jQuery, Bootstrap, Glyphicons to ui/ui/lib/"
	@echo "  make generate-assets   - Generate assets_vfsdata.go from web UI (required before build)"
	@echo "  make build             - Build vermeer for current platform (default: local architecture)"
	@echo "  make build-linux-amd64 - Build for Linux AMD64 (for deployment)"
	@echo "  make build-linux-arm64 - Build for Linux ARM64 (for ARM servers)"
	@echo "  make clean             - Remove generated files and binaries (keep downloaded tools)"
	@echo "  make clean-all         - Remove everything including downloaded tools and UI assets"
	@echo "  make all               - Generate assets and build (default target)"
	@echo "  make help              - Show this help message"
