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
#! /bin/bash
export BUILD_REPO_WS=$PWD

go env -w GOPATH=$BUILD_REPO_WS/packages
go env -w GOCACHE=$BUILD_REPO_WS/cache
go env -w GO111MODULE="on"                      ## 开启 go mod 模式，必须

go env -w GONOSUMDB=\*                          ## 目前有一些代码库还不支持sumdb索引，暂时屏蔽此功能


#go env -w CC=/opt/compiler/gcc-8.2/bin/gcc
#go env -w CXX=/opt/compiler/gcc-8.2/bin/g++

go mod download

# Download binary dependencies if not exist
echo "Checking binary dependencies..."
./scripts/download_binaries.sh

# Download UI assets if not exist
echo "Checking UI assets..."
 if [ ! -f "ui/ui/lib/jquery-3.5.1.min.js" ]; then
     ./scripts/download_ui_assets.sh
 fi

# Generate assets if not exist
if [ ! -f "asset/assets_vfsdata.go" ]; then
    echo "Generating assets..."
    cd asset && go generate && cd ..
fi
ARCH=$1
CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" go build

VERSION=$(cat ./apps/version/version.go | grep 'Version' | awk -F '"' '{print $2}')
cp tools/supervisord/linux_"$ARCH"/supervisord supervisord
tar --exclude=config/afs_client.conf -zcvf vermeer-"$VERSION"-"$ARCH".tar.gz vermeer config/ supervisord vermeer.sh mem_supervisor.sh

mkdir "$BUILD_REPO_WS"/output
mv vermeer-"$VERSION"-"$ARCH".tar.gz "$BUILD_REPO_WS"/output/
