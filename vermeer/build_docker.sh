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
#!/usr/bin/env bash
VERSION=$(awk -F '"' '/^const Version[[:space:]]/ {print $2}' ./apps/version/version.go)


USERNAME=$1 # 用户邮箱前缀
PASSWORD=$2 # 镜像仓库控制台个人中心设置的密码，不是UUAP密码
if [ -z ${USERNAME} ]; then
    echo "Enter Your Name: "
    read USERNAME
fi
if [ -z ${PASSWORD} ]; then
    echo "Enter Password: "
    read -s PASSWORD
fi

NS=hugegraph-vermeer # 空间名称
IMAGE=vermeer #仓库名称
TAG=${VERSION} # 镜像的tag
