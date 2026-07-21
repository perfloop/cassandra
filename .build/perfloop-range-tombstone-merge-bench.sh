#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Fresh proof workers do not initialize Cassandra's Accord submodule for us.
git submodule update --init --recursive --depth 1 --jobs 8
test -x modules/accord/gradlew

# test/bin/jmh uses the compiled test classes and the Cassandra jar.
ant build-test

: "${PERFLOOP_BENCH_BIN:?PERFLOOP_BENCH_BIN must name the replay launcher}"
cat >"${PERFLOOP_BENCH_BIN}" <<'LAUNCHER'
#!/usr/bin/env bash
set -euo pipefail

# Proof commands run from the repository root, so this remains valid after the
# controller reconstructs either revision in a fresh worktree.
exec "$PWD/test/bin/jmh" "$@"
LAUNCHER
chmod +x "${PERFLOOP_BENCH_BIN}"
