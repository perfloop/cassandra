#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ ! -f build.xml ]]; then
    echo "run from the Cassandra repository root" >&2
    exit 2
fi

export HOME=/workspace/deps/perfloop-cassandra-home
export GRADLE_USER_HOME=/workspace/deps/perfloop-cassandra-gradle
export JAVA_TOOL_OPTIONS=-Duser.home=/workspace/deps/perfloop-cassandra-home

fetch_jacoco_agent()
{
    local repository=https://repo1.maven.org/maven2
    local destination="$HOME/.m2/repository/org/jacoco/org.jacoco.agent/0.8.8/org.jacoco.agent-0.8.8.jar"
    local directory artifact checksum

    directory=$(dirname "$destination")
    mkdir -p "$directory" "$GRADLE_USER_HOME"
    artifact=$(mktemp "$directory/.perfloop-artifact.XXXXXX")
    checksum=$(mktemp "$directory/.perfloop-checksum.XXXXXX")

    curl --fail --location --retry 3 --silent --show-error --output "$artifact" "$repository/org/jacoco/org.jacoco.agent/0.8.8/org.jacoco.agent-0.8.8.jar"
    curl --fail --location --retry 3 --silent --show-error --output "$checksum" "$repository/org/jacoco/org.jacoco.agent/0.8.8/org.jacoco.agent-0.8.8.jar.sha1"
    awk -v artifact="$artifact" '{ print $1 "  " artifact }' "$checksum" | sha1sum -c -
    mv "$artifact" "$destination"
    chmod 644 "$destination"
    rm -f "$checksum"
}

write_test_build()
{
    mkdir -p build
    cat > build/perfloop-test-build.xml <<'EOF'
<project name="perfloop-test-build" basedir=".." default="perfloop-build-test">
  <import file="../build.xml"/>
  <target name="perfloop-build-test" depends="_main-jar,stress-build-test,fqltool-build-test,sstableloader-build-test,simulator-jars">
    <antcall target="_build-test" inheritRefs="true"/>
    <antcall target="_check-test-names" inheritRefs="true"/>
  </target>
</project>
EOF
}

compile_tests()
{
    fetch_jacoco_agent
    git submodule update --init --recursive --depth 1 --jobs 8
    write_test_build
    ant -f build/perfloop-test-build.xml perfloop-build-test
}

build_jmh_launcher()
{
    local benchmark_bin=$1

    mkdir -p "$(dirname "$benchmark_bin")"
    python3 - "$PWD" <<'PY'
import os
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
entries = [root / "build" / "test" / "classes", root / "test" / "conf"]
entries.extend(sorted((root / "build").glob("*.jar")))
for directory in (root / "build" / "lib", root / "build" / "test" / "lib" / "jars"):
    entries.extend(sorted(directory.rglob("*.jar")))
seen = set()
entries = [entry.resolve() for entry in entries if entry.exists() and not (str(entry.resolve()) in seen or seen.add(str(entry.resolve())))]
if not entries or not (root / "build" / "test" / "classes").is_dir():
    raise SystemExit("JMH build did not produce the Cassandra test classes")
(root / ".perfloop-jmh-classpath").write_text(os.pathsep.join(map(str, entries)) + "\n")

def as_url(entry):
    value = entry.as_uri()
    return value + "/" if entry.is_dir() else value

manifest = root / "build" / "perfloop-jmh-manifest.mf"
with manifest.open("w", encoding="utf-8", newline="\n") as output:
    output.write("Manifest-Version: 1.0\n")
    output.write("Main-Class: PerfloopJmhLauncher\n")
    line = "Class-Path: "
    for entry in entries:
        value = as_url(entry)
        if len((line + value + " ").encode("utf-8")) > 68:
            output.write(line + "\n")
            line = " " + value + " "
        else:
            line += value + " "
    output.write(line + "\n\n")

launcher = root / "build" / "perfloop-jmh-launcher"
classes = launcher / "classes"
classes.mkdir(parents=True, exist_ok=True)
(launcher / "PerfloopJmhLauncher.java").write_text('''import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class PerfloopJmhLauncher
{
    public static void main(String[] args) throws Exception
    {
        String classpath = Files.readString(Paths.get(System.getProperty("user.dir"), ".perfloop-jmh-classpath")).trim();
        if (classpath.isEmpty())
            throw new IllegalStateException("empty JMH classpath");
        System.setProperty("java.class.path", classpath);
        try
        {
            Class<?> main = Class.forName("org.openjdk.jmh.Main");
            main.getMethod("main", String[].class).invoke(null, (Object) args);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Error)
                throw (Error) cause;
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            throw new RuntimeException(cause);
        }
    }
}
''', encoding="utf-8")
(root / ".perfloop-jmh-java.args").write_text('''-Xmx1G
-XX:+UseZGC
-XX:+ZGenerational
-XX:-UseCompressedOops
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-Djdk.attach.allowAttachSelf=true
-Djava.security.manager=allow
-Dnet.bytebuddy.experimental=true
-Dio.netty.tryReflectionSetAccessible=true
--add-exports=java.base/java.lang.ref=ALL-UNNAMED
--add-exports=java.base/java.lang.reflect=ALL-UNNAMED
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED
--add-exports=java.rmi/sun.rmi.server=ALL-UNNAMED
--add-exports=java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED
--add-exports=java.sql/java.sql=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-exports=jdk.attach/sun.tools.attach=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.module=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.math=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED
--add-opens=java.base/jdk.internal.math=ALL-UNNAMED
--add-opens=java.base/jdk.internal.module=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED
--add-opens=jdk.management/com.sun.management=ALL-UNNAMED
''', encoding="utf-8")
PY

    javac -d build/perfloop-jmh-launcher/classes build/perfloop-jmh-launcher/PerfloopJmhLauncher.java
    jar --create --file "$benchmark_bin" --manifest build/perfloop-jmh-manifest.mf -C build/perfloop-jmh-launcher/classes .
}

case ${1:-} in
    compile)
        compile_tests
        ;;
    build)
        : "${2:?usage: $0 build PERFLOOP_BENCH_BIN}"
        compile_tests
        build_jmh_launcher "$2"
        ;;
    *)
        echo "usage: $0 {compile|build PERFLOOP_BENCH_BIN}" >&2
        exit 2
        ;;
esac
