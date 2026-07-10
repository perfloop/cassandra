# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

# 1. Patch build.xml
content = open('build.xml').read()

old_str = """    <!-- Non-java resources needed by the test suite -->
    <copy todir="${test.classes}">
      <fileset dir="${test.resources}"/>
    </copy>
  </target>"""

new_str = old_str + """

  <target name="microbench-build" depends="_main-jar,resolver-dist-lib" description="Compile only what is needed for microbench">
    <mkdir dir="${test.classes}"/>
    <javac
     compiler="modern"
     debug="true"
     debuglevel="${debuglevel}"
     destdir="${test.classes}"
     includeantruntime="true"
     source="${ant.java.version}"
     target="${ant.java.version}"
     encoding="utf-8">
     <classpath>
        <path refid="cassandra.classpath.test"/>
     </classpath>
     <compilerarg value="-XDignore.symbol.file"/>
     <compilerarg line="${jdk11plus-javac-exports}"/>
     <src path="${test.anttasks.src}"/>
     <src path="${test.unit.src}"/>
     <src path="${test.microbench.src}"/>
     <src path="${test.distributed.src}"/>
     <src path="${test.harry.src}"/>
     <exclude name="**/*Test.java"/>
     <exclude name="**/*TestBase.java"/>
     <exclude name="**/distributed/test/**/*.java"/>
     <exclude name="**/CassandraBriefJUnitResultFormatter.java"/>
     <exclude name="**/CassandraXMLJUnitResultFormatter.java"/>
     <exclude name="**/JStackJUnitTask.java"/>
     <exclude name="**/InstanceConfig.java"/>
     <exclude name="**/IsolatedJmx.java"/>
     <exclude name="**/CompactStoragePagingWithProtocolTester.java"/>
     <exclude name="**/ConfigCompatibilityTestGenerate.java"/>
     <exclude name="**/MultiNodeTableWalkBase.java"/>
     <exclude name="**/CasMultiNodeTableWalkBase.java"/>
     <exclude name="**/AccordInteropMultiNodeTableWalkBase.java"/>
     <exclude name="**/AccordInteropMultiNodeTokenConflictBase.java"/>
     <exclude name="**/SimulatedOperation.java"/>
     <exclude name="**/CASCommonTestCases.java"/>
     <exclude name="**/BaseAssassinatedCase.java"/>
    </javac>
    <javac
     compiler="modern"
     debug="true"
     debuglevel="${debuglevel}"
     destdir="${test.classes}"
     includeantruntime="true"
     source="${ant.java.version}"
     target="${ant.java.version}"
     encoding="utf-8">
     <classpath>
        <path refid="cassandra.classpath.test"/>
        <pathelement location="${test.classes}"/>
     </classpath>
     <compilerarg value="-XDignore.symbol.file"/>
     <compilerarg line="${jdk11plus-javac-exports}"/>
     <src path="${test.classes}"/>
     <include name="**/jmh_generated/*.java"/>
    </javac>
    <!-- Non-java resources needed by the test suite -->
    <copy todir="${test.classes}">
      <fileset dir="${test.resources}"/>
    </copy>
  </target>"""

if "microbench-build" not in content and old_str in content:
    open('build.xml', 'w').write(content.replace(old_str, new_str, 1))
    print("build.xml successfully patched")
else:
    print("build.xml already patched or marker not found")
