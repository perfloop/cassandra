#!/usr/bin/env python3
import json
import os
import subprocess
import sys

def patch_build_gradle(path):
    if not os.path.exists(path):
        print(f"File to patch not found: {path}", file=sys.stderr)
        return
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.splitlines()
    new_lines = []
    in_rat = False
    brace_count = 0
    for line in lines:
        if 'id("org.nosphere.apache.rat")' in line:
            continue
        if line.strip().startswith("rat {"):
            in_rat = True
            brace_count = 1
            continue
        if in_rat:
            brace_count += line.count("{")
            brace_count -= line.count("}")
            if brace_count == 0:
                in_rat = False
            continue
        new_lines.append(line)
            
    with open(path, 'w', encoding='utf-8') as f:
        f.write("\n".join(new_lines))

def patch_conventions(path):
    if not os.path.exists(path):
        print(f"File to patch not found: {path}", file=sys.stderr)
        return
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    content = content.replace("dependsOn(':rat')", "// dependsOn(':rat')")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--build":
        print("Dynamically patching accord build to remove rat plugin...", file=sys.stderr)
        patch_build_gradle("modules/accord/build.gradle")
        patch_conventions("modules/accord/buildSrc/src/main/groovy/accord.java-conventions.gradle")
        
        # Build and publish accord local jar
        gradle_cmd = [
            "./modules/accord/gradlew", "-p", "modules/accord",
            "publishToMavenLocal",
            "-Paccord_group=org.apache.cassandra",
            "-Paccord_artifactId=cassandra-accord",
            "-Paccord_version=7.0-SNAPSHOT"
        ]
        print(f"Running gradle build: {' '.join(gradle_cmd)}", file=sys.stderr)
        gradle_res = subprocess.run(gradle_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if gradle_res.returncode != 0:
            print("Gradle build failed!", file=sys.stderr)
            print(gradle_res.stdout, file=sys.stderr)
            print(gradle_res.stderr, file=sys.stderr)
            sys.exit(gradle_res.returncode)
            
        print("Gradle build succeeded.", file=sys.stderr)
        sys.exit(0)

    # Run the ant microbench command directly since everything is already built
    cmd = [
        "ant", "microbench",
        "-lib", "build/test/lib/jars/ant-junit-1.10.12.jar",
        "-Dbenchmark.name=NamesQueryMultiSSTablesBench",
        "-Dno-build-accord=true",
        "-Djmh.args=-wi 0 -i 1 -f 1"
    ]
    
    print(f"Running command: {' '.join(cmd)}", file=sys.stderr)
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Print output of command to stderr for debugging/visibility
    print(result.stdout, file=sys.stderr)
    print(result.stderr, file=sys.stderr)
    
    if result.returncode != 0:
        print("Command failed!", file=sys.stderr)
        sys.exit(result.returncode)
        
    # Read the output json file
    json_path = "build/test/jmh-result.json"
    if not os.path.exists(json_path):
        print(f"Result file {json_path} not found!", file=sys.stderr)
        sys.exit(1)
        
    with open(json_path, 'r') as f:
        data = json.load(f)
        
    # Find the entry for numSSTables=20 and numRows=100
    score = None
    for entry in data:
        params = entry.get("params", {})
        if params.get("numSSTables") == "20" and params.get("numRows") == "100":
            score = entry["primaryMetric"]["score"]
            break
            
    if score is None:
        score = data[0]["primaryMetric"]["score"]
    
    # Print the perfloop required JSON format to stdout
    out_obj = {"metric": "ns/op", "value": float(score)}
    print(json.dumps(out_obj))

if __name__ == "__main__":
    main()
