#!/usr/bin/env python3
"""Automatic upstream IoT Benchmark data/parameter capture + SchemaLens Stages 1-8.

This script deliberately does NOT run any Cassandra benchmark. It retrieves a pinned
upstream source, builds only its core module, launches the upstream generateDataMode,
compiles and invokes the existing SchemaLens Java query-object sampler, and uses an
isolated copy of the V3 pipeline for the pre-benchmark selection.

Limitations: samples query-object parameters, NOT executed SQL or scheduler ordering.
Every command is logged. Existing experimental directories are never deleted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone

PROJECT = Path(__file__).resolve().parents[1]
REPO_URL = "https://github.com/thulab/iot-benchmark.git"
PINNED_SHA = "62e0dc100e56443978c8b4c66472b1d8a7ef0238"
EXPORTER = PROJECT / "java_bridge/cn/edu/tsinghua/iot/benchmark/tools/SchemaLensQueryCapture.java"
PROFILE = PROJECT / "phase_a1/inputs/iot_benchmark_pilot.properties"
APP_CLASS = "cn.edu.tsinghua.iot.benchmark.App"
CAPTURE_CLASS = "cn.edu.tsinghua.iot.benchmark.tools.SchemaLensQueryCapture"


class SetupError(RuntimeError):
    pass


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Generate real IoT Benchmark CSVs and query-object samples, then freeze SchemaLens Stage 8 (no Cassandra).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--workdir", type=Path, default=PROJECT / "native_auto_work",
                   help="Workspace for upstream clone and isolated experiment runs")
    p.add_argument("--devices", type=int, default=3, help="Number of IoT Benchmark devices (>=2)")
    p.add_argument("--loops", type=int, default=60, help="Native generator number of loops (>=1)")
    p.add_argument("--batch-size", type=int, default=5, help="Observations per write batch (>=1)")
    p.add_argument("--samples-per-class", type=int, default=1,
                   help="Export Java query parameter objects per class (pipeline uses first per class)")
    p.add_argument("--max-cells", type=int, default=500000,
                   help="Maximum imported logical measurement cells in Python bridge")
    p.add_argument("--check", action="store_true", help="Show prerequisites; do not clone or generate anything")
    p.add_argument("--dry-run", action="store_true", help="Preview the steps; no changes or network calls")
    p.add_argument("--repo-dir", type=Path,
                   help="Optional pre-cloned upstream checkout at the pinned commit; never modified")
    p.add_argument("--skip-java-build", action="store_true",
                   help="Reuse previously built upstream core classes + cached dependency classpath")
    return p.parse_args(argv)


def inspect_prereqs():
    available = {name: shutil.which(name) for name in ("python3", "git", "java", "javac", "mvn")}
    missing = [k for k, v in available.items() if v is None]
    for name, cmd in available.items():
        print(f"  {name:<7}: {cmd or 'MISSING'}")
    if not missing:
        version = subprocess.run(["java", "-version"], text=True, capture_output=True)
        m = re.search(r'\bversion\s+"(\d+)', version.stderr + version.stdout)
        if not m:
            m = re.search(r'openjdk\s+(\d+)', version.stderr + version.stdout, re.I)
        if not m:
            print("  Java version: could not determine (attempting build anyway)")
        elif int(m.group(1)) < 17:
            missing.append("Java 17+")
    if missing:
        print("\nOn Ubuntu, install missing dependencies using:")
        print("  sudo apt update && sudo apt install -y git openjdk-17-jdk maven python3")
    return missing


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def command(args: list[str], log_file: Path, *, cwd: Path | None = None):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    display_args = [str(x) for x in args]
    if "-cp" in display_args:
        cp_position = display_args.index("-cp") + 1
        if cp_position < len(display_args):
            display_args[cp_position] = "<Java classpath recorded in log>"
    print("  $ " + " ".join(display_args), flush=True)
    with log_file.open("a", encoding="utf-8") as fp:
        fp.write("\nCOMMAND: " + repr(args) + "\n")
        fp.flush()
        with subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                              errors="replace", bufsize=1) as proc:
            assert proc.stdout is not None
            for line in proc.stdout:
                fp.write(line)
                # Print concise progress; logs keep the full output.
                if any(tag in line for tag in ("BUILD FAILURE", "BUILD SUCCESS", "Exported ",
                                                "A1 completed", "Native Java-object", "ERROR")):
                    print("    " + line.strip(), flush=True)
            code = proc.wait()
        if code:
            raise SetupError(f"Command exited {code}: {' '.join(map(str,args))}\nFull log: {log_file}")


def log_tail(path: Path, *, lines: int = 100) -> str:
    """Return the last log lines without hiding native Java errors behind a zero exit code."""
    if not path.is_file():
        return f"Log file not found: {path}"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:]) or "(empty log)"


def native_java_command(classpath: str, config_dir: Path) -> list[str]:
    """Pass the native configuration and logging settings at JVM startup.

    Config.initInnerFunction() reads function.xml from benchmark-conf and calls
    System.exit(0) when it is missing. The sibling XML must exist before this
    function is invoked. Log4j must be configured to expose early native errors.
    """
    config_dir = config_dir.resolve()
    return ["java", "-Dfile.encoding=UTF-8",
            f"-Dbenchmark-conf={config_dir}",
            f"-Dlog4j.configuration={(config_dir / 'log4j.properties').as_uri()}",
            "-cp", classpath, APP_CLASS, "-cf", str(config_dir)]


def capture_java_command(classpath: str, config_dir: Path, output: Path, repetitions: int) -> list[str]:
    """Run native query-object capture against the SAME config as data generation."""
    config_dir = config_dir.resolve()
    return ["java", "-Dfile.encoding=UTF-8",
            f"-Dbenchmark-conf={config_dir}",
            f"-Dlog4j.configuration={(config_dir / 'log4j.properties').as_uri()}",
            "-cp", classpath, CAPTURE_CLASS, str(output), str(repetitions)]


def copy_upstream_support_files(upstream_conf: Path, config_dir: Path):
    """Copy native files mandatory for IoT Benchmark's Config.initialize().

    In the pinned upstream Config.initInnerFunction(), function.xml is opened
    relative to benchmark-conf. A missing file is logged and System.exit(0) is
    called, so process return code ALONE cannot indicate a successful run.
    """
    config_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for name in ("function.xml", "log4j.properties", "logback.xml"):
        src = upstream_conf / name
        if not src.is_file():
            if name in ("function.xml", "log4j.properties"):
                raise SetupError(f"Upstream support file missing: {src}")
            continue
        shutil.copy2(src, config_dir / name)
        copied.append(name)
    return copied


def run_capture(args: list[str], log_file: Path, cwd: Path | None = None):
    command(args, log_file, cwd=cwd)


def build_config(profile: Path, output: Path, dataset: Path, *, devices: int, loops: int, batch_size: int):
    """Create an isolated native configuration from the project V3 profile.

    We deliberately preserve the Python activation scenario's TWO DOUBLE sensors,
    one query device, two selected query sensors, and query operation parameters.
    The only scale changes here are devices, loops and batch size.
    """
    if not profile.is_file():
        raise SetupError(f"V3 property profile missing: {profile}")
    if devices < 2 or loops < 1 or batch_size < 1:
        raise SetupError("Require --devices >= 2, --loops >= 1, --batch-size >= 1")
    profile_lines = profile.read_text(encoding="utf-8").splitlines()
    overrides = {
        "DB_SWITCH": "IoTDB-200-SESSION_BY_TABLET",
        "IoTDB_DIALECT_MODE": "table",
        "BENCHMARK_WORK_MODE": "generateDataMode",
        "FILE_PATH": str(dataset.resolve()),
        "DEVICE_NUMBER": str(devices),
        "SENSOR_NUMBER": "2",
        "SCHEMA_CLIENT_NUMBER": "1",
        "DATA_CLIENT_NUMBER": "1",
        "GROUP_NUMBER": "1",
        "IoTDB_TABLE_NUMBER": "1",
        "LOOP": str(loops),
        "BATCH_SIZE_PER_WRITE": str(batch_size),
        "BIG_BATCH_SIZE": "100",
        "QUERY_DEVICE_NUM": "1",
        "QUERY_SENSOR_NUM": "2",
        "QUERY_SET_OP_NUM": "2",
        "QUERY_SET_OP_TYPE": "union",
        "IS_SENSOR_TS_ALIGNMENT": "true",
        "IS_REGULAR_FREQUENCY": "true",
        "IS_OUT_OF_ORDER": "false",
        "NULL_RATIO": "0",
        "TIMESTAMP_PRECISION": "ms",
        "INSERT_DATATYPE_PROPORTION": "0:0:0:0:1:0:0:0:0:0:0",
        "OPERATION_PROPORTION": "1:0:0:0:0:0:0:0:0:0:0:0:0",
    }
    original_keys = set()
    lines = []
    for line in profile_lines:
        mt = re.match(r'^(\s*)([A-Za-z_][A-Za-z_0-9]*)\s*=', line)
        if mt:
            key = mt.group(2)
            if key in overrides:
                lines.append(f"{key}={overrides[key]}")
                original_keys.add(key)
                continue
        lines.append(line)
    lines.append("\n# Added by SchemaLens V3 automatic generator")
    for key, value in overrides.items():
        if key not in original_keys:
            lines.append(f"{key}={value}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def check_git_checkout(repo_dir: Path, log: Path):
    if not (repo_dir / ".git").exists():
        raise SetupError(f"Not a git checkout: {repo_dir}")
    out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_dir,
                         text=True, capture_output=True, check=True).stdout.strip()
    if out != PINNED_SHA:
        raise SetupError(f"Upstream at {out}; required {PINNED_SHA}.\n"
                         "Use a separate checkout at the pinned revision; this script never resets repositories.")
    print("  Upstream pinned commit verified: " + out)
    if not (repo_dir / "core/pom.xml").is_file():
        raise SetupError(f"Upstream checkout lacks core/pom.xml: {repo_dir}")


def isolate_pipeline(destination: Path):
    def ignore(parent: str, names: list[str]):
        excluded = {"__pycache__", ".venv", "native_auto_work", ".git", ".pytest_cache"}
        # Never copy pilot-generated outputs into a new experiment.
        if Path(parent).resolve() == PROJECT.resolve():
            excluded.update({"outputs"})
        if Path(parent).resolve() == (PROJECT / "phase_a1").resolve():
            excluded.add("outputs")
        return {name for name in names if name in excluded or name.endswith(".pyc")}
    shutil.copytree(PROJECT, destination, ignore=ignore, dirs_exist_ok=False)


def find_latest_output(run_root: Path):
    p = run_root / "schemalens/outputs/stage8"
    summary = p / "30_hbench_summary.csv"
    manifest = p / "32_frozen_manifest.json"
    for f in (summary, manifest):
        if not f.is_file() or not f.stat().st_size:
            raise SetupError(f"Expected frozen Stage 8 output not found: {f}")
    return summary, manifest


def main(argv=None):
    args = parse_args(argv)
    print("\nSchemaLens / IoT Benchmark V3 — fully automated PRE-BENCHMARK")
    print("Pinned Java source: " + PINNED_SHA)
    print("Target: Cassandra | Stages 1–8 only | NO Stage 9 measurements\n")
    if args.samples_per_class < 1 or args.samples_per_class > 10000:
        raise SetupError("--samples-per-class must be 1..10000")
    if args.max_cells < 1:
        raise SetupError("--max-cells must be positive")
    if args.devices < 2 or args.loops < 1 or args.batch_size < 1:
        raise SetupError("--devices >=2, --loops >=1, --batch-size >=1 are required")
    total_cells = args.devices * 2 * args.loops * args.batch_size
    if total_cells > args.max_cells:
        raise SetupError(f"Requested workload has ~{total_cells} cells > --max-cells {args.max_cells}. "
                         "Increase --max-cells or reduce --devices/--loops/--batch-size.")
    if args.check or args.dry_run:
        print("Dependencies:")
        missing = inspect_prereqs()
        print("\nSteps: clone pinned upstream (unless --repo-dir), compile core, generate native CSVs, "
              "compile+run Java sampler, run SchemaLens Stages 1–8, freeze HBench.")
        print("Logical cells planned:", total_cells)
        print("Working directory:", args.workdir.expanduser().resolve())
        print("No database will be contacted. No files have been written.")
        return 1 if (args.check and missing) else 0
    missing = inspect_prereqs()
    if missing:
        raise SetupError("Missing prerequisites: " + ", ".join(missing))
    workdir = args.workdir.expanduser().resolve()
    # Protect against mistakenly placing the workdir under the data sources of an existing run.
    if workdir == PROJECT or workdir == PROJECT.parent:
        raise SetupError("--workdir must be a dedicated workspace, not the project directory")
    repo_dir = args.repo_dir.expanduser().resolve() if args.repo_dir else workdir / "upstream_iot_benchmark"
    if args.repo_dir and not repo_dir.is_dir():
        raise SetupError("--repo-dir does not exist: " + str(repo_dir))
    workdir.mkdir(parents=True, exist_ok=True)
    cache_dir = workdir / "build_cache"
    cache_dir.mkdir(exist_ok=True)
    if not repo_dir.is_dir():
        print("\n[1/7] Cloning IoT Benchmark from GitHub...")
        command(["git", "clone", "--quiet", REPO_URL, str(repo_dir)], cache_dir / "clone.log")
        command(["git", "checkout", "--quiet", PINNED_SHA], cache_dir / "checkout.log", cwd=repo_dir)
    else:
        print("\n[1/7] Reusing verified IoT Benchmark checkout")
    check_git_checkout(repo_dir, cache_dir / "check.log")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_root = workdir / "runs" / f"v3_{stamp}_pid{os.getpid()}"
    if run_root.exists():
        raise SetupError(f"Run directory already exists: {run_root}")
    run_root.mkdir(parents=True)
    logs = run_root / "logs"
    logs.mkdir()
    config_dir = run_root / "java_config"
    dataset = run_root / "native_data"
    config = build_config(PROFILE, config_dir / "config.properties", dataset,
                          devices=args.devices, loops=args.loops, batch_size=args.batch_size)
    support_files = copy_upstream_support_files(repo_dir / "configuration/conf", config_dir)
    print("  Copied official Java support files: " + ", ".join(support_files), flush=True)
    log_records = {}
    def step(k, heading, fn):
        print("\n" + heading, flush=True)
        try:
            fn()
            log_records[k] = "OK"
        except Exception:
            log_records[k] = "FAILED"
            (run_root / "run_status.json").write_text(json.dumps(log_records, indent=2)+"\n")
            raise
    cpfile = cache_dir / "core_classpath.txt"
    def build():
        # Core-only build avoids optional IoTDB-2.0 snapshot artifacts and does not need an IoTDB server.
        command(["mvn", "-B", "-q", "-pl", "core", "-am", "package",
                 "-Dmaven.test.skip=true", "-Dspotless.skip=true"], logs / "build_core.log", cwd=repo_dir)
        command(["mvn", "-B", "-q", "-f", "core/pom.xml", "dependency:build-classpath",
                 f"-Dmdep.outputFile={cpfile}"], logs / "dependencies.log", cwd=repo_dir)
    if args.skip_java_build:
        if not (repo_dir / "core/target/classes").is_dir() or not cpfile.is_file():
            raise SetupError("Cannot skip build: core/target/classes or cached classpath missing")
        print("\n[2/7] Reusing compiled upstream core and classpath")
    else:
        step("build", "[2/7] Compiling upstream core (first Maven run downloads dependencies)...", build)
    classpath = os.pathsep.join([str(repo_dir / "core/target/classes"), cpfile.read_text(encoding="utf-8").strip()])
    if not cpfile.read_text(encoding="utf-8").strip():
        raise SetupError("Maven returned an empty dependency classpath")

    def generate():
        # The native writer creates the data directory: do not pre-create or reuse it.
        generator_log = logs / "native_generation.log"
        print(f"  Java configuration: {config.resolve()}", flush=True)
        print(f"  Expected data directory: {dataset.resolve()}", flush=True)
        try:
            command(native_java_command(classpath, config_dir), generator_log, cwd=repo_dir)
        except SetupError:
            print("\n--- native_generation.log: last 100 lines ---\n" +
                  log_tail(generator_log) + "\n--- end of Java log ---", flush=True)
            raise
        missing = [name for name in ("schema.txt", "info.txt") if not (dataset / name).is_file()]
        batches = sorted(dataset.glob("*/BigBatch_*.csv")) if dataset.is_dir() else []
        if missing or not batches:
            fallback_paths = [repo_dir / "data/test/schema.txt",
                              repo_dir / "data/schemalens_iot_pilot/schema.txt"]
            detected = [str(p) for p in fallback_paths if p.is_file()]
            print("\n--- native_generation.log: last 100 lines ---\n" +
                  log_tail(generator_log) + "\n--- end of Java log ---", flush=True)
            diagnostic = (f"Native Java generator completed without expected outputs. "
                          f"Missing: {missing or 'none'}, CSV batches: {len(batches)}. "
                          f"Configured FILE_PATH: {dataset}. "
                          + (f"Outputs found under upstream default path(s): {detected}. " if detected else "")
                          + f"Full log: {generator_log}")
            raise SetupError(diagnostic)
        print(f"  Generated {len(batches)} native CSV files in {dataset}")
    step("generate", "[3/7] Running original Java generateDataMode (NO database)...", generate)

    capture = run_root / "native_queries.jsonl"
    javaclasses = run_root / "capture_classes"
    javaclasses.mkdir()
    def generate_params():
        command(["javac", "-encoding", "UTF-8", "-cp", classpath, "-d", str(javaclasses),
                 str(EXPORTER)], logs / "compile_exporter.log", cwd=repo_dir)
        cp = os.pathsep.join((str(javaclasses), classpath))
        command(capture_java_command(cp, config_dir, capture, args.samples_per_class),
                logs / "capture.log", cwd=repo_dir)
        if not capture.is_file() or not capture.stat().st_size:
            raise SetupError("Java query capture was not generated")
    step("capture", "[4/7] Compiling and executing Java query-object exporter...", generate_params)

    experiment = run_root / "schemalens"
    step("isolate", "[5/7] Creating isolated SchemaLens V3 experiment copy...",
         lambda: isolate_pipeline(experiment))

    def prebenchmark():
        command(["bash", "scripts/run_native_prebenchmark.sh", str(dataset), str(capture),
                 str(config), str(args.max_cells)], logs / "prebenchmark.log", cwd=experiment)
        find_latest_output(run_root)
    step("prebenchmark", "[6/7] Importing real Java inputs; running Stages 1–8; freezing HBench...", prebenchmark)

    def manifest_step():
        summary, frozen = find_latest_output(run_root)
        metadata = {
            "experiment_type": "iot_benchmark_native_query_object_capture_prebenchmark",
            "upstream_repo": REPO_URL, "upstream_commit": PINNED_SHA,
            "runtime_data_origin": "native generateDataMode CSV",
            "native_generator_query_object_sample": True,
            "official_executed_sql_captured": False,
            "native_operation_scheduler_replayed": False,
            "benchmarks_executed": False,
            "Cassandra_used": False,
            "stage_8_selection_frozen_before_benchmark": True,
            "run_root": str(run_root),
            "paths": {"native_data": str(dataset), "query_capture": str(capture),
                      "native_config": str(config), "hbench": str(summary),
                      "frozen_manifest": str(frozen), "run_logs": str(logs)},
            "sha256": {"java_config": sha256(config), "query_capture": sha256(capture),
                       "schema": sha256(dataset / "schema.txt"),
                       "stage8_manifest": sha256(frozen), "hbench_csv": sha256(summary)},
            "requested_scale": {"devices": args.devices, "sensors": 2,
                                "loops": args.loops, "batch_size": args.batch_size,
                                "logical_cells_estimated": total_cells,
                                "samples_per_class": args.samples_per_class},
            "caveat": "Java parameter-object samples only; Q1-Q12 SQL semantics and mixed-workload frequencies remain to be verified.",
        }
        (run_root / "AUTO_RUN_MANIFEST.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
        (run_root / "run_status.json").write_text(json.dumps(log_records | {"final": "OK"}, indent=2)+"\n")
    step("manifest", "[7/7] Writing reproducibility manifest...", manifest_step)
    summary, frozen = find_latest_output(run_root)
    print("\n=============================================================")
    print("SUCCESS: native IoT Benchmark -> SchemaLens Stage 8 frozen")
    print("Experiment:", run_root)
    print("Data CSVs:", dataset)
    print("Native parameter objects:", capture)
    print("HBench summary:", summary)
    print("Frozen manifest:", frozen)
    print("NO Cassandra benchmark was executed.")
    print("Send back AUTO_RUN_MANIFEST.json, 30_hbench_summary.csv and 32_frozen_manifest.json.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (SetupError, OSError, subprocess.CalledProcessError) as exc:
        print("\nERROR:", exc, file=sys.stderr)
        sys.exit(2)
