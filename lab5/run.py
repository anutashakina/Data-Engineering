"""Run from the repository root: python lab5/run.py memory|storage|scheduling|quota RUN."""
import argparse
import csv
import json
import math
import re
import secrets
import subprocess
import time
from datetime import datetime
from pathlib import Path
from string import Template

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("stage", choices=["memory", "storage", "scheduling", "quota"])
parser.add_argument("run", help="Short lowercase run ID, e.g. a1")
parser.add_argument("--context", choices=["docker-desktop", "kind-lab5"], default="docker-desktop")
args = parser.parse_args()
if not re.fullmatch(r"[a-z0-9]{1,12}", args.run):
    parser.error("Use 1 to 12 lowercase letters or digits")
namespace = "dz5-" + args.run + ("-quota" if args.stage == "quota" else "")
out = ROOT / "logs" / "lab5" / args.run / (args.stage + "-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
out.mkdir(parents=True)
kubectl = ["kubectl", "--context", args.context, "-n", namespace]


def command(argv, *, stdin=None, check=True):
    result = subprocess.run(argv, cwd=ROOT, input=stdin, encoding="utf-8", errors="replace",
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    with (out / "commands.txt").open("a", encoding="utf-8") as log:
        print("$", subprocess.list2cmdline(argv), file=log)
        print(result.stdout, file=log)
    if check and result.returncode:
        raise RuntimeError(result.stdout)
    return result


def k(*argv):
    return command(kubectl + list(argv)).stdout


def render(template, name, **values):
    manifest = Template((HERE / template).read_text()).substitute(name=name, **values)
    path = out / (name + ".yaml")
    path.write_text(manifest, encoding="utf-8")
    return str(path)


def run_job(name, mode, request, limit, image="worker", parts=1, oom=False):
    path = render("job.yaml.in", name, parts=parts, image=image, mode=mode,
                  completion_mode="Indexed" if parts > 1 else "NonIndexed",
                  run_id=args.run, request=request, limit=limit)
    k("create", "-f", path)
    condition = "failed" if oom else "complete"
    try:
        k("wait", "--for=condition=" + condition, "job/" + name, "--timeout=12m")
    finally:
        selector = "job-name=" + name
        pods = json.loads(k("get", "pods", "-l", selector, "-o", "json"))["items"]
        k("describe", "pods", "-l", selector)
        for pod in pods:
            podname = pod["metadata"]["name"]
            result = command(kubectl + ["logs", podname], check=False)
            (out / (podname + ".log")).write_text(result.stdout, encoding="utf-8")
    if oom:
        states = [s["state"].get("terminated", {}) for p in pods
                  for s in p["status"].get("containerStatuses", [])]
        if not any(t.get("reason") == "OOMKilled" for t in states):
            raise RuntimeError(f"Expected OOMKilled, observed {states}")
        print("Confirmed OOMKilled", flush=True)
    return k("logs", "job/" + name) if parts == 1 and not oom else ""


def memory():
    base_image = "python:3.13-slim"
    if command(["docker", "image", "inspect", "--format", "{{.Id}}", base_image], check=False).returncode:
        for attempt in range(1, 4):
            print(f"Pulling {base_image} ({attempt}/3)", flush=True)
            pull = command(["docker", "pull", base_image], check=False)
            if pull.returncode == 0:
                break
            if attempt == 3 or not re.search(r"EOF|timeout|connection reset", pull.stdout, re.I):
                raise RuntimeError(pull.stdout)
            time.sleep(3 * attempt)
    for image in ("worker", "input"):
        print("Building", image, flush=True)
        command(["docker", "build", "--target", image, "-t", f"dz5-pairs-{image}:local",
                 "-f", "lab5/Dockerfile", "."])
        if args.context == "kind-lab5":
            command(["kind", "load", "docker-image", f"dz5-pairs-{image}:local", "--name", "lab5"])
    run_job("pairs-oom", "single", "24Mi", "24Mi", "input", oom=True)
    profile = run_job("pairs-profile", "single", "128Mi", "1536Mi", "input")
    peak = int(re.search(r"^peak_bytes=(\d+)$", profile, re.M)[1]) / 2**20
    with (ROOT / "results/rustfs/profiling.csv").open() as f:
        dz3 = max(float(r["peak_tree_rss_mib"]) for r in csv.DictReader(f)
                  if r["method"] == "mapreduce")
    limit = math.ceil(1.25 * max(peak, dz3))
    measure = dict(container_peak_MiB=peak, dz3_peak_RSS_MiB=dz3,
                   margin_percent=25, limit_MiB=limit)
    (out / "memory.json").write_text(json.dumps(measure, indent=2))
    print(measure, flush=True)
    print(run_job("pairs-tuned", "single", "128Mi", f"{limit}Mi", "input"))


def storage():
    # Secret travels only over stdin, never in command arguments or saved manifests.
    secret = dict(apiVersion="v1", kind="Secret", metadata=dict(name="rustfs-login"),
                  stringData=dict(AWS_ACCESS_KEY_ID="lab5-" + secrets.token_hex(6),
                                  AWS_SECRET_ACCESS_KEY=secrets.token_hex(24)))
    if not k("get", "secret", "rustfs-login", "--ignore-not-found", "-o", "name").strip():
        command(kubectl + ["create", "-f", "-"], stdin=json.dumps(secret))
    k("apply", "-f", str(HERE / "rustfs.yaml"))
    k("rollout", "status", "deployment/rustfs", "--timeout=6m")
    k("get", "pvc,svc,pods", "-o", "wide")
    run_job("pairs-seed", "seed", "128Mi", "1Gi", "input")
    run_job("pairs-map", "map", "64Mi", "384Mi", parts=4)
    result = run_job("pairs-reduce", "reduce", "64Mi", "384Mi")
    summary = json.loads(result.strip().splitlines()[-1])
    with (out / "top10.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["skills", "vacancies"])
        for pair, n in summary["top10"]:
            writer.writerow([json.dumps(pair, ensure_ascii=False), n])
    print(result)


def millicpu(value):
    return int(value[:-1]) if value.endswith("m") else round(float(value) * 1000)


def node_capacity():
    nodes = json.loads(k("get", "nodes", "-o", "json"))["items"]
    node = next(n for n in nodes if not n["spec"].get("unschedulable") and
                any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"]["conditions"]))
    hostname = node["metadata"]["labels"]["kubernetes.io/hostname"]
    description = k("describe", "node", node["metadata"]["name"])
    allocated = re.search(r"^\s*cpu\s+(\S+)\s+\(", description.split("Allocated resources:")[-1], re.M)
    free = millicpu(node["status"]["allocatable"]["cpu"]) - millicpu(allocated[1])
    if free < 4:
        raise RuntimeError(f"Only {free} millicores available")
    return hostname, f"{free * 3 // 4}m"


def pod(name, priority, hostname, cpu):
    return render("pod.yaml.in", name, priority=priority, hostname=hostname, cpu=cpu)


def scheduling():
    k("apply", "-f", str(HERE / "priorities.yaml"))
    hostname, cpu = node_capacity()
    def create(name, priority):
        k("create", "-f", pod(name, priority, hostname, cpu))
    create("low", "dz5-low")
    try:
        k("wait", "--for=condition=Ready", "pod/low", "--timeout=2m")
        create("waiting", "dz5-low")
        for _ in range(60):
            status = json.loads(k("get", "pod", "waiting", "-o", "json"))["status"]
            if status["phase"] == "Pending" and any("Insufficient cpu" in c.get("message", "")
                                                   for c in status.get("conditions", [])):
                break
            time.sleep(2)
        else:
            raise RuntimeError("Did not observe Pending with Insufficient cpu")
        k("describe", "pod", "waiting")
        k("delete", "pod", "waiting")
        create("high", "dz5-high")
        k("wait", "--for=condition=Ready", "pod/high", "--timeout=3m")
        for _ in range(30):
            events = json.loads(k("get", "events", "--field-selector",
                                  "involvedObject.name=low", "-o", "json"))["items"]
            if any(e["reason"] == "Preempted" for e in events):
                break
            time.sleep(2)
        else:
            raise RuntimeError("Missing victim Preempted event")
        print("Confirmed Pending (Insufficient cpu) and Preempted", flush=True)
    finally:
        k("get", "events", "--sort-by=.metadata.creationTimestamp")
        k("get", "pods", "-o", "wide")
        k("delete", "pod", "low", "waiting", "high", "--ignore-not-found")


def quota():
    k("apply", "-f", str(HERE / "priorities.yaml"))
    hostname, _ = node_capacity()
    k("apply", "-f", str(HERE / "quota.yaml"))
    k("create", "-f", pod("allowed", "dz5-low", hostname, "5m"))
    try:
        k("wait", "--for=condition=Ready", "pod/allowed", "--timeout=2m")
        result = command(kubectl + ["create", "-f", pod("over-quota", "dz5-low", hostname, "5m")],
                         check=False)
        k("describe", "resourcequota")
        if not result.returncode or "exceeded quota" not in result.stdout:
            raise RuntimeError("Expected quota rejection: " + result.stdout)
        print(result.stdout)
    finally:
        k("delete", "pod", "allowed", "over-quota", "--ignore-not-found")


if __name__ == "__main__":
    print("Logs:", out, flush=True)
    print(k("get", "nodes", "-o", "wide"), flush=True)
    if not k("get", "namespace", namespace, "--ignore-not-found", "-o", "name").strip():
        k("create", "namespace", namespace)
    {"memory": memory, "storage": storage, "scheduling": scheduling, "quota": quota}[args.stage]()

