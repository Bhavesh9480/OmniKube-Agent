"""Collector and parser for Kubernetes Metrics Server (metrics.k8s.io) resource metrics.

Lightweight, dependency-free parser for live container CPU and Memory utilization.
"""

import re
from typing import Any, Dict, List, Optional, Tuple


def parse_cpu_quantity(value: str) -> int:
    """Parse Kubernetes CPU quantity into millicores.

    Examples:
        '100m' -> 100
        '1' -> 1000
        '50000000n' (nanocores) -> 50
        '50000u' (microcores) -> 50
    """
    if not value:
        return 0
    val_str = str(value).strip()

    if val_str.endswith("m"):
        try:
            return int(val_str[:-1])
        except ValueError:
            return 0
    elif val_str.endswith("n"):
        try:
            # 1 millicore = 1,000,000 nanocores
            return int(int(val_str[:-1]) / 1_000_000)
        except ValueError:
            return 0
    elif val_str.endswith("u"):
        try:
            # 1 millicore = 1,000 microcores
            return int(int(val_str[:-1]) / 1_000)
        except ValueError:
            return 0
    else:
        try:
            # Whole cores
            return int(float(val_str) * 1000)
        except ValueError:
            return 0


def parse_memory_quantity(value: str) -> int:
    """Parse Kubernetes Memory quantity into bytes.

    Supports binary SI units (Ki, Mi, Gi, Ti) and decimal SI units (k, M, G, T).

    Examples:
        '64Mi' -> 67,108,864
        '128Ki' -> 131,072
        '1Gi' -> 1,073,741,824
        '100M' -> 100,000,000
    """
    if not value:
        return 0
    val_str = str(value).strip()

    units_binary = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "Pi": 1024**5,
        "Ei": 1024**6,
    }
    units_decimal = {
        "k": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
        "P": 1000**5,
        "E": 1000**6,
    }

    for unit, multiplier in units_binary.items():
        if val_str.endswith(unit):
            try:
                num = float(val_str[: -len(unit)])
                return int(num * multiplier)
            except ValueError:
                return 0

    for unit, multiplier in units_decimal.items():
        if val_str.endswith(unit):
            try:
                num = float(val_str[: -len(unit)])
                return int(num * multiplier)
            except ValueError:
                return 0

    try:
        return int(float(val_str))
    except ValueError:
        return 0


def format_memory_bytes(bytes_val: int) -> str:
    """Format bytes into a human-readable memory string (MiB / GiB)."""
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024**2:
        return f"{bytes_val / 1024:.1f} KiB"
    elif bytes_val < 1024**3:
        return f"{bytes_val / (1024**2):.1f} MiB"
    else:
        return f"{bytes_val / (1024**3):.2f} GiB"


def parse_pod_metrics(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Parse PodMetricsList items into a lookup map keyed by '{namespace}/{name}'.

    Returns:
        Dict mapping '{namespace}/{name}' to structured CPU/Memory usage dict.
    """
    metrics_map: Dict[str, Dict[str, Any]] = {}

    for item in items:
        metadata = item.get("metadata", {})
        namespace = metadata.get("namespace", "default")
        name = metadata.get("name", "unknown")
        timestamp = item.get("timestamp")
        containers = item.get("containers", [])

        total_cpu_m = 0
        total_mem_bytes = 0
        parsed_containers: List[Dict[str, Any]] = []

        for c in containers:
            c_name = c.get("name", "unknown")
            usage = c.get("usage", {})
            cpu_raw = usage.get("cpu", "0")
            mem_raw = usage.get("memory", "0")

            c_cpu_m = parse_cpu_quantity(cpu_raw)
            c_mem_b = parse_memory_quantity(mem_raw)

            total_cpu_m += c_cpu_m
            total_mem_bytes += c_mem_b

            parsed_containers.append({
                "name": c_name,
                "cpuMillicores": c_cpu_m,
                "cpuFormatted": f"{c_cpu_m}m",
                "memoryBytes": c_mem_b,
                "memoryFormatted": format_memory_bytes(c_mem_b),
            })

        key = f"{namespace}/{name}"
        metrics_map[key] = {
            "namespace": namespace,
            "name": name,
            "timestamp": timestamp,
            "cpuMillicores": total_cpu_m,
            "cpuFormatted": f"{total_cpu_m}m",
            "memoryBytes": total_mem_bytes,
            "memoryFormatted": format_memory_bytes(total_mem_bytes),
            "containers": parsed_containers,
        }

    return metrics_map
