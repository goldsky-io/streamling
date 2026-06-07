#!/usr/bin/env python3
"""
Deterministic risk scoring for AI code review.

Computes a composite risk score per changed file using five signals derived
from git history and static file analysis. No external dependencies — uses
only the Python standard library.

Usage:
    python risk_score.py \
        --changed-files changed_files.txt \
        --config review_config.json \
        --base-ref origin/main \
        --output risk_report.json
"""

import argparse
import json
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from fnmatch import fnmatch


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def run_git(*args: str) -> str:
    """Run a git command and return stdout. Returns empty string on failure."""
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""


@lru_cache(maxsize=2048)
def read_file_cached(path: str) -> str:
    """Read file contents with caching."""
    try:
        return Path(path).read_text(errors="replace")
    except OSError:
        return ""


def count_file_lines(path: str) -> int:
    """Count lines in a file."""
    content = read_file_cached(path)
    if not content:
        return 0
    return content.count("\n") + (1 if not content.endswith("\n") else 0)


# ---------------------------------------------------------------------------
# Module / crate helpers
# ---------------------------------------------------------------------------

def extract_crate_name(file_path: str) -> str:
    """Extract the crate name from a file path like crates/streamling-core/src/foo.rs."""
    parts = Path(file_path).parts
    # Pattern: crates/<crate-name>/...
    if len(parts) >= 2 and parts[0] == "crates":
        return parts[1]
    return ""


def extract_module_name(file_path: str) -> str:
    """
    Extract a rough Rust module path for import-matching.
    e.g. crates/streamling-core/src/topology.rs -> topology
         crates/streamling-core/src/ops/filter.rs -> filter
    """
    p = Path(file_path)
    stem = p.stem
    if stem in ("mod", "lib", "main"):
        # Use parent directory name instead
        return p.parent.name
    return stem


def is_test_file(file_path: str) -> bool:
    """Heuristic: is this file primarily test code?"""
    p = Path(file_path)
    parts_lower = [part.lower() for part in p.parts]
    name = p.name.lower()

    if "tests" in parts_lower or "test" in parts_lower:
        return True
    if name.startswith("test_") or name.endswith("_test.rs"):
        return True
    if "e2e" in parts_lower:
        return True

    # Check for #[cfg(test)] as the dominant content
    content = read_file_cached(file_path)
    if content:
        lines = content.split("\n")
        total = len(lines)
        test_lines = sum(1 for l in lines if "#[test]" in l or "#[cfg(test)]" in l or "assert" in l)
        if total > 0 and test_lines / total > 0.15:
            return True

    return False


def should_ignore(file_path: str, patterns: list[str]) -> bool:
    """Check if a file matches any ignore pattern."""
    for pattern in patterns:
        if fnmatch(file_path, pattern):
            return True
        # Also check just the filename for simple patterns
        if fnmatch(Path(file_path).name, pattern):
            return True
    return False


# ---------------------------------------------------------------------------
# Signal 1: Change Density
# ---------------------------------------------------------------------------

def compute_change_density(file_path: str, base_ref: str) -> float:
    """
    Ratio of changed lines to total file lines.
    New files get a score of 0.5 (not max — new code is often well-thought-out).
    """
    diff_output = run_git("diff", "--numstat", base_ref, "--", file_path)
    if not diff_output.strip():
        return 0.0

    first_line = diff_output.strip().split("\n")[0]
    parts = first_line.split("\t")
    if len(parts) < 2:
        return 0.0

    # Binary files show "-" for added/deleted
    if parts[0] == "-" or parts[1] == "-":
        return 0.5

    added = int(parts[0])
    deleted = int(parts[1])
    lines_changed = added + deleted

    total_lines = count_file_lines(file_path)
    if total_lines == 0:
        return 0.5  # New file

    return min(1.0, lines_changed / total_lines)


# ---------------------------------------------------------------------------
# Signal 2: Historical Volatility
# ---------------------------------------------------------------------------

def compute_historical_volatility(file_path: str, lookback_days: int, max_commits: int) -> float:
    """
    Normalized commit count for this file over the lookback period.
    High-churn files are higher risk.
    """
    since_date = run_git("log", "-1", f"--before={lookback_days} days ago", "--format=%ci")
    # Use --since instead for simplicity
    output = run_git(
        "log",
        "--oneline",
        f"--since={lookback_days} days ago",
        "--follow",
        "--",
        file_path,
    )
    if not output.strip():
        return 0.0

    commit_count = len(output.strip().split("\n"))
    return min(1.0, commit_count / max_commits)


# ---------------------------------------------------------------------------
# Signal 3: File Complexity
# ---------------------------------------------------------------------------

def compute_complexity(file_path: str) -> float:
    """
    Complexity proxy using file size, unsafe blocks, and unwrap usage.
    Non-Rust files get a low baseline.
    """
    if not file_path.endswith(".rs"):
        return 0.1

    total_lines = count_file_lines(file_path)
    content = read_file_cached(file_path)

    # File size score: 0-200 lines = low, 1000+ = high
    size_score = min(1.0, total_lines / 1000.0)

    # Unsafe blocks
    unsafe_count = content.count("unsafe ")
    unsafe_score = min(1.0, unsafe_count / 3.0)

    # Unwrap usage (in non-test code, rough heuristic)
    unwrap_count = content.count(".unwrap()")
    unwrap_score = min(1.0, unwrap_count / 10.0)

    # Nesting depth proxy: count lines with 4+ levels of indentation (16+ spaces)
    deep_lines = sum(1 for line in content.split("\n") if line.startswith(" " * 16) and line.strip())
    nesting_score = min(1.0, deep_lines / 50.0)

    return (
        0.40 * size_score
        + 0.20 * unsafe_score
        + 0.20 * unwrap_score
        + 0.20 * nesting_score
    )


# ---------------------------------------------------------------------------
# Signal 4: Blast Radius
# ---------------------------------------------------------------------------

def compute_blast_radius(file_path: str, all_rs_files: list[str], core_crates: list[str]) -> float:
    """
    How many other files reference this module.
    Core crates get a 1.5x multiplier since changes ripple further.
    """
    if not file_path.endswith(".rs"):
        return 0.1

    module_name = extract_module_name(file_path)
    if not module_name or module_name in ("lib", "main", "mod"):
        # These are entry points — high blast radius by definition
        return 0.8

    # Count files that reference this module name
    import_count = 0
    for other_file in all_rs_files:
        if other_file == file_path:
            continue
        other_content = read_file_cached(other_file)
        # Look for use/mod statements or direct module references
        if f"use {module_name}" in other_content or f"mod {module_name}" in other_content or f"{module_name}::" in other_content:
            import_count += 1

    # Crate multiplier
    crate_name = extract_crate_name(file_path)
    crate_multiplier = 1.5 if crate_name in core_crates else 1.0

    # Normalize: 0 imports = 0.0, 20+ = 1.0
    raw_score = min(1.0, import_count / 20.0)
    return min(1.0, raw_score * crate_multiplier)


# ---------------------------------------------------------------------------
# Signal 5: Test Coverage Delta
# ---------------------------------------------------------------------------

def compute_test_coverage_delta(changed_files: list[str], base_ref: str) -> float:
    """
    Ratio of test line changes to implementation line changes across the whole PR.
    Returns a risk score: well-tested PRs get low score, undertested get high.
    This is a PR-level signal applied equally to all files.
    """
    test_lines_changed = 0
    impl_lines_changed = 0

    for f in changed_files:
        diff_output = run_git("diff", "--numstat", base_ref, "--", f)
        if not diff_output.strip():
            continue

        first_line = diff_output.strip().split("\n")[0]
        parts = first_line.split("\t")
        if len(parts) < 2 or parts[0] == "-":
            continue

        added = int(parts[0])
        deleted = int(parts[1])
        total = added + deleted

        if is_test_file(f):
            test_lines_changed += total
        else:
            impl_lines_changed += total

    if impl_lines_changed == 0:
        return 0.0  # Pure test changes or no impl changes = low risk

    ratio = test_lines_changed / impl_lines_changed

    if ratio >= 0.5:
        return 0.0   # Well tested
    elif ratio >= 0.2:
        return 0.3   # Moderately tested
    elif ratio >= 0.05:
        return 0.6   # Lightly tested
    else:
        return 0.8   # Undertested


# ---------------------------------------------------------------------------
# Scoring aggregation
# ---------------------------------------------------------------------------

def compute_file_risk(signals: dict[str, float], weights: dict[str, float]) -> float:
    """Weighted sum of all signals, clamped to [0, 1]."""
    score = sum(signals.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
    return round(min(1.0, max(0.0, score)), 4)


def compute_overall_risk(file_scores: dict) -> float:
    """
    Overall PR risk: max file risk + mild penalty for many files changed.
    """
    if not file_scores:
        return 0.0

    max_risk = max(entry["composite"] for entry in file_scores.values())
    file_count_penalty = min(0.15, len(file_scores) / 100.0)
    return round(min(1.0, max_risk + file_count_penalty), 4)


def risk_level(score: float, thresholds: dict) -> str:
    """Map a risk score to HIGH / MEDIUM / LOW."""
    if score >= thresholds.get("high", 0.7):
        return "HIGH"
    elif score >= thresholds.get("medium", 0.4):
        return "MEDIUM"
    else:
        return "LOW"


def risk_badge(level: str) -> str:
    """Emoji badge for a risk level."""
    return {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}.get(level, "\u2753")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compute per-file risk scores for AI code review")
    parser.add_argument("--changed-files", required=True, help="Path to file listing changed file paths (one per line)")
    parser.add_argument("--config", required=True, help="Path to review_config.json")
    parser.add_argument("--base-ref", required=True, help="Git ref for the base branch (e.g. origin/main)")
    parser.add_argument("--output", required=True, help="Path for output JSON")
    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = json.load(f)

    weights = config["weights"]
    thresholds = config["thresholds"]
    core_crates = config.get("core_crates", [])
    ignore_patterns = config.get("ignore_patterns", [])
    lookback_days = config.get("lookback_days", 180)
    max_volatility = config.get("max_volatility_commits", 50)

    # Load changed files
    with open(args.changed_files) as f:
        changed_files = [line.strip() for line in f if line.strip()]

    # Filter ignored files
    scored_files = [fp for fp in changed_files if not should_ignore(fp, ignore_patterns)]
    ignored_files = [fp for fp in changed_files if should_ignore(fp, ignore_patterns)]

    if not scored_files:
        # Nothing to score
        report = {
            "overall_risk": 0.0,
            "risk_level": "LOW",
            "risk_badge": risk_badge("LOW"),
            "file_count": 0,
            "scored_files": 0,
            "ignored_files": len(ignored_files),
            "files": {},
            "high_risk_summary": [],
        }
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Risk report written to {args.output} (no files to score)")
        return

    # Gather all .rs files in the project for blast radius calculation
    all_rs_output = run_git("ls-files", "*.rs")
    all_rs_files = [line.strip() for line in all_rs_output.split("\n") if line.strip()]

    # Compute PR-level test coverage delta (shared across all files)
    pr_test_delta = compute_test_coverage_delta(scored_files, args.base_ref)

    # Compute per-file signals
    file_scores: dict[str, dict] = {}

    for file_path in scored_files:
        signals = {
            "change_density": compute_change_density(file_path, args.base_ref),
            "historical_volatility": compute_historical_volatility(file_path, lookback_days, max_volatility),
            "complexity": compute_complexity(file_path),
            "blast_radius": compute_blast_radius(file_path, all_rs_files, core_crates),
            "test_coverage_delta": pr_test_delta,
        }

        composite = compute_file_risk(signals, weights)
        level = risk_level(composite, thresholds)

        file_scores[file_path] = {
            "composite": composite,
            "risk_level": level,
            "risk_badge": risk_badge(level),
            "signals": {k: round(v, 4) for k, v in signals.items()},
        }

    # Sort by composite score descending
    sorted_files = dict(
        sorted(file_scores.items(), key=lambda x: x[1]["composite"], reverse=True)
    )

    # Compute overall risk
    overall = compute_overall_risk(sorted_files)
    overall_level = risk_level(overall, thresholds)

    # Build high-risk summary strings
    high_risk_summary = []
    for fp, data in sorted_files.items():
        if data["composite"] >= thresholds.get("medium", 0.4):
            reasons = []
            sigs = data["signals"]
            if sigs["historical_volatility"] >= 0.5:
                reasons.append("high churn")
            if sigs["complexity"] >= 0.5:
                reasons.append("complex file")
            if sigs["blast_radius"] >= 0.5:
                reasons.append("high blast radius")
            if sigs["change_density"] >= 0.5:
                reasons.append("large change")
            if sigs["test_coverage_delta"] >= 0.5:
                reasons.append("undertested")
            reason_str = ", ".join(reasons) if reasons else "multiple moderate signals"
            short_path = Path(fp).name
            high_risk_summary.append(f"{short_path}: {reason_str} (score: {data['composite']})")

    report = {
        "overall_risk": overall,
        "risk_level": overall_level,
        "risk_badge": risk_badge(overall_level),
        "file_count": len(changed_files),
        "scored_files": len(scored_files),
        "ignored_files": len(ignored_files),
        "files": sorted_files,
        "high_risk_summary": high_risk_summary,
    }

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    # Also print summary to stdout for CI logs
    print(f"Risk Report: {overall_level} ({overall}/1.00)")
    print(f"  Scored {len(scored_files)} files, ignored {len(ignored_files)}")
    for summary_line in high_risk_summary[:5]:
        print(f"  - {summary_line}")
    print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()
