"""Regression gate shared by the read-path tools.

A cell (one config and scenario) regresses when the candidate tree is worse
than the baseline tree by more than the allowed percentage, and by more than
the uncertainty of that delta. The delta compares the two trees' medians, so
its uncertainty is how far those medians could move, not how much a single
round varies: it is half the 95% bootstrap interval of the delta, resampling
each tree's rounds (or passes), widened by INTERVAL_FACTOR. With more rounds
the interval narrows, so more rounds buy a finer verdict. The noise rule
keeps a noisy cell from failing a run on a difference it cannot resolve; the
allowed percentage is the floor below which a slowdown or growth is
accepted.

The noise rule has a ceiling, --max-limit (or the allowed percentage, when
that is higher). A cell whose noise term would exceed it is inconclusive: it
is too noisy to catch a regression of that size, so it fails the gate instead
of passing unseen. run_bench gives inconclusive cells
the same extra rounds as regressed ones before the verdict.

Every tool ends with the same summary: whether the gate passed, the allowed
percentage, and for every regressed or inconclusive cell the baseline and
candidate numbers, the delta and the limit. With
--fail-on-regression the tool exits 1 when a cell regressed or is
inconclusive, after writing its results. --warn-inconclusive still reports
inconclusive cells but fails only on regressions, for runners too noisy to
judge every cell.
"""

import math
import os
import random
import statistics
import sys

# Default allowed regression, in percent. Timing noise on a quiet machine is
# 1-8% per cell, so 5% plus the noise rule passes run-to-run variance while
# still catching the slowdowns that matter for every command. Peak memory
# moves by a few percent between runs too, so mem_probe also measures its
# noise across passes.
DEFAULT_ALLOWED_PCT = 5.0
# A bootstrap interval from 8-12 rounds runs narrow; widen it so the gate
# errs towards calling a cell noisy rather than missing that it is.
INTERVAL_FACTOR = 1.5
# Bootstrap draws per cell, from a fixed seed so a verdict is reproducible.
BOOTSTRAP_DRAWS = 2000
# Below this many samples per tree a bootstrap has too little to resample
# and its interval is far too narrow, so the cell gets no verdict.
MIN_SAMPLES = 4
# Largest limit the noise rule may set, in percent. Above it a cell cannot
# catch a regression that size and is reported as inconclusive.
DEFAULT_MAX_LIMIT_PCT = 10.0


def add_gate_args(parser, default_allowed=DEFAULT_ALLOWED_PCT):
    parser.add_argument(
        "--compare",
        nargs="*",
        default=[],
        help="candidate:baseline label pairs to compare, e.g. pr:master",
    )
    parser.add_argument(
        "--allowed-regression",
        type=float,
        default=default_allowed,
        metavar="PCT",
        help=f"accepted slowdown or growth in percent (default {default_allowed})",
    )
    parser.add_argument(
        "--max-limit",
        type=float,
        default=DEFAULT_MAX_LIMIT_PCT,
        metavar="PCT",
        help="largest limit the noise rule may set; noisier cells are "
        f"inconclusive and fail the gate (default {DEFAULT_MAX_LIMIT_PCT:g})",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="exit 1 when a compared pair regressed or is inconclusive",
    )
    parser.add_argument(
        "--warn-inconclusive",
        action="store_true",
        help="report inconclusive cells without failing the gate, for "
        "runners too noisy to judge some cells",
    )
    parser.add_argument(
        "--summary",
        metavar="FILE",
        help="also append the summary to FILE, e.g. $GITHUB_STEP_SUMMARY",
    )


def compare_pairs(args, trees):
    pairs = []
    for pair in args.compare:
        new, old = pair.split(":", 1)
        if new not in trees or old not in trees:
            raise SystemExit(f"--compare {pair}: unknown tree label")
        pairs.append((new, old))
    return pairs


def delta_uncertainty(old, new):
    """Half the 95% bootstrap interval of median(new) / median(old) - 1.

    Each tree's samples are resampled on their own: the trees run in
    separate processes, and pairing them by round does not narrow the
    interval on a shared runner. Infinite with fewer than MIN_SAMPLES per
    tree, which makes the cell inconclusive.
    """
    if len(old) < MIN_SAMPLES or len(new) < MIN_SAMPLES:
        return math.inf
    rng = random.Random(0)
    median = statistics.median
    draws = sorted(
        median(rng.choices(new, k=len(new))) / median(rng.choices(old, k=len(old)))
        for _ in range(BOOTSTRAP_DRAWS)
    )
    low = draws[int(0.025 * BOOTSTRAP_DRAWS)]
    high = draws[int(0.975 * BOOTSTRAP_DRAWS) - 1]
    return (high - low) / 2


def evaluate(cells, allowed_pct, max_limit_pct=DEFAULT_MAX_LIMIT_PCT):
    """Return the cells with delta, noise, limit, regressed and inconclusive
    filled in.

    Each cell is a dict with config, scenario, old_samples and new_samples
    (one value per round or pass, lower is better); old and new are their
    medians.
    """
    out = []
    for c in cells:
        old_samples, new_samples = c["old_samples"], c["new_samples"]
        old, new = statistics.median(old_samples), statistics.median(new_samples)
        delta = (new - old) / old
        noise = INTERVAL_FACTOR * delta_uncertainty(old_samples, new_samples)
        limit = max(allowed_pct / 100, noise)
        regressed = delta > limit
        out.append(
            {
                **c,
                "old": old,
                "new": new,
                "delta": delta,
                "noise": noise,
                "limit": limit,
                "regressed": regressed,
                # only the noise term is capped: an allowed regression above
                # --max-limit is the caller's choice, not a noisy cell
                "inconclusive": not regressed
                and noise > max(allowed_pct, max_limit_pct) / 100,
            }
        )
    return out


def table(rows, new, old):
    lines = [
        f"| config | scenario | {old} | {new} | delta | limit |",
        "|---|---|---|---|---|---|",
    ]
    for c in rows:
        lines.append(
            f"| {c['config']} | {c['scenario']} | {c['old']:.2f} | {c['new']:.2f} "
            f"| {c['delta'] * 100:+.1f}% | {limit_text(c)} |"
        )
    return lines


def limit_text(c):
    if math.isinf(c["limit"]):
        return "too few samples"
    return f"+{c['limit'] * 100:.1f}%"


def annotation(level, title, c, new, old):
    """A GitHub Actions workflow command that flags one cell on the run's
    checks page. % must be escaped in the message."""
    message = (
        f"{title}: {c['config']} {c['scenario']}: {new} vs {old} "
        f"{c['delta'] * 100:+.1f}%, limit {limit_text(c)}"
    )
    return f"::{level}::{message.replace('%', '%25')}"


def render(
    title,
    evaluated,
    new,
    old,
    allowed_pct,
    unit,
    max_limit_pct=None,
    inconclusive_fails=True,
):
    if max_limit_pct is None:
        max_limit_pct = DEFAULT_MAX_LIMIT_PCT
    regressed = sorted(
        (c for c in evaluated if c["regressed"]), key=lambda c: -c["delta"]
    )
    inconclusive = sorted(
        (c for c in evaluated if c["inconclusive"]), key=lambda c: -c["limit"]
    )
    problems = []
    if regressed:
        problems.append(f"{len(regressed)} regression(s)")
    if inconclusive and inconclusive_fails:
        problems.append(f"{len(inconclusive)} inconclusive")
    if problems:
        status = f"**FAILED: {', '.join(problems)}**"
    elif inconclusive:
        status = f"**passed, {len(inconclusive)} inconclusive**"
    else:
        status = "**passed**"
    lines = [
        f"### {title}: {new} vs {old}, {status}",
        "",
        f"Allowed regression: {allowed_pct:g}%, or the uncertainty of the delta "
        f"({INTERVAL_FACTOR:g}x half its 95% bootstrap interval) when that is "
        f"larger, up to {max_limit_pct:g}%. A cell whose limit "
        f"would exceed {max_limit_pct:g}% is inconclusive. {len(evaluated)} cells "
        "compared. "
        f"Values are {unit}; lower is better.",
        "",
    ]
    if regressed:
        lines += ["Regressions:", "", *table(regressed, new, old), ""]
    if inconclusive:
        lines += [
            f"Inconclusive, too noisy to catch a {max_limit_pct:g}% regression:",
            "",
            *table(inconclusive, new, old),
            "",
        ]
    if not problems:
        # nothing failed: show the closest cells, so the margin is visible
        closest = sorted(evaluated, key=lambda c: -c["delta"])[:5]
        lines += [
            "Largest deltas (none above the limit):",
            "",
            *table(closest, new, old),
        ]
    annotations = [
        annotation("error", f"{title} regressed", c, new, old) for c in regressed
    ] + [
        annotation(
            "error" if inconclusive_fails else "warning",
            f"{title} too noisy to judge",
            c,
            new,
            old,
        )
        for c in inconclusive
    ]
    return "\n".join(lines).rstrip(), bool(problems), annotations


def finish(args, summaries):
    """Print and optionally append the summaries; exit 1 on a regression,
    or on an inconclusive cell without --warn-inconclusive, when
    --fail-on-regression is set. On GitHub Actions every regressed and
    inconclusive cell is also annotated, so the PR's checks page shows it
    even when the job passes."""
    text = "\n\n".join(s for s, _, _ in summaries)
    print("\n" + text)
    if args.summary:
        with open(args.summary, "a") as f:
            f.write(text + "\n\n")
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for _, _, annotations in summaries:
            for line in annotations:
                print(line)
    if args.fail_on_regression and any(failed for _, failed, _ in summaries):
        sys.exit(1)
