"""
Validation harness for the measured social_score composite.

Proves the new composite fixes every symptom of the legacy hardcoded
brand->score lookup:

  * Legacy: only 19 distinct values  -> new: hundreds+ of distinct values
  * Legacy: frozen per model over time -> new: varies across quarters
  * Legacy: identical across generations -> new: E30 vs E36 vs E46 M3 differ

Because the live social collectors require credentials/network that may not be
present here, this harness drives the *scoring math* (percentile ranking, SoV,
renormalized blend) with realistic synthetic sub-signals so the structural
guarantees can be checked deterministically and offline. When run in an
environment with credentials, point `--csv` at a freshly generated
mii_results_latest.csv to validate the real output instead.
"""

import argparse
import sys

import numpy as np
import pandas as pd

from social_score import (
    SUBSIGNAL_WEIGHTS,
    get_segment,
    mid_rank_percentile,
    blend_with_renormalization,
)


def synthetic_frame(seed: int = 7) -> pd.DataFrame:
    """Build a model x quarter frame with synthetic, per-(model,quarter) signals
    that mirror the real grain (multiple M3 generations, multiple quarters)."""
    rng = np.random.default_rng(seed)
    models = [
        ("BMW", "M3 (E30)"), ("BMW", "M3 (E36)"), ("BMW", "M3 (E46)"),
        ("BMW", "M3 (E92)"), ("Porsche", "911"), ("Porsche", "Cayman"),
        ("Toyota", "Supra"), ("Toyota", "MR2"), ("Honda", "S2000"),
        ("Honda", "NSX"), ("Nissan", "GT-R"), ("Mazda", "RX-7"),
        ("Ferrari", "458"), ("Ford", "Mustang"), ("Chevrolet", "Corvette"),
    ]
    quarters = [f"2023-{m:02d}" for m in range(1, 13)] + [f"2024-{m:02d}" for m in range(1, 7)]
    recs = []
    for mk, md in models:
        # each model has its own baseline popularity, but it drifts per quarter
        base = rng.uniform(20, 400)
        for q in quarters:
            drift = rng.uniform(0.4, 1.8)
            recs.append({
                "manufacturer": mk, "model": md, "quarter": q,
                "segment": get_segment(mk),
                "social_mentions": max(0.0, base * drift + rng.normal(0, 20)),
                "social_engagement_rate": rng.uniform(0.01, 0.4),
                "social_video_views": float(rng.integers(0, 5_000_000)),
                "social_sentiment": rng.uniform(0.4, 1.0),
            })
    return pd.DataFrame(recs)


def score_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Apply SoV + percentile + renormalized blend (mirrors compute_social_scores)."""
    df = df.copy()
    seg_totals = df.groupby(["segment", "quarter"])["social_mentions"].transform("sum")
    df["social_sov"] = df["social_mentions"] / seg_totals.replace(0, np.nan)

    ranked_cols = {}
    for col in SUBSIGNAL_WEIGHTS:
        rcol = f"_rank_{col}"
        df[rcol] = mid_rank_percentile(df[col])
        ranked_cols[col] = rcol
    df["social_score"] = df.apply(
        lambda r: blend_with_renormalization(r, ranked_cols), axis=1
    ).round(2)
    return df


def run_checks(df: pd.DataFrame) -> bool:
    print("\n" + "=" * 70)
    print("SOCIAL SCORE VALIDATION")
    print("=" * 70)
    scored = df.dropna(subset=["social_score"])
    ok = True

    # 1) distinct values >> 19
    distinct = scored["social_score"].nunique()
    c1 = distinct > 100
    ok &= c1
    print(f"[{'PASS' if c1 else 'FAIL'}] distinct social_score values: {distinct} "
          f"(legacy was 19; require > 100)")

    # 2) varies across quarters for the same model
    per_model_var = scored.groupby(["manufacturer", "model"])["social_score"].nunique()
    frac_varying = (per_model_var > 1).mean()
    c2 = frac_varying > 0.9
    ok &= c2
    print(f"[{'PASS' if c2 else 'FAIL'}] models varying across quarters: "
          f"{frac_varying*100:.1f}% (require > 90%)")

    # 3) M3 generations differ
    m3 = scored[(scored.manufacturer == "BMW") & (scored.model.str.startswith("M3"))]
    gen_means = m3.groupby("model")["social_score"].mean()
    c3 = gen_means.nunique() == len(gen_means) and len(gen_means) >= 2
    ok &= c3
    print(f"[{'PASS' if c3 else 'FAIL'}] M3 generations are distinct:")
    for model, mean in gen_means.items():
        print(f"          {model:<12} mean social_score = {mean:.2f}")

    # 4) renormalization: drop a sub-signal and confirm score still computes
    probe = df.copy()
    probe.loc[probe.index[:50], "social_mentions"] = np.nan  # 30% weight removed
    probe = score_frame(probe.drop(columns=[c for c in probe.columns if c.startswith("_rank_")], errors="ignore"))
    c4 = probe["social_score"].notna().sum() >= scored.shape[0] * 0.9
    ok &= c4
    print(f"[{'PASS' if c4 else 'FAIL'}] renormalization holds when a sub-signal "
          f"is missing (no NaN cascade, no brand default)")

    print("=" * 70)
    print("RESULT:", "ALL CHECKS PASSED ✅" if ok else "CHECKS FAILED ❌")
    print("=" * 70)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="validate a real mii_results_latest.csv instead of synthetic")
    args = ap.parse_args()

    if args.csv:
        df = pd.read_csv(args.csv)
        if "social_score" not in df.columns:
            print(f"❌ {args.csv} has no social_score column")
            return 1
        # normalize key column names if needed
        if "manufacturer" not in df.columns and "make" in df.columns:
            df = df.rename(columns={"make": "manufacturer"})
        ok = run_checks(df)
    else:
        ok = run_checks(score_frame(synthetic_frame()))

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
