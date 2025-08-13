from __future__ import annotations

import importlib.util
import math
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .schemas import (
    AcademicComponents,
    ExperienceComponents,
    ScoreBreakdown,
    StudentBundle,
)


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def round3(value: float) -> float:
    return round(value + 1e-12, 3)


def _load_ce_scorer():
    """Dynamically load Scorer from CE_RANKING file.

    Returns (ScorerClass, cfg_dict) or (None, None) if not available.
    """
    repo_root = Path(__file__).resolve().parents[1]
    cwd_root = Path.cwd()
    candidates = [
        repo_root / "CE_RANKING.PY",
        repo_root / "CE_RANKING.py",
        repo_root / "ce_rank.py",
        cwd_root / "CE_RANKING.PY",
        cwd_root / "CE_RANKING.py",
        cwd_root / "ce_rank.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            spec = importlib.util.spec_from_file_location("ce_rank_module", str(candidate))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore[attr-defined]
                Scorer = getattr(module, "Scorer", None)
                CFG = getattr(module, "CFG", None)
                if Scorer is not None and CFG is not None:
                    return Scorer, CFG
    return None, None


def _map_university(university_tier: str) -> str:
    # Map our tiers to CE lookups: {Oxford, Cambridge, LSE, Imperial, Warwick, Non-Target}
    mapping = {
        "Oxbridge": "Oxford",
        "Imperial/LSE": "Imperial",
        "UCL": "Warwick",
        "KCL/Edinburgh": "Warwick",
        "Warwick/Bath/Durham": "Warwick",
        "Other": "Non-Target",
    }
    return mapping.get(university_tier, "Non-Target")


def _aggregate_alevel_band(grades: List[str]) -> str:
    # Build best-3 band like {"A*AA","AAA","AAB","ABB","BBB","BBC"}
    if not grades:
        return "Others"
    ordered = sorted(grades, key=lambda g: {"A*": 5, "A": 4, "B": 3, "C": 2, "D": 1, "E": 0}.get(g, -1), reverse=True)
    top3 = ordered[:3]
    counts = {g: top3.count(g) for g in set(top3)}
    # Normalize common patterns
    if counts.get("A*", 0) >= 1 and counts.get("A", 0) >= 2:
        return "A*AA"
    if counts.get("A", 0) == 3:
        return "AAA"
    if counts.get("A", 0) == 2 and counts.get("B", 0) == 1:
        return "AAB"
    if counts.get("A", 0) == 1 and counts.get("B", 0) == 2:
        return "ABB"
    if counts.get("B", 0) == 3:
        return "BBB"
    if counts.get("B", 0) == 2 and counts.get("C", 0) == 1:
        return "BBC"
    return "Others"


def _map_gcse_category(num_gcse: int) -> str:
    # Without grades distribution, choose a mid-tier neutral band
    if num_gcse >= 8:
        return "8+7-9"
    if num_gcse >= 6:
        return "6-7 7-9"
    if num_gcse >= 4:
        return "4-5 7-9"
    if num_gcse >= 1:
        return "Pass"
    return "Below"


def _map_degree_grade_band(grade: str | None, year: int) -> str:
    # Provide sensible defaults to avoid NaN in CE lookups
    if not grade:
        if year == 2:
            return "60-69"
        if year >= 3:
            return "66-69"
        return "N/A"
    g = grade
    if year == 2:
        return {
            "First": "70-79",
            "2:1": "60-69",
            "2:2": "50-54",
            "Third": "<50",
        }.get(g, "60-69")
    if year >= 3:
        return {
            "First": "73-79",
            "2:1": "66-69",
            "2:2": "50-54",
            "Third": "40-49",
        }.get(g, "66-69")
    return "N/A"


def _map_bank_tier(internships: List[dict]) -> str:
    best = None
    order = {"Bulge": 5, "Elite": 4, "UpperMid": 3, "Mid": 2, "Boutique": 1, "LowerMid": 1, "N/A": 0}
    for i in internships:
        t = (i.get("tier") or "").lower()
        val = (
            "Bulge" if "bulge" in t else
            "Elite" if "elite" in t else
            "Mid" if "middle" in t else
            "Boutique" if "regional" in t else
            "N/A"
        )
        if best is None or order[val] > order[best]:
            best = val
    return best or "N/A"


def _map_exposure(exp: str) -> str:
    return {
        "Placement": "Direct",
        "Summer Internship": "Related",
        "Spring Week": "General",
        "Shadowing": "General",
        "None": "None",
    }.get(exp, "None")


def _years_since(year: int, month: int) -> int:
    today = date.today()
    years = today.year - year + (today.month - month) / 12.0
    return int(max(0, round(years)))


def _map_intern_list(internships: List[dict]) -> List[dict]:
    out: List[dict] = []
    for it in internships:
        tier_text = (it.get("tier") or "").lower()
        # CE expects numeric tiers: 1 best → 3
        if "bulge" in tier_text:
            tier_num = 1
        elif "elite" in tier_text:
            tier_num = 2
        else:
            tier_num = 3
        months = int(it.get("months") or 0)
        end_year = int(it.get("end_year") or date.today().year)
        end_month = int(it.get("end_month") or 6)
        yrs = _years_since(end_year, end_month)
        out.append({"tier": tier_num, "months": months, "years": yrs})
    return out


def _map_society_list(roles: List[dict]) -> List[dict]:
    size_map = {"Large": 100, "Medium": 60, "Small": 30}
    role_map = {"President": "president", "Committee": "committee", "Member": "member"}
    out: List[dict] = []
    for r in roles:
        out.append({
            "role": role_map.get(r.get("role", "Member"), "member"),
            "size": size_map.get(r.get("size", "Small"), 30),
            # Treat society roles as current (no decay)
            "years": 0,
        })
    return out


def compute_scores(bundle: StudentBundle) -> Tuple[float, float, float, ScoreBreakdown]:
    Scorer, CFG = _load_ce_scorer()
    if Scorer is None or CFG is None:
        raise RuntimeError("CE scorer not available. Ensure CE_RANKING.PY exists in repo root.")

    year = min(max(int(bundle.academic_year), 0), 3)

    alevel_band = _aggregate_alevel_band([a.grade for a in bundle.alevels])
    gcse_cat = _map_gcse_category(bundle.num_gcse)
    uni = _map_university(bundle.university_tier)
    grade_band = _map_degree_grade_band(bundle.grade, year)
    bank_tier = _map_bank_tier([i.dict() for i in bundle.internships])
    exposure = _map_exposure(bundle.exposure)
    months = int(bundle.total_months_experience)
    internships = _map_intern_list([i.dict() for i in bundle.internships])
    society = _map_society_list([r.dict() for r in bundle.society_roles])

    row = {
        "ID": bundle.user_id,
        "year": year,
        "university": uni,
        "alevel": alevel_band,
        "gcse": gcse_cat,
        "grades": grade_band,
        "awards": int(bundle.awards_count or 0),
        "certs": int(bundle.certifications_count or 0),
        "bank_tier": bank_tier,
        "exposure": exposure,
        "months": months,
        "internships": internships,
        "society": society,
    }

    df = pd.DataFrame([row])
    scorer = Scorer(CFG)
    scored = scorer.score(df)
    srow = scored.iloc[0]

    academic_points = float(srow["Academic"])  # CE academic points
    experience_points = float(srow["Experience"])  # CE experience points
    composite_points = float(srow["Composite"])  # CE composite (sum)

    academic_total = round3(academic_points)
    experience_total = round3(experience_points)
    composite_val = round3(composite_points)

    breakdown = ScoreBreakdown(
        academic_components=AcademicComponents(
            universityPrestige=0.0,
            grades=0.0,
            aLevels=0.0,
            gcses=0.0,
            awards=float(bundle.awards_count or 0),
        ),
        experience_components=ExperienceComponents(
            internships=0.0,
            monthsOfExperience=float(months),
            societyRoles=0.0,
            certifications=float(bundle.certifications_count or 0),
            industryExposure=0.0,
        ),
        effective_academic_weights={"year": float(year)},
        academic_total=academic_total,
        experience_total=experience_total,
        composite=composite_val,
    )

    return composite_val, academic_total, experience_total, breakdown


