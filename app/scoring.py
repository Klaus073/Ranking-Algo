from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple

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


# Academic Components
def score_university_prestige(university_tier: str, academic_year: int) -> float:
    base_map = {
        "Oxbridge": 100,
        "Imperial/LSE": 95,
        "UCL": 90,
        "KCL/Edinburgh": 85,
        "Warwick/Bath/Durham": 80,
    }
    base = base_map.get(university_tier, 0)
    mult = 1 + 0.05 * academic_year
    return clamp(base * mult)


def score_grades(grade: str | None) -> float:
    if grade is None:
        return 0.0
    mapping = {"First": 100, "2:1": 80, "2:2": 60, "Third": 40}
    return clamp(mapping.get(grade, 0))


def score_alevels(alevels: List[dict]) -> float:
    if not alevels:
        return 0.0
    grade_points = {"A*": 100, "A": 90, "B": 80, "C": 70, "D": 60, "E": 50}
    subject_mults = {"Further Maths": 1.2, "STEM": 1.1, "Traditional": 1.0, "Creative": 0.8}

    per_subject: List[float] = []
    for item in alevels:
        g = grade_points.get(item["grade"], 0)
        m = subject_mults.get(item["category"], 1.0)
        per_subject.append(g * m)

    avg = sum(per_subject) / len(per_subject) if per_subject else 0.0
    bonus = max(0, len(alevels) - 4) * 5
    return clamp(avg + bonus)


def score_gcses(num_gcse: int) -> float:
    n = max(0, min(num_gcse, 10))
    return 100.0 * n / 10.0


def score_awards(awards_count: int) -> float:
    n = max(0, awards_count)
    if n <= 5:
        return clamp(20 * n)
    return clamp(100 + 5 * (n - 5))


def compute_academic_total(
    academic_year: int,
    universityPrestige: float,
    grades: float,
    aLevels: float,
    gcses: float,
    awards: float,
) -> Tuple[float, Dict[str, float]]:
    # Default weights
    weights = {
        "universityPrestige": 0.35,
        "grades": 0.25,
        "aLevels": 0.20,
        "gcses": 0.10,
        "awards": 0.10,
    }

    # Redistribute grade weight if academic_year < 2
    if academic_year < 2:
        grade_weight = weights.pop("grades")
        total_other = sum(weights.values())
        for k in list(weights.keys()):
            weights[k] = weights[k] + grade_weight * (weights[k] / total_other)

    # Normalize weights to sum to 1 (defensive)
    s = sum(weights.values())
    if s <= 0:
        normalized = {k: 0.0 for k in weights}
    else:
        normalized = {k: v / s for k, v in weights.items()}

    values = {
        "universityPrestige": universityPrestige,
        "grades": grades,
        "aLevels": aLevels,
        "gcses": gcses,
        "awards": awards,
    }
    # compute A = 100 * (Σ score_k * w_k) / (Σ w_k) ; w already normalized
    weighted_sum = sum(values[k] * normalized.get(k, 0.0) for k in normalized)
    academic_total = clamp(100.0 * weighted_sum / 100.0)
    return academic_total, normalized


# Experience Components
def _years_since(year: int, month: int) -> float:
    today = date.today()
    years = today.year - year + (today.month - month) / 12.0
    return max(0.0, years)


def score_internships(internships: List[dict]) -> float:
    if not internships:
        return 0.0
    base_map = {
        "Bulge Bracket": 100,
        "Elite Boutique": 90,
        "Middle Market": 70,
        "Regional": 50,
    }

    adjusted: List[float] = []
    for it in internships:
        base = base_map.get(it["tier"], 0)
        months = it.get("months", 0)
        if months <= 0:
            dur_mult = 0.0
        elif months < 3:
            dur_mult = months / 3.0
        elif months <= 6:
            dur_mult = 1.0
        else:
            dur_mult = max(0.5, 6.0 / months)

        years = _years_since(it.get("end_year", date.today().year), it.get("end_month", date.today().month))
        decay = (1 - 0.10) ** years
        single = clamp(base * dur_mult * decay)
        adjusted.append(single)

    if len(adjusted) == 1:
        return adjusted[0]
    return clamp(sum(adjusted) / math.sqrt(len(adjusted)))


def score_months_experience(total_months: int) -> float:
    m = max(0, min(int(total_months), 24))
    return 100.0 * math.log(1 + m) / math.log(1 + 24)


def score_society_roles(roles: List[dict]) -> float:
    if not roles:
        return 0.0
    role_pts = {"President": 100, "Committee": 70, "Member": 40}
    size_mult = {"Large": 1.2, "Medium": 1.0, "Small": 0.8}
    per_vals: List[float] = []
    for r in roles:
        pts = role_pts.get(r["role"], 0)
        mult = size_mult.get(r["size"], 1.0)
        years = max(0, min(int(r.get("years", 0)), 3))
        per_vals.append(pts * mult * (years / 3.0))
    if not per_vals:
        return 0.0
    avg = sum(per_vals) / len(per_vals)
    if len(roles) > 1:
        avg *= 1.05
    return clamp(avg)


def score_certifications(cert_count: int) -> float:
    return clamp(25 * min(max(cert_count, 0), 4))


def score_exposure(exposure: str) -> float:
    mapping = {
        "Placement": 100,
        "Summer Internship": 80,
        "Spring Week": 60,
        "Shadowing": 40,
        "None": 0,
    }
    return float(mapping.get(exposure, 0))


def compute_experience_total(
    academic_year: int,
    internships: float,
    months: float,
    societies: float,
    certifications: float,
    exposure: float,
) -> float:
    e_base = 100.0 * (0.4 * internships + 0.2 * months + 0.2 * societies + 0.1 * certifications + 0.1 * exposure) / 100.0
    bonus = {0: 0.20, 1: 0.20, 2: 0.10}.get(academic_year, 0.0)
    return clamp(e_base * (1 + bonus))


def compute_scores(bundle: StudentBundle) -> Tuple[float, float, float, ScoreBreakdown]:
    # Academic components
    uni = score_university_prestige(bundle.university_tier, bundle.academic_year)
    grd = score_grades(bundle.grade)
    alevels = [dict(grade=a.grade, category=a.category) for a in bundle.alevels]
    ale = score_alevels(alevels)
    gcs = score_gcses(bundle.num_gcse)
    awd = score_awards(bundle.awards_count)
    academic_total, eff_weights = compute_academic_total(
        bundle.academic_year, uni, grd, ale, gcs, awd
    )

    # Experience components
    internships = [
        dict(tier=i.tier, months=i.months, end_year=i.end_year, end_month=i.end_month)
        for i in bundle.internships
    ]
    exp_intern = score_internships(internships)
    exp_months = score_months_experience(bundle.total_months_experience)
    exp_soc = score_society_roles([dict(role=r.role, size=r.size, years=r.years) for r in bundle.society_roles])
    exp_cert = score_certifications(bundle.certifications_count)
    exp_exposure = score_exposure(bundle.exposure)
    experience_total = compute_experience_total(
        bundle.academic_year, exp_intern, exp_months, exp_soc, exp_cert, exp_exposure
    )

    composite_1000 = round3(10 * clamp(0.4 * academic_total + 0.6 * experience_total))

    breakdown = ScoreBreakdown(
        academic_components=AcademicComponents(
            universityPrestige=uni,
            grades=grd,
            aLevels=ale,
            gcses=gcs,
            awards=awd,
        ),
        experience_components=ExperienceComponents(
            internships=exp_intern,
            monthsOfExperience=exp_months,
            societyRoles=exp_soc,
            certifications=exp_cert,
            industryExposure=exp_exposure,
        ),
        effective_academic_weights=eff_weights,
        academic_total=round3(academic_total),
        experience_total=round3(experience_total),
        composite=round3(composite_1000),
    )

    return composite_1000, academic_total, experience_total, breakdown


