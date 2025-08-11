from datetime import date
from app.schemas import StudentBundle, Internship
from app.scoring import compute_scores


def make_base_bundle() -> StudentBundle:
    return StudentBundle(
        user_id="00000000-0000-0000-0000-000000000000",
        academic_year=2,
        university_tier="Other",
        grade=None,
        alevels=[],
        num_gcse=0,
        awards_count=0,
        internships=[],
        total_months_experience=0,
        society_roles=[],
        certifications_count=0,
        exposure="None",
    )


def test_more_months_never_hurts():
    b = make_base_bundle()
    b.total_months_experience = 0
    c0, _, _, _ = compute_scores(b)
    b.total_months_experience = 12
    c1, _, _, _ = compute_scores(b)
    assert c1 >= c0


def test_newer_internship_not_worse():
    b_old = make_base_bundle()
    b_new = make_base_bundle()
    year = date.today().year
    b_old.internships = [Internship(tier="Middle Market", months=3, end_year=year-2, end_month=6)]
    b_new.internships = [Internship(tier="Middle Market", months=3, end_year=year-1, end_month=6)]
    c_old, _, _, _ = compute_scores(b_old)
    c_new, _, _, _ = compute_scores(b_new)
    assert c_new >= c_old




