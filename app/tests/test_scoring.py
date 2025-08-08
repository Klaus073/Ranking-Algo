from app.schemas import StudentBundle, ALevel, Internship, SocietyRole
from app.scoring import compute_scores


def test_basic_scoring_runs():
    bundle = StudentBundle(
        user_id="00000000-0000-0000-0000-000000000000",
        academic_year=2,
        university_tier="UCL",
        grade="First",
        alevels=[ALevel(grade="A*", category="STEM")],
        num_gcse=10,
        awards_count=2,
        internships=[Internship(tier="Bulge Bracket", months=3, end_year=2024, end_month=8)],
        total_months_experience=12,
        society_roles=[SocietyRole(role="Committee", size="Medium", years=2)],
        certifications_count=2,
        exposure="Summer Internship",
    )
    composite, academic, experience, breakdown = compute_scores(bundle)
    assert 0 <= composite <= 1000
    assert 0 <= academic <= 100
    assert 0 <= experience <= 100


