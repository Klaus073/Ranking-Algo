from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional, Any

from pydantic import BaseModel, Field, conint


# Inbound webhook payload
class WebhookEvent(BaseModel):
    user_id: str
    table: Literal[
        "student_profiles",
        "student_alevels",
        "student_gcses",
        "student_internships",
        "student_society_roles",
    ]
    op: Literal["INSERT", "UPDATE", "DELETE"]
    ts: datetime
    event_id: str


class EnqueueJob(BaseModel):
    job_id: str
    user_id: str
    reason: Literal["student_updated", "user_created", "manual"]
    event_ids: List[str] = Field(default_factory=list)
    enqueued_at: datetime
    config_version: str
    attempt: int = 1
    profile: Optional[Dict[str, Any]] = None


class SupabaseWebhook(BaseModel):
    event: str
    user_id: str
    email: Optional[str] = None
    verified_at: Optional[datetime] = None
    profile: Dict[str, Any] = Field(default_factory=dict)
    rankings: Dict[str, Any] = Field(default_factory=dict)
    score_breakdown: Dict[str, Any] = Field(default_factory=dict)


# Scoring inputs (bundled)
class ALevel(BaseModel):
    grade: Literal["A*", "A", "B", "C", "D", "E"]
    category: Literal["Further Maths", "STEM", "Traditional", "Creative"]


class Internship(BaseModel):
    tier: Literal["Bulge Bracket", "Elite Boutique", "Middle Market", "Regional"]
    months: int
    end_year: int
    end_month: conint(ge=1, le=12)


class SocietyRole(BaseModel):
    role: Literal["President", "Committee", "Member"]
    size: Literal["Large", "Medium", "Small"]
    years: int


class StudentBundle(BaseModel):
    user_id: str
    academic_year: conint(ge=0, le=5) = 0
    university_tier: Literal[
        "Oxbridge",
        "Imperial/LSE",
        "UCL",
        "KCL/Edinburgh",
        "Warwick/Bath/Durham",
        "Other",
    ] = "Other"
    grade: Optional[Literal["First", "2:1", "2:2", "Third"]] = None
    alevels: List[ALevel] = Field(default_factory=list)
    num_gcse: int = 0
    awards_count: int = 0
    internships: List[Internship] = Field(default_factory=list)
    total_months_experience: int = 0
    society_roles: List[SocietyRole] = Field(default_factory=list)
    certifications_count: int = 0
    exposure: Literal["Placement", "Summer Internship", "Spring Week", "Shadowing", "None"] = "None"


# Scoring outputs
class AcademicComponents(BaseModel):
    universityPrestige: float
    grades: float
    aLevels: float
    gcses: float
    awards: float


class ExperienceComponents(BaseModel):
    internships: float
    monthsOfExperience: float
    societyRoles: float
    certifications: float
    industryExposure: float


class ScoreBreakdown(BaseModel):
    academic_components: AcademicComponents
    experience_components: ExperienceComponents
    effective_academic_weights: Dict[str, float]
    academic_total: float
    experience_total: float
    composite: float


class RankingResponse(BaseModel):
    user_id: str
    composite: float
    academic: float
    experience: float
    percentile: Optional[float]
    rank: Optional[int]
    out_of: Optional[int]
    breakdown: ScoreBreakdown


