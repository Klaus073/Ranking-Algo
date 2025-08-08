"""init tables

Revision ID: 0001_init
Revises: 
Create Date: 2025-08-08
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS student_rankings (
          user_id UUID PRIMARY KEY,
          composite NUMERIC(8,3) NOT NULL,
          academic NUMERIC(5,3) NOT NULL,
          experience NUMERIC(5,3) NOT NULL,
          rank INT NULL,
          percentile NUMERIC(5,2),
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
          config_version TEXT NOT NULL,
          compute_run_id UUID NOT NULL,
          input_checksum TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_student_rankings_composite_desc ON student_rankings (composite DESC);
        CREATE INDEX IF NOT EXISTS idx_student_rankings_updated_at_desc ON student_rankings (updated_at DESC);

        CREATE TABLE IF NOT EXISTS student_score_breakdown (
          user_id UUID PRIMARY KEY,
          academic_components JSONB NOT NULL,
          experience_components JSONB NOT NULL,
          effective_academic_weights JSONB NOT NULL,
          academic_total NUMERIC(5,3) NOT NULL,
          experience_total NUMERIC(5,3) NOT NULL,
          composite NUMERIC(8,3) NOT NULL,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
          config_version TEXT NOT NULL,
          compute_run_id UUID NOT NULL
        );

        CREATE TABLE IF NOT EXISTS student_score_history (
          user_id UUID NOT NULL,
          computed_at TIMESTAMP WITH TIME ZONE NOT NULL,
          academic NUMERIC(5,3) NOT NULL,
          experience NUMERIC(5,3) NOT NULL,
          composite NUMERIC(8,3) NOT NULL,
          config_version TEXT NOT NULL,
          compute_run_id UUID NOT NULL,
          PRIMARY KEY (user_id, computed_at)
        );

        CREATE TABLE IF NOT EXISTS ranking_updates_log (
          id BIGSERIAL PRIMARY KEY,
          user_id UUID NOT NULL,
          reason TEXT NOT NULL,
          old_score NUMERIC(8,3),
          new_score NUMERIC(8,3) NOT NULL,
          delta NUMERIC(8,3),
          payload JSONB NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
          config_version TEXT NOT NULL,
          compute_run_id UUID NOT NULL
        );

        CREATE TABLE IF NOT EXISTS score_histogram (
          bucket_id INT PRIMARY KEY,
          count BIGINT NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS global_ranking_stats (
          id INT PRIMARY KEY,
          total_users BIGINT NOT NULL DEFAULT 0,
          p50 NUMERIC(8,3),
          p90 NUMERIC(8,3),
          p99 NUMERIC(8,3),
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
          config_version TEXT NOT NULL
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS global_ranking_stats;
        DROP TABLE IF EXISTS score_histogram;
        DROP TABLE IF EXISTS ranking_updates_log;
        DROP TABLE IF EXISTS student_score_history;
        DROP TABLE IF EXISTS student_score_breakdown;
        DROP TABLE IF EXISTS student_rankings;
        """
    )


