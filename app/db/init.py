"""Create the application schema. Safe to run repeatedly."""
from sqlalchemy import text
from app.db.models import Base
from app.db.session import engine


def main() -> None:
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ;"))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS analyzed_at TIMESTAMPTZ;"))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS category TEXT;"))
        conn.execute(text("ALTER TABLE incidents ADD COLUMN IF NOT EXISTS alternatives JSONB;"))
    print("Database schema is ready.")


if __name__ == "__main__":
    main()