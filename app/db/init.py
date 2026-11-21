"""Create the application schema. Safe to run repeatedly."""
from app.db.models import Base
from app.db.session import engine


def main() -> None:
    Base.metadata.create_all(engine)
    print("Database schema is ready.")


if __name__ == "__main__":
    main()