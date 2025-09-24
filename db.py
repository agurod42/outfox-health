import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


def _build_database_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "health")
    user = os.getenv("POSTGRES_USER", "health")
    password = os.getenv("POSTGRES_PASSWORD", "health")
    return f"postgresql+asyncpg://{user}:{password}@{host}:5432/{db}"


def _prefer_available_driver(url: str) -> str:
    """Prefer asyncpg, but gracefully fall back to psycopg if asyncpg isn't installed.

    This avoids requiring asyncpg wheels for environments (e.g., Python 3.13) where
    they may not be available just to run tests that stub DB access.
    """
    if "+asyncpg" in url:
        try:
            import asyncpg  # type: ignore # noqa: F401
            return url
        except Exception:
            # Replace driver with psycopg which we pin and provide binary wheels for.
            return url.replace("+asyncpg", "+psycopg")
    return url


DATABASE_URL = _prefer_available_driver(os.getenv("DATABASE_URL", _build_database_url()))

engine: AsyncEngine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


