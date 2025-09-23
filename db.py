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


DATABASE_URL = os.getenv("DATABASE_URL", _build_database_url())

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


