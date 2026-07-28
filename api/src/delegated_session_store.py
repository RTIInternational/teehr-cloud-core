from datetime import UTC, datetime

import asyncpg


class DelegatedSessionStore:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def startup(self):
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=5)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS delegated_sessions (
                    sid TEXT PRIMARY KEY,
                    subject TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    realm TEXT NOT NULL,
                    catalog TEXT NOT NULL,
                    audience TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS delegated_sessions_expires_at_idx
                ON delegated_sessions (expires_at);
                """
            )

    async def shutdown(self):
        if self._pool:
            await self._pool.close()

    async def put_session(
        self,
        *,
        sid: str,
        subject: str,
        user_id: str,
        session_id: str,
        realm: str,
        catalog: str,
        audience: str,
        refresh_token: str,
        expires_at_epoch_seconds: int,
    ):
        if self._pool is None:
            raise RuntimeError("Delegated session store is not initialized")

        expires_at = datetime.fromtimestamp(expires_at_epoch_seconds, tz=UTC)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO delegated_sessions (
                    sid,
                    subject,
                    user_id,
                    session_id,
                    realm,
                    catalog,
                    audience,
                    refresh_token,
                    expires_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                ON CONFLICT (sid)
                DO UPDATE SET
                    subject = EXCLUDED.subject,
                    user_id = EXCLUDED.user_id,
                    session_id = EXCLUDED.session_id,
                    realm = EXCLUDED.realm,
                    catalog = EXCLUDED.catalog,
                    audience = EXCLUDED.audience,
                    refresh_token = EXCLUDED.refresh_token,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW()
                """,
                sid,
                subject,
                user_id,
                session_id,
                realm,
                catalog,
                audience,
                refresh_token,
                expires_at,
            )

    async def get_session(self, sid: str) -> dict | None:
        if self._pool is None:
            raise RuntimeError("Delegated session store is not initialized")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    sid,
                    subject,
                    user_id,
                    session_id,
                    realm,
                    catalog,
                    audience,
                    refresh_token,
                    expires_at
                FROM delegated_sessions
                WHERE sid = $1
                """,
                sid,
            )

        if row is None:
            return None

        expires_at = row["expires_at"]
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)

        return {
            "sid": row["sid"],
            "subject": row["subject"],
            "user_id": row["user_id"],
            "session_id": row["session_id"],
            "realm": row["realm"],
            "catalog": row["catalog"],
            "audience": row["audience"],
            "refresh_token": row["refresh_token"],
            "expires_at": int(expires_at.timestamp()),
        }

    async def update_refresh_token(self, sid: str, refresh_token: str):
        if self._pool is None:
            raise RuntimeError("Delegated session store is not initialized")

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE delegated_sessions
                SET refresh_token = $1, updated_at = NOW()
                WHERE sid = $2
                """,
                refresh_token,
                sid,
            )

    async def delete_session(self, sid: str):
        if self._pool is None:
            raise RuntimeError("Delegated session store is not initialized")

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM delegated_sessions
                WHERE sid = $1
                """,
                sid,
            )
