Alembic migrations for pricing-sdk

Usage:

1. Install dependencies:

```bash
pip install alembic sqlalchemy
```

2. Ensure `DATABASE_URL` is set (e.g. `postgresql://user:pass@host:5432/db`)

3. Apply migrations:

```bash
cd pricing-platform/pricing-sdk
alembic upgrade head
```

Notes:
- The initial migration runs `sql/schema.sql` to create tables.
- Runtime code continues using `asyncpg`; Alembic only manages schema.
