---
paths: database/**/*.sql, backend/migrations/**/*.sql
---

# Database Schema Rules

## SQL Style
- Use UPPERCASE for SQL keywords (SELECT, FROM, WHERE)
- Use lowercase for table and column names
- Use snake_case for naming
- Add appropriate indexes for frequently queried columns

## PostgreSQL Compatibility
- Use SERIAL for auto-increment primary keys
- Use TIMESTAMP for date/time columns
- Use TEXT instead of VARCHAR for strings
- Use appropriate data types (INTEGER, BOOLEAN, JSONB)

## Migrations
- Name migrations with version prefix: `001_initial.sql`, `002_add_feature.sql`
- Include both up and down migrations when possible
- Test migrations on SQLite before deploying to PostgreSQL
