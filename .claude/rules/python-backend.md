---
paths: backend/**/*.py
---

# Python Backend Rules

## Code Style
- Use 4-space indentation
- Follow PEP 8 naming conventions
- Add type hints for function parameters and return values
- Use docstrings for public functions

## Database
- Always use parameterized queries to prevent SQL injection
- Use the `DatabaseConnection` context manager from `database.py`
- Handle both SQLite and PostgreSQL syntax differences
- Close database connections properly

## API Endpoints
- Use appropriate HTTP methods (GET, POST, PUT, DELETE)
- Return proper status codes (200, 201, 400, 401, 404, 500)
- Include descriptive error messages in responses
- Validate input data before processing

## Security
- Never log sensitive data (passwords, API keys, tokens)
- Hash passwords with bcrypt before storing
- Validate Bearer tokens for protected endpoints
