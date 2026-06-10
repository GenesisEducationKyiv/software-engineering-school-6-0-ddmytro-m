# Commit Rules

Use Conventional Commits with these types: `feat`, `fix`, `chore`, `test`.

The title must be short, clear, and self-explanatory on its own.
The body (extended description) describes what was actually done — which files/components were changed and how.

Never add Claude as a co-author.

## Format

```
type: short clear title

What was changed and how, in a few sentences or bullet points.
```

## Examples

**`05dd54d`** — wrapped `defer Close()` calls to handle errors, handled `AutoMigrate` return value, discarded `w.Write` returns in tests, refactored if/else chain to switch:
```
fix: handle all ignored error returns

- Wrapped defer redisClient.Close() and res.Body.Close() to log errors
- Checked AutoMigrate return value and panic on failure
- Discarded w.Write return values in tests with _, _
- Replaced if/else if/else chain with switch in subscription handler
```

**`6d2a654`** — introduced `internal/config/config.go` (singleton env config via godotenv) and `internal/infra/db/db.go` (GORM models for Repository and Subscription, singleton DB connection), added gorm/godotenv to go.mod:
```
feat: add env config and postgres db layer

Added internal/config with a godotenv-based singleton config (reads
.env.<APP_ENV> then .env). Added internal/infra/db with GORM models
for Repository and Subscription and a singleton connection that
auto-migrates on startup.
```