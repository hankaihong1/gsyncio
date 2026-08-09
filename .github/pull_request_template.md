# Pull Request

## Description of changes

<!-- Describe what you changed and why. Include *what* and *why*, not just *how*. -->

## Type

- [ ] bugfix
- [ ] feature
- [ ] docs
- [ ] chore

> **Labels**: after opening the PR, add the matching label
> (`feature` / `fix` / `docs` / `chore` / `breaking`) — Release Drafter
> groups PRs by label and derives the next version number from them.

## Checklist

<!--
Before submitting, make sure the pre-submission checklist passes.
See CONTRIBUTING.md -> "Pre-submission checklist" for details.
-->

- [ ] Tests pass (`uv run pytest -x -m "not slow"`)
- [ ] Lint passes (`uv run ruff check .`)
- [ ] Type check passes (`uv run mypy --strict src/gsyncio`)
- [ ] Rust clippy passes (`cargo clippy -- -D warnings`)
- [ ] Rust fmt (`cargo fmt --check`)
- [ ] CHANGELOG updated (if applicable)
- [ ] EN/ZH docs synced (`README.md` ↔ `README_ZH.md`, `docs/*.md` ↔ `docs/*_ZH.md` — mirrors must stay identical in structure)

## Related issue

Closes #<issue>
