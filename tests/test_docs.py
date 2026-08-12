"""Documentation consistency tests: public API coverage + doc code-block syntax.

Two responsibilities:

1. Every public symbol in ``gsyncio.__all__`` must have a ``### `X``` entry in
   docs/API.md — prevents "new API without docs" drift.
2. All python code blocks in README / docs / examples must pass ``ast.parse``
   — prevents stale example syntax.  Syntax-level check only, no execution
   (timing-sensitive examples would be flaky).
"""

from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent

# API.md entry headings look like: ### `FastChannel` (the Quick Examples
# section uses a "### 1. `X`" prefix)
_API_SYMBOL_RE = r"^###\s+(?:\d+\.\s+)?`([A-Za-z_]\w*)`"


def _extract_api_symbols(path: pathlib.Path) -> set[str]:
    """Extract documented symbol names of the form ``### `Name``` from markdown."""
    import re

    return set(re.findall(_API_SYMBOL_RE, path.read_text(encoding="utf-8"), re.MULTILINE))


def _extract_python_blocks(path: pathlib.Path) -> list[str]:
    """Extract all ```python fenced code blocks from markdown."""
    import re

    text = path.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, re.DOTALL)


def test_all_public_symbols_documented() -> None:
    """Every symbol in __all__ must have a docs/API.md entry (__version__ is
    exempt — it is a build constant, not an API symbol)."""
    import gsyncio

    documented = _extract_api_symbols(ROOT / "docs" / "API.md")
    missing = sorted(set(gsyncio.__all__) - documented - {"__version__"})
    assert not missing, f"docs/API.md is missing documentation entries for: {missing}"


def test_doc_code_blocks_parse() -> None:
    """All python code blocks in README / docs must be syntactically valid."""
    for name in ("README.md", "docs/API.md", "docs/CHOOSING.md", "docs/CONCURRENCY.md"):
        path = ROOT / name
        for i, block in enumerate(_extract_python_blocks(path)):
            try:
                ast.parse(block)
            except SyntaxError as exc:
                raise AssertionError(
                    f"{name} code block #{i + 1} has a syntax error: {exc}"
                ) from exc


def test_examples_parse() -> None:
    """examples/*.py must be syntactically valid (same anti-drift check level as docs)."""
    examples = sorted((ROOT / "examples").glob("*.py"))
    assert examples, "no .py files under examples/"
    for path in examples:
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            raise AssertionError(f"{path.name} has a syntax error: {exc}") from exc


# ---------------------------------------------------------------------------
# EN/ZH mirror consistency
# ---------------------------------------------------------------------------

# (English file, Chinese mirror) pairs; structure must be identical (translation
# changes only the language, never the structure)
_MIRROR_PAIRS = [
    ("README.md", "README_ZH.md"),
    ("docs/API.md", "docs/API_ZH.md"),
    ("docs/CHOOSING.md", "docs/CHOOSING_ZH.md"),
    ("docs/CONCURRENCY.md", "docs/CONCURRENCY_ZH.md"),
]


def _count_headings(path: pathlib.Path, level: int) -> list[str]:
    """Extract heading texts of the given level ('#' count).  Matching the rule
    with a trailing space (e.g. ``## ``) avoids `###` being mis-matched by a
    lower-level rule."""
    prefix = "#" * level
    return [
        line.strip()[len(prefix) :].strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith(prefix + " ")
    ]


def test_api_mirror_symbols_match() -> None:
    """The ### `X` symbol sets of API.md and API_ZH.md must be identical (symbols are not translated)."""
    en = _extract_api_symbols(ROOT / "docs" / "API.md")
    zh = _extract_api_symbols(ROOT / "docs" / "API_ZH.md")
    assert en == zh, (
        f"API mirror symbol sets differ: only-EN={sorted(en - zh)}, only-ZH={sorted(zh - en)}"
    )


def test_doc_mirror_headings_count_match() -> None:
    """Each mirror pair must have the same ## / ### heading count (guards against
    forgetting the mirror when adding/removing large sections)."""
    for en_name, zh_name in _MIRROR_PAIRS:
        en_path, zh_path = ROOT / en_name, ROOT / zh_name
        for level in (2, 3):
            en_n = len(_count_headings(en_path, level))
            zh_n = len(_count_headings(zh_path, level))
            assert en_n == zh_n, (
                f"{en_name} vs {zh_name}: {'#' * level} heading count differs: EN={en_n}, ZH={zh_n}"
            )
