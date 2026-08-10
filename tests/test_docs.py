"""文档一致性测试：公开 API 覆盖 + 文档代码块语法。

两个职责：
1. ``gsyncio.__all__`` 中每个公开符号必须在 docs/API.md 有 ``### `X``` 条目
   ——防止"加了新 API 忘了写文档"的漂移。
2. README / docs / examples 中所有 python 代码块必须通过 ``ast.parse``
   ——防止示例语法过期。只做语法级检查，不做运行级（时序敏感会 flaky）。
"""

import ast
import pathlib
import re

import gsyncio

ROOT = pathlib.Path(__file__).resolve().parent.parent

_DOC_FILES = (
    "README.md",
    "README_ZH.md",
    "docs/API.md",
    "docs/API_ZH.md",
    "docs/CHOOSING.md",
    "docs/CHOOSING_ZH.md",
    "docs/CONCURRENCY.md",
    "docs/CONCURRENCY_ZH.md",
)

# API.md 的条目标题形如：### `FastChannel`（Quick Examples 区有 "### 1. `X`" 前缀）
_HEADER_RE = re.compile(r"^###\s+(?:\d+\.\s+)?`([A-Za-z_]\w*)`", re.MULTILINE)


def _extract_api_symbols(path: pathlib.Path) -> set[str]:
    """提取 markdown 中 ``### `Name``` 形式的文档化符号名。"""
    return set(_HEADER_RE.findall(path.read_text(encoding="utf-8")))


def _extract_python_blocks(path: pathlib.Path) -> list[str]:
    """提取 markdown 中所有 ```python 围栏代码块。"""
    blocks: list[str] = []
    in_block = False
    buf: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("```python"):
            in_block = True
            buf = []
            continue
        if line.strip() == "```" and in_block:
            in_block = False
            blocks.append("\n".join(buf))
            continue
        if in_block:
            buf.append(line)
    return blocks


def test_all_public_symbols_documented() -> None:
    """__all__ 中每个公开符号必须在 docs/API.md 有文档条目。"""
    documented = _extract_api_symbols(ROOT / "docs" / "API.md")
    missing = sorted(set(gsyncio.__all__) - documented - {"__version__"})
    assert not missing, f"docs/API.md 缺少以下符号的文档条目: {missing}"


def test_doc_code_blocks_parse() -> None:
    """README / docs 中所有 python 代码块必须语法合法。"""
    for name in _DOC_FILES:
        path = ROOT / name
        blocks = _extract_python_blocks(path)
        # 没有 python 块的文档（如纯命令说明）允许跳过
        for i, block in enumerate(blocks):
            try:
                ast.parse(block)
            except SyntaxError as exc:
                raise AssertionError(f"{name} 第 {i + 1} 个代码块语法错误: {exc}") from exc


def test_examples_parse() -> None:
    """examples/*.py 必须语法合法（与文档同级的防漂移检查）。"""
    examples = sorted((ROOT / "examples").glob("*.py"))
    assert examples, "examples/ 下没有 .py 文件"
    for path in examples:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


# ── 中英镜像一致性 ──────────────────────────────────────────────

# (英文文件, 中文镜像) 配对；结构必须一致（翻译只改语言，不改结构）
_MIRROR_PAIRS = [
    ("README.md", "README_ZH.md"),
    ("docs/API.md", "docs/API_ZH.md"),
    ("docs/CHOOSING.md", "docs/CHOOSING_ZH.md"),
    ("docs/CONCURRENCY.md", "docs/CONCURRENCY_ZH.md"),
]


def _count_headings(path: pathlib.Path, level: int) -> list[str]:
    """提取指定级别（'#' 数量）的标题文本。匹配带尾随空格的规则
    （如 `## `）可避免 `###` 被低级别规则误匹配。"""
    prefix = "#" * level
    return [
        line.strip()[len(prefix) :].strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith(prefix + " ")
    ]


def test_api_mirror_symbols_match() -> None:
    """API.md 与 API_ZH.md 的 ### `X` 符号集合必须一致（符号不翻译）。"""
    en = _extract_api_symbols(ROOT / "docs" / "API.md")
    zh = _extract_api_symbols(ROOT / "docs" / "API_ZH.md")
    assert en == zh, f"API 镜像符号集不一致: 仅英文={sorted(en - zh)}, 仅中文={sorted(zh - en)}"


def test_doc_mirror_headings_count_match() -> None:
    """每对镜像的 ## / ### 标题数量必须一致（防大段增删忘镜像）。"""
    for en_name, zh_name in _MIRROR_PAIRS:
        en_path, zh_path = ROOT / en_name, ROOT / zh_name
        for level in (2, 3):
            en_n = len(_count_headings(en_path, level))
            zh_n = len(_count_headings(zh_path, level))
            assert en_n == zh_n, (
                f"{en_name} 与 {zh_name} 的 {'#' * level} 标题数不一致: 英文={en_n}, 中文={zh_n}"
            )
