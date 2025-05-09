import shutil
from pathlib import Path
from typing import List

import nox

nox.options.sessions = ("lint", "tests")
nox.options.reuse_existing_virtualenvs = True

SRCS = ("src", "tests", "noxfile.py")


@nox.session(python=["3.12"])
def test(session):
    args = session.posargs or [
        "--cov=src",
        "--cov-report",
        "xml:coverage.xml",
        "--cov-report",
        "term",
        "--junitxml=junit.xml",
    ]
    session.run("poetry", "install", external=True)
    session.run("pytest", *args)


@nox.session(python=["3.12"])
def lint(session):
    args = session.posargs or SRCS
    session.run("poetry", "install", external=True)
    session.run("flake8", *args)
    session.run("pyright", *args)


@nox.session(python=["3.12"])
def format(session):
    args = session.posargs or SRCS
    session.run("poetry", "install", external=True)
    session.run("black", *args)
    session.run("isort", *args)


@nox.session(python=False)
def clean(_: nox.Session):
    """Cleans up the project by removing unnecessary files and directories"""

    def remove_dir(directory_path: Path):
        if directory_path.is_dir():
            shutil.rmtree(directory_path)
            print(f"Removed: {directory_path}")

    def remove_recursively(pattern: str, exclude_dirs: List[str] | None = None):
        if exclude_dirs is None:
            exclude_dirs = []

        def is_excluded(file: Path) -> bool:
            return any(dir in str(file) for dir in exclude_dirs)

        for file in Path(".").rglob(pattern):
            if not is_excluded(file):
                remove_dir(file)

    remove_dir(Path(".nox"))
    remove_dir(Path("dist"))
    remove_recursively("__pycache__", exclude_dirs=[".venv"])
    remove_recursively(".pytest_cache", exclude_dirs=[".venv"])
