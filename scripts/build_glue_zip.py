"""Bundle pure Python modules for Glue (DuckDB is only imported by local modules)."""

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(__file__).resolve().parents[1]
destination = root / "dist" / "logistics_platform.zip"
destination.parent.mkdir(exist_ok=True)
with ZipFile(destination, "w", ZIP_DEFLATED) as archive:
    for path in sorted((root / "src" / "logistics_platform").rglob("*")):
        if path.is_file() and path.suffix in {".py", ".sql"}:
            archive.write(path, path.relative_to(root / "src"))
print(destination)
