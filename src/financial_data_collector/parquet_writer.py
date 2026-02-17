import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, List


class ParquetWriterError(RuntimeError):
    pass


class ParquetWriter:
    def write(self, path: Path, rows: List[Dict]) -> int:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ParquetWriterError("pyarrow is required for parquet export") from exc

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path.as_posix())
        return len(rows)

    @staticmethod
    def sha256(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def write_manifest(path: Path, manifest: Dict) -> None:
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

