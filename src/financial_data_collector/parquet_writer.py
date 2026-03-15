import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, List


class ParquetWriterError(RuntimeError):
    pass


class ParquetWriter:
    def write(self, path: Path, rows: Iterable[Dict], batch_size: int = 10000) -> int:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ParquetWriterError("pyarrow is required for parquet export") from exc

        total_rows = 0
        parquet_writer = None
        batch: List[Dict] = []
        try:
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    table = pa.Table.from_pylist(batch)
                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(path.as_posix(), table.schema)
                    parquet_writer.write_table(table)
                    total_rows += len(batch)
                    batch = []
            if batch:
                table = pa.Table.from_pylist(batch)
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(path.as_posix(), table.schema)
                parquet_writer.write_table(table)
                total_rows += len(batch)
            if parquet_writer is None:
                empty_table = pa.table({})
                pq.write_table(empty_table, path.as_posix())
                return 0
            return total_rows
        finally:
            if parquet_writer is not None:
                parquet_writer.close()

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

