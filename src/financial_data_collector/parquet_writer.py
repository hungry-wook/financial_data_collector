import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional


class ParquetWriterError(RuntimeError):
    pass


class ParquetWriter:
    TYPE_MAP = {
        "string": "string",
        "double": "float64",
        "int64": "int64",
        "bool": "bool",
        "list[string]": "list[string]",
    }

    def write(
        self,
        path: Path,
        rows: Iterable[Dict],
        batch_size: int = 10000,
        column_types: Optional[Dict[str, str]] = None,
    ) -> int:
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
                    table = self._build_table(pa, batch, column_types)
                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(path.as_posix(), table.schema)
                    parquet_writer.write_table(table)
                    total_rows += len(batch)
                    batch = []
            if batch:
                table = self._build_table(pa, batch, column_types)
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(path.as_posix(), table.schema)
                parquet_writer.write_table(table)
                total_rows += len(batch)
            if parquet_writer is None:
                empty_table = self._build_table(pa, [], column_types)
                pq.write_table(empty_table, path.as_posix())
                return 0
            return total_rows
        finally:
            if parquet_writer is not None:
                parquet_writer.close()

    @classmethod
    def _build_table(cls, pa, rows: List[Dict], column_types: Optional[Dict[str, str]]) -> "pa.Table":
        if not column_types:
            return pa.Table.from_pylist(rows)

        arrays = {}
        row_keys: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in row_keys:
                    row_keys.append(key)

        ordered_keys = list(column_types.keys()) + [key for key in row_keys if key not in column_types]
        for key in ordered_keys:
            values = [row.get(key) for row in rows]
            type_name = column_types.get(key)
            if type_name:
                arrays[key] = pa.array(values, type=cls._resolve_type(pa, type_name))
            else:
                arrays[key] = pa.array(values)
        return pa.table(arrays)

    @classmethod
    def _resolve_type(cls, pa, type_name: str):
        normalized = cls.TYPE_MAP.get(type_name, type_name)
        if normalized == "string":
            return pa.string()
        if normalized == "float64":
            return pa.float64()
        if normalized == "int64":
            return pa.int64()
        if normalized == "bool":
            return pa.bool_()
        if normalized == "list[string]":
            return pa.list_(pa.string())
        raise ValueError(f"unsupported parquet column type: {type_name}")

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
