import json

from financial_data_collector.export_service import ExportService
from financial_data_collector.sample_backtest_run import SampleRunConfig, run_backtest_sample


class FakeParquetWriter:
    def write(self, path, rows):
        path.write_text(json.dumps(rows), encoding="utf-8")
        return len(rows)

    def sha256(self, path):
        return "fakehash"

    def write_manifest(self, path, manifest):
        path.write_text(json.dumps(manifest), encoding="utf-8")


def test_sample_backtest_run_generates_dataset(tmp_path, repo):
    out_path = tmp_path / "out"
    result = run_backtest_sample(
        SampleRunConfig(database_url=repo.database_url, output_path=out_path.as_posix()),
        export_service=ExportService(
            repo=repo,
            writer=FakeParquetWriter(),
        ),
    )

    assert result["result"]["status"] == "SUCCEEDED"
    assert (out_path / "instrument_daily.parquet").exists()
    assert (out_path / "benchmark_daily.parquet").exists()
    assert (out_path / "trading_calendar.parquet").exists()
    assert (out_path / "manifest.json").exists()
