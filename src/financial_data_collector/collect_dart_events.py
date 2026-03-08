import argparse
import json
from datetime import date, timedelta

from .dart_client import DARTClient, DARTClientConfig
from .settings import OpenDARTSettings, load_dotenv


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect OpenDART filing metadata (DS001 list API)")
    parser.add_argument("--bgn-de", help="begin date YYYY-MM-DD (default: today-7d)")
    parser.add_argument("--end-de", help="end date YYYY-MM-DD (default: today)")
    parser.add_argument("--corp-code", default="", help="DART corp_code")
    parser.add_argument("--pblntf-ty", default="B", help="Disclosure type (default: B, major report)")
    parser.add_argument("--page-no", type=int, default=1)
    parser.add_argument("--page-count", type=int, default=100)
    parser.add_argument("--last-reprt-at", default="Y", choices=["Y", "N"])
    args = parser.parse_args()

    load_dotenv(".env")
    settings = OpenDARTSettings.from_env()
    settings.validate()

    end_de = _parse_date(args.end_de) if args.end_de else date.today()
    bgn_de = _parse_date(args.bgn_de) if args.bgn_de else (end_de - timedelta(days=7))

    client = DARTClient(DARTClientConfig.from_settings(settings))
    payload = client.list_filings(
        bgn_de=bgn_de,
        end_de=end_de,
        corp_code=args.corp_code,
        pblntf_ty=args.pblntf_ty,
        page_no=args.page_no,
        page_count=args.page_count,
        last_reprt_at=args.last_reprt_at,
    )

    print(
        json.dumps(
            {
                "status": payload.get("status"),
                "message": payload.get("message"),
                "page_no": payload.get("page_no"),
                "page_count": payload.get("page_count"),
                "total_count": payload.get("total_count"),
                "returned": len(payload.get("list", []) or []),
                "sample": (payload.get("list", []) or [])[:5],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
