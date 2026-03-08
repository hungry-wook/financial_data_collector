import argparse
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from .dart_client import DARTClient, DARTClientConfig
from .dart_event_parser import extract_text_from_document_zip, infer_event_status, infer_raw_factor
from .repository import Repository
from .settings import OpenDARTSettings, load_dotenv


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _map_event_type(report_nm: str) -> Optional[str]:
    text = str(report_nm or "")
    mapping = [
        ("유무상증자", "RIGHTS_BONUS_ISSUE"),
        ("무상증자", "BONUS_ISSUE"),
        ("유상증자", "RIGHTS_ISSUE"),
        ("감자", "CAPITAL_REDUCTION"),
        ("분할합병", "SPLIT_MERGER"),
        ("회사합병", "MERGER"),
        ("회사분할", "SPLIT"),
        ("주식교환", "STOCK_SWAP"),
        ("주식이전", "STOCK_TRANSFER"),
    ]
    for key, value in mapping:
        if key in text:
            return value
    return None


def _normalize_report_name(report_nm: str) -> str:
    text = str(report_nm or "").strip()
    text = re.sub(r"^\s*\[[^\]]+\]\s*", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _filing_effective_date(filing: Dict) -> Optional[str]:
    rcept_dt = str(filing.get("rcept_dt") or "").strip()
    if len(rcept_dt) == 8 and rcept_dt.isdigit():
        return f"{rcept_dt[0:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}"
    return None


def _safe_float(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _extract_ratio_pair(text: str) -> Optional[Tuple[float, float]]:
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)", str(text or ""))
    if not m:
        return None
    try:
        left = float(m.group(1))
        right = float(m.group(2))
    except ValueError:
        return None
    if left <= 0 or right < 0:
        return None
    return left, right


def _pick_latest_revisions(filings: List[Dict]) -> Dict[str, str]:
    latest_by_key: Dict[Tuple[str, str, str], Dict] = {}
    for filing in filings:
        event_type = _map_event_type(filing.get("report_nm"))
        if not event_type:
            continue
        corp_code = str(filing.get("corp_code") or "").strip()
        norm_name = _normalize_report_name(filing.get("report_nm"))
        key = (corp_code, event_type, norm_name)
        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = filing
            continue
        cur_no = str(current.get("rcept_no") or "")
        new_no = str(filing.get("rcept_no") or "")
        if new_no > cur_no:
            latest_by_key[key] = filing
    return {str(v.get("rcept_no") or ""): "KEEP" for v in latest_by_key.values()}


def _ds005_window_from_rcept_dt(rcept_dt: str) -> Tuple[date, date]:
    if len(rcept_dt) == 8 and rcept_dt.isdigit():
        d = date(int(rcept_dt[:4]), int(rcept_dt[4:6]), int(rcept_dt[6:8]))
    else:
        d = date.today()
    return (d - timedelta(days=45), d + timedelta(days=45))


def _extract_ds005_row(client: DARTClient, event_type: str, corp_code: str, rcept_no: str, rcept_dt: str) -> Tuple[Dict, str]:
    if not corp_code:
        return {}, "NO_CORP_CODE"
    bgn_de, end_de = _ds005_window_from_rcept_dt(rcept_dt)
    try:
        if event_type == "BONUS_ISSUE":
            payload = client.get_bonus_issue_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "RIGHTS_ISSUE":
            payload = client.get_rights_issue_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "RIGHTS_BONUS_ISSUE":
            payload = client.get_rights_bonus_issue_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "CAPITAL_REDUCTION":
            payload = client.get_capital_reduction_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "MERGER":
            payload = client.get_merger_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "SPLIT":
            payload = client.get_split_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type == "SPLIT_MERGER":
            payload = client.get_split_merger_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        elif event_type in {"STOCK_SWAP", "STOCK_TRANSFER"}:
            payload = client.get_stock_swap_disclosures(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        else:
            return {}, "UNSUPPORTED_EVENT"
    except Exception:
        return {}, "REQUEST_FAIL"

    rows = payload.get("list", []) or []
    if not rows:
        return {}, "NO_DATA"

    for row in rows:
        if str(row.get("rcept_no") or "").strip() == rcept_no:
            return row, "EXACT"

    if len(rows) == 1:
        return rows[0], "SINGLE_FALLBACK"

    return rows[0], "FIRST_FALLBACK"


def _infer_ds005_factor(event_type: str, ds005_row: Dict) -> Optional[float]:
    if not ds005_row:
        return None

    if event_type == "BONUS_ISSUE":
        per_share = _safe_float(ds005_row.get("nstk_asstd"))
        if per_share is None:
            per_share = _safe_float(ds005_row.get("nstk_ascnt_ps_ostk")) or _safe_float(ds005_row.get("nstk_ascnt_ps_estk"))
        if per_share is not None and 0 < per_share < 10:
            return 1.0 / (1.0 + per_share)

        old_shares = _safe_float(ds005_row.get("bfic_tisstk_ostk")) or _safe_float(ds005_row.get("bfic_tisstk_estk"))
        new_shares = _safe_float(ds005_row.get("nstk_ostk_cnt")) or _safe_float(ds005_row.get("nstk_estk_cnt"))
        if new_shares and old_shares and new_shares > 0 and old_shares > 0:
            return old_shares / (old_shares + new_shares)

    if event_type in {"RIGHTS_ISSUE", "RIGHTS_BONUS_ISSUE"}:
        old_shares = _safe_float(ds005_row.get("bfic_tisstk_ostk")) or _safe_float(ds005_row.get("bfic_tisstk_estk"))
        new_shares = _safe_float(ds005_row.get("nstk_ostk_cnt")) or _safe_float(ds005_row.get("nstk_estk_cnt"))
        if new_shares and old_shares and new_shares > 0 and old_shares > 0:
            return old_shares / (old_shares + new_shares)

    if event_type == "CAPITAL_REDUCTION":
        before = _safe_float(ds005_row.get("bfcr_tisstk_ostk")) or _safe_float(ds005_row.get("bfcr_tisstk_estk"))
        after = _safe_float(ds005_row.get("atcr_tisstk_ostk")) or _safe_float(ds005_row.get("atcr_tisstk_estk"))
        if before and after and before > 0 and after > 0:
            return before / after
        for key in ("cr_rt_ostk", "cr_rt_estk", "cvrt_ratio", "crrt", "red_rt"):
            p = _safe_float(ds005_row.get(key))
            if p is not None and 0 < p < 100:
                pct = p * 100.0 if p < 1 else p
                remain = 1.0 - (pct / 100.0)
                if remain > 0:
                    return 1.0 / remain

    if event_type == "SPLIT":
        before = _safe_float(ds005_row.get("abcr_nstkascnd"))
        after = _safe_float(ds005_row.get("abcr_nstkasstd"))
        if before and after and before > 0 and after > 0:
            return before / after

    if event_type == "MERGER":
        pair = _extract_ratio_pair(ds005_row.get("mg_rt") or ds005_row.get("ex_sm_r") or "")
        if pair and pair[1] == 0:
            return 1.0

    if event_type == "SPLIT_MERGER":
        ratio_text = str(ds005_row.get("dvmg_rt") or ds005_row.get("dvmg_rt_bs") or ds005_row.get("abcr_shstkcnt_rt_at_rs") or "")
        pair = _extract_ratio_pair(ratio_text)
        if pair and pair[0] > 0 and pair[1] > 0 and abs(pair[0] - pair[1]) < 1e-9:
            return 1.0
        if "??? ????? ???? ???????" in ratio_text and "1:1" in ratio_text:
            return 1.0

    if event_type in {"STOCK_SWAP", "STOCK_TRANSFER"}:
        pair = _extract_ratio_pair(ds005_row.get("extr_rt") or ds005_row.get("ex_sm_r") or "")
        if pair and pair[1] == 0:
            return 1.0

    return None


def _chain_lookup_window_from_rcept_dt(rcept_dt: str) -> Tuple[date, date]:
    if len(rcept_dt) == 8 and rcept_dt.isdigit():
        d = date(int(rcept_dt[:4]), int(rcept_dt[4:6]), int(rcept_dt[6:8]))
    else:
        d = date.today()
    return (d - timedelta(days=540), d + timedelta(days=30))


def _recover_factor_from_remote_chain(
    client: DARTClient,
    event_type: str,
    corp_code: str,
    revision_anchor: str,
    current_rcept_no: str,
    rcept_dt: str,
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if not corp_code or not revision_anchor or not hasattr(client, 'list_filings'):
        return None, None, None

    bgn_de, end_de = _chain_lookup_window_from_rcept_dt(rcept_dt)
    filings: List[Dict] = []
    for page_no in range(1, 6):
        payload = client.list_filings(
            bgn_de=bgn_de,
            end_de=end_de,
            corp_code=corp_code,
            pblntf_ty='B',
            page_no=page_no,
            page_count=100,
            last_reprt_at='N',
        )
        page_filings = payload.get('list', []) or []
        filings.extend(page_filings)
        total_count = int(payload.get('total_count') or 0)
        if page_no * 100 >= total_count or not page_filings:
            break

    candidates = []
    for filing in filings:
        candidate_event_type = _map_event_type(filing.get('report_nm'))
        if candidate_event_type != event_type:
            continue
        if _normalize_report_name(filing.get('report_nm')) != revision_anchor:
            continue
        candidate_rcept_no = str(filing.get('rcept_no') or '').strip()
        if not candidate_rcept_no or candidate_rcept_no >= current_rcept_no:
            continue
        candidates.append(filing)

    for filing in sorted(candidates, key=lambda row: str(row.get('rcept_no') or ''), reverse=True):
        candidate_rcept_no = str(filing.get('rcept_no') or '').strip()
        candidate_rcept_dt = str(filing.get('rcept_dt') or '').strip()
        ds005_row, ds005_match_type = _extract_ds005_row(
            client=client,
            event_type=event_type,
            corp_code=corp_code,
            rcept_no=candidate_rcept_no,
            rcept_dt=candidate_rcept_dt,
        )
        ds005_factor = _infer_ds005_factor(event_type, ds005_row)
        if ds005_factor and ds005_factor > 0:
            return ds005_factor, f'remote_chain_ds005_{ds005_match_type.lower()}', candidate_rcept_no

        try:
            doc_zip = client.get_document_zip(candidate_rcept_no)
            doc_text = extract_text_from_document_zip(doc_zip)
        except Exception:
            continue

        disposition = infer_event_status(event_type, doc_text)
        if disposition and disposition[0] == 'REJECTED':
            continue

        raw_factor, factor_rule = infer_raw_factor(event_type, doc_text)
        if raw_factor and raw_factor > 0:
            return raw_factor, f'remote_chain_{factor_rule}', candidate_rcept_no

    return None, None, None


def _resolve_instrument_id(
    repo: Repository,
    filing: Dict,
    corp_to_stock: Dict[str, str],
) -> Tuple[Optional[str], str]:
    stock_code = str(filing.get("stock_code") or "").strip()
    corp_code = str(filing.get("corp_code") or "").strip()
    market_code = None
    corp_cls = str(filing.get("corp_cls") or "").strip().upper()
    if corp_cls == "K":
        market_code = "KOSDAQ"
    elif corp_cls == "Y":
        market_code = "KOSPI"

    if stock_code:
        instrument_id = repo.get_instrument_id_by_external_code(stock_code, market_code=market_code)
        if instrument_id:
            return instrument_id, "stock_code_market"
        instrument_id = repo.get_instrument_id_by_external_code(stock_code, market_code=None)
        if instrument_id:
            return instrument_id, "stock_code_any_market"

    if corp_code:
        mapped_stock = str(corp_to_stock.get(corp_code) or "").strip()
        if mapped_stock:
            instrument_id = repo.get_instrument_id_by_external_code(mapped_stock, market_code=market_code)
            if instrument_id:
                return instrument_id, "corp_code_master_market"
            instrument_id = repo.get_instrument_id_by_external_code(mapped_stock, market_code=None)
            if instrument_id:
                return instrument_id, "corp_code_master_any_market"

        instrument_id = repo.get_instrument_id_by_corp_code_history(corp_code)
        if instrument_id:
            return instrument_id, "corp_code_event_history"

    return None, "not_found"


def collect_corporate_events(
    repo: Repository,
    client: DARTClient,
    bgn_de: date,
    end_de: date,
    pblntf_ty: str = "B",
    last_reprt_at: str = "Y",
    max_pages: int = 20,
    verify_document: bool = True,
) -> Dict[str, int]:
    now = _utc_now_iso()
    all_filings: List[Dict] = []

    for page_no in range(1, max_pages + 1):
        payload = client.list_filings(
            bgn_de=bgn_de,
            end_de=end_de,
            pblntf_ty=pblntf_ty,
            page_no=page_no,
            page_count=100,
            last_reprt_at=last_reprt_at,
        )
        page_filings = payload.get("list", []) or []
        all_filings.extend(page_filings)
        total_count = int(payload.get("total_count") or 0)
        if page_no * 100 >= total_count:
            break
        if not page_filings:
            break

    keep_rcept = _pick_latest_revisions(all_filings)

    corp_to_stock: Dict[str, str] = {}
    if hasattr(client, "get_corp_codes"):
        try:
            corp_to_stock = client.get_corp_codes()
        except Exception:
            corp_to_stock = {}

    events: List[Dict] = []
    validations: List[Dict] = []
    superseded_source_ids: List[str] = []
    chain_factor_cache: Dict[Tuple[str, str, str], float] = {}

    for filing in sorted(all_filings, key=lambda x: str(x.get("rcept_no") or "")):
        report_nm = filing.get("report_nm")
        event_type = _map_event_type(report_nm)
        if not event_type:
            continue

        rcept_no = str(filing.get("rcept_no") or "").strip()
        corp_code = str(filing.get("corp_code") or "").strip()
        rcept_dt = str(filing.get("rcept_dt") or "").strip()
        revision_anchor = _normalize_report_name(report_nm)
        chain_key = (corp_code, event_type, revision_anchor)

        if rcept_no and keep_rcept.get(rcept_no) != "KEEP":
            superseded_source_ids.append(rcept_no)
            validations.append(
                {
                    "source_event_id": rcept_no,
                    "check_name": "REVISION_CHAIN",
                    "result": "SKIP",
                    "detail": "Superseded revision in current collection window",
                    "validated_at": now,
                }
            )
            continue

        instrument_id, mapping_source = _resolve_instrument_id(repo, filing, corp_to_stock)
        if not instrument_id:
            validations.append(
                {
                    "source_event_id": rcept_no,
                    "check_name": "INSTRUMENT_MAPPING",
                    "result": "PARSE_FAIL",
                    "detail": (
                        f"No instrument mapping for stock_code={str(filing.get('stock_code') or '').strip()}, "
                        f"corp_code={corp_code}, market={str(filing.get('corp_cls') or '').strip()}, "
                        f"source={mapping_source}"
                    ),
                    "validated_at": now,
                }
            )
            continue

        effective_date = _filing_effective_date(filing)
        raw_factor = None
        factor_rule = "not_verified"
        status = "NEEDS_REVIEW"
        confidence = "LOW"

        ds005_row, ds005_match_type = _extract_ds005_row(
            client=client,
            event_type=event_type,
            corp_code=corp_code,
            rcept_no=rcept_no,
            rcept_dt=rcept_dt,
        )
        ds005_factor = _infer_ds005_factor(event_type, ds005_row)
        if ds005_factor and ds005_factor > 0:
            raw_factor = ds005_factor
            factor_rule = f"ds005_{ds005_match_type.lower()}"
            status = "ACTIVE"
            confidence = "HIGH"
            validations.append(
                {
                    "source_event_id": rcept_no,
                    "check_name": "DS005_FACTOR",
                    "result": "MATCH",
                    "detail": f"Parsed factor={raw_factor:.10f} from DS005 ({ds005_match_type})",
                    "validated_at": now,
                }
            )
        elif ds005_row:
            ds005_expected = event_type in {"BONUS_ISSUE", "RIGHTS_ISSUE", "RIGHTS_BONUS_ISSUE", "CAPITAL_REDUCTION", "SPLIT"}
            validations.append(
                {
                    "source_event_id": rcept_no,
                    "check_name": "DS005_FACTOR",
                    "result": "PARSE_FAIL" if ds005_expected else "SKIP",
                    "detail": (
                        f"DS005 row found but no parsable factor ({ds005_match_type})"
                        if ds005_expected
                        else f"DS005 row found for {event_type} but factor not modeled"
                    ),
                    "validated_at": now,
                }
            )

        if verify_document and rcept_no and raw_factor is None:
            try:
                doc_zip = client.get_document_zip(rcept_no)
                doc_text = extract_text_from_document_zip(doc_zip)
                raw_factor, factor_rule = infer_raw_factor(event_type, doc_text)
                if raw_factor and raw_factor > 0:
                    status = "ACTIVE"
                    confidence = "MEDIUM"
                    validations.append(
                        {
                            "source_event_id": rcept_no,
                            "check_name": "DOCUMENT_FACTOR_PARSE",
                            "result": "MATCH",
                            "detail": f"Parsed factor={raw_factor:.10f} using rule={factor_rule}",
                            "validated_at": now,
                        }
                    )
                else:
                    disposition = infer_event_status(event_type, doc_text)
                    if disposition:
                        status, factor_rule = disposition
                        confidence = "HIGH"
                        validations.append(
                            {
                                "source_event_id": rcept_no,
                                "check_name": "DOCUMENT_EVENT_STATUS",
                                "result": "MATCH",
                                "detail": f"Classified event as {status} using rule={factor_rule}",
                                "validated_at": now,
                            }
                        )
                    else:
                        validations.append(
                            {
                                "source_event_id": rcept_no,
                                "check_name": "DOCUMENT_FACTOR_PARSE",
                                "result": "PARSE_FAIL",
                                "detail": f"No factor parsed (rule={factor_rule})",
                                "validated_at": now,
                            }
                        )
            except Exception as exc:
                validations.append(
                    {
                        "source_event_id": rcept_no,
                        "check_name": "DOCUMENT_DOWNLOAD",
                        "result": "PARSE_FAIL",
                        "detail": str(exc),
                        "validated_at": now,
                    }
                )

        if raw_factor is None and status == "NEEDS_REVIEW":
            remote_factor, remote_rule, remote_source = _recover_factor_from_remote_chain(
                client=client,
                event_type=event_type,
                corp_code=corp_code,
                revision_anchor=revision_anchor,
                current_rcept_no=rcept_no,
                rcept_dt=rcept_dt,
            )
            if remote_factor is not None and remote_factor > 0:
                raw_factor = remote_factor
                factor_rule = remote_rule or "remote_chain_factor"
                status = "ACTIVE"
                confidence = "MEDIUM"
                validations.append(
                    {
                        "source_event_id": rcept_no,
                        "check_name": "REMOTE_REVISION_CHAIN",
                        "result": "MATCH",
                        "detail": f"Recovered factor={raw_factor:.10f} from prior filing {remote_source}",
                        "validated_at": now,
                    }
                )

        if raw_factor is None and status == "NEEDS_REVIEW":
            inherited = chain_factor_cache.get(chain_key)
            if inherited is None:
                inherited = repo.get_latest_factor_for_chain(corp_code=corp_code, event_type=event_type, revision_anchor=revision_anchor)
            if inherited is not None and inherited > 0:
                raw_factor = inherited
                factor_rule = "inherited_revision_factor"
                status = "ACTIVE"
                confidence = "LOW"
                validations.append(
                    {
                        "source_event_id": rcept_no,
                        "check_name": "REVISION_FACTOR_INHERIT",
                        "result": "MATCH",
                        "detail": f"Inherited factor={raw_factor:.10f} for revision chain",
                        "validated_at": now,
                    }
                )

        if raw_factor is not None and raw_factor > 0:
            chain_factor_cache[chain_key] = raw_factor

        event_id = f"dart:{rcept_no}:{event_type}"
        payload = dict(filing)
        payload["factor_rule"] = factor_rule
        payload["mapping_source"] = mapping_source
        payload["revision_anchor"] = revision_anchor
        payload["ds005_match_type"] = ds005_match_type
        if ds005_row:
            payload["ds005_row"] = ds005_row

        events.append(
            {
                "event_id": event_id,
                "event_version": 1,
                "instrument_id": instrument_id,
                "event_type": event_type,
                "announce_date": effective_date,
                "effective_date": effective_date,
                "source_event_id": rcept_no,
                "source_name": "opendart",
                "collected_at": now,
                "raw_factor": raw_factor,
                "confidence": confidence,
                "status": status,
                "payload": payload,
            }
        )

    refresh_source_ids = []
    seen_source_ids = set()
    for source_id in superseded_source_ids + [str(e.get("source_event_id") or "").strip() for e in events]:
        source_id = str(source_id or "").strip()
        if not source_id or source_id in seen_source_ids:
            continue
        seen_source_ids.add(source_id)
        refresh_source_ids.append(source_id)

    deleted_superseded = repo.delete_corporate_events_by_source_ids(refresh_source_ids)
    event_count = repo.upsert_corporate_events(events)
    validation_count = repo.insert_event_validation_results(validations)

    return {
        "filings_seen": len(all_filings),
        "events_upserted": event_count,
        "validations_inserted": validation_count,
        "active_events": len([e for e in events if e["status"] == "ACTIVE"]),
        "needs_review_events": len([e for e in events if e["status"] != "ACTIVE"]),
        "superseded_deleted": deleted_superseded,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect OpenDART filings into corporate_events")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--bgn-de", help="begin date YYYY-MM-DD (default: today-7d)")
    parser.add_argument("--end-de", help="end date YYYY-MM-DD (default: today)")
    parser.add_argument("--pblntf-ty", default="B")
    parser.add_argument("--last-reprt-at", default="Y", choices=["Y", "N"])
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--skip-document-verify", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")

    load_dotenv(".env")
    settings = OpenDARTSettings.from_env()
    settings.validate()

    end_de = _parse_date(args.end_de) if args.end_de else date.today()
    bgn_de = _parse_date(args.bgn_de) if args.bgn_de else (end_de - timedelta(days=7))

    client = DARTClient(DARTClientConfig.from_settings(settings))
    repo = Repository(args.database_url)
    repo.init_schema()

    result = collect_corporate_events(
        repo=repo,
        client=client,
        bgn_de=bgn_de,
        end_de=end_de,
        pblntf_ty=args.pblntf_ty,
        last_reprt_at=args.last_reprt_at,
        max_pages=args.max_pages,
        verify_document=not args.skip_document_verify,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


