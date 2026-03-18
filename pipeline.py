"""Ingest EDINET filings into the local SQLite database.

If a filing index exists (built during seed.py), documents are fetched
directly by ID — no date scanning required.

Usage:
    python pipeline.py 7203                    # Doc 120 (financials + shareholders)
    python pipeline.py 7203 --doc-type 180     # Extraordinary reports
    python pipeline.py 7203 --doc-type 220     # Buyback reports
    python pipeline.py 7203 --all              # All supported doc types
    python pipeline.py 7203 6758 8035          # Multiple tickers
"""
import argparse
import json
import os
import sys

from dotenv import load_dotenv
import edinet_tools

load_dotenv()

# Flask app context needed for SQLAlchemy
from app import app, db
from models import Company, Financial, Shareholder, MaterialEvent, Buyback, FilingIndex


SUPPORTED_DOC_TYPES = ["120", "180", "220"]


def resolve_entity(ticker: str):
    """Resolve ticker to EDINET entity."""
    entity = edinet_tools.entity_by_ticker(ticker)
    if entity:
        return entity
    # Fallback: try search
    results = edinet_tools.search(ticker, limit=5)
    for e in results:
        if e.ticker == ticker:
            return e
    return results[0] if results else None


def upsert_company(entity):
    """Insert or update company record."""
    co = Company.query.get(entity.edinet_code)
    if not co:
        co = Company(edinet_code=entity.edinet_code)
        db.session.add(co)
    co.name = entity.name_jp or entity.name
    co.name_en = entity.name_en or entity.name
    co.ticker = entity.ticker
    co.sector = entity.industry if entity.industry and entity.industry != '-' else None
    db.session.commit()
    return co


def get_indexed_docs(edinet_code: str, doc_type: str):
    """Look up documents from the filing index (built during seed).

    Returns list of FilingIndex rows, or empty list if no index exists.
    """
    return FilingIndex.query.filter_by(
        edinet_code=edinet_code, doc_type_code=doc_type
    ).all()


def extract_shareholders_with_llm(text: str):
    """Extract shareholder names and holding ratios from text using an LLM.

    The MajorShareholdersTextBlock is unstructured Japanese text with
    shareholder names, addresses, share counts, and percentages concatenated.
    An LLM handles this reliably where regex would be fragile.
    """
    try:
        import llm
        from pydantic import BaseModel, Field

        class ShareholderEntry(BaseModel):
            name: str = Field(description="Shareholder name in Japanese")
            name_en: str = Field(description="Shareholder name translated to English")
            holding_pct: float = Field(description="Holding percentage (e.g., 52.40)")

        class ShareholderList(BaseModel):
            shareholders: list[ShareholderEntry] = Field(description="List of major shareholders")

        model = llm.get_model("gemini-3-flash-preview")
        response = model.prompt(
            f"Extract the major shareholders and their holding percentages from this Japanese disclosure text. "
            f"Translate each shareholder name to English. "
            f"Return only individual shareholders, not totals (計/合計).\n\n{text[:3000]}",
            schema=ShareholderList,
        )
        result = json.loads(response.text())
        return [{'name': s['name'], 'name_en': s['name_en'], 'holding_ratio': s['holding_pct']}
                for s in result.get('shareholders', [])]
    except Exception as e:
        print(f"    Shareholder extraction skipped: {e}")
        return []


def _parse_doc(doc_or_index_entry):
    """Parse a document — from either a Document object or a FilingIndex entry.

    When using the filing index, we use edinet_tools.fetch_and_parse() to go
    directly from doc_id to typed report without scanning dates.
    """
    if isinstance(doc_or_index_entry, FilingIndex):
        return edinet_tools.fetch_and_parse(
            doc_or_index_entry.doc_id,
            doc_or_index_entry.doc_type_code,
        )
    return doc_or_index_entry.parse()


def _get_doc_id(doc_or_index_entry):
    """Get doc_id from either a Document or FilingIndex entry."""
    if isinstance(doc_or_index_entry, FilingIndex):
        return doc_or_index_entry.doc_id
    return doc_or_index_entry.doc_id


def _get_documents(entity, doc_type: str, days_back: int):
    """Get documents for an entity — from index if available, else API scan."""
    # If the filing index has been built (by seed.py), use it exclusively
    index_exists = FilingIndex.query.first() is not None
    if index_exists:
        indexed = get_indexed_docs(entity.edinet_code, doc_type)
        print(f"  Found {len(indexed)} Doc {doc_type} filings in index")
        return indexed

    # No index at all — fall back to date-by-date API scan
    api_key = os.environ.get("EDINET_API_KEY")
    if not api_key:
        print("Error: Set EDINET_API_KEY in .env to ingest filings. See README.")
        sys.exit(1)
    edinet_tools.configure(api_key=api_key)
    docs = entity.documents(doc_type=doc_type, days=days_back)
    print(f"  Found {len(docs)} Doc {doc_type} filings via API scan")
    return docs


def ingest_doc_120(entity, days_back: int = 730):
    """Ingest securities reports (Doc 120) — financials + shareholders."""
    docs = _get_documents(entity, '120', days_back)

    count = 0
    for doc in docs:
        try:
            report = _parse_doc(doc)
            if not report:
                continue

            fy = str(report.fiscal_year_end or "")
            if not fy:
                continue

            # Upsert financial record
            existing = Financial.query.filter_by(
                edinet_code=entity.edinet_code, fiscal_year_end=fy
            ).first()
            f = existing or Financial(edinet_code=entity.edinet_code, fiscal_year_end=fy)
            if not existing:
                db.session.add(f)

            f.revenue = report.net_sales
            f.operating_income = report.operating_income
            f.net_income = report.net_income
            f.total_assets = report.total_assets
            f.net_assets = report.net_assets
            f.equity_ratio = float(report.equity_ratio) if report.equity_ratio is not None else None
            f.roe = float(report.roe) if report.roe is not None else None
            f.eps = float(report.earnings_per_share) if report.earnings_per_share is not None else None
            f.bps = float(report.net_assets_per_share) if report.net_assets_per_share is not None else None
            f.operating_cf = report.operating_cash_flow
            f.investing_cf = report.investing_cash_flow
            f.financing_cf = report.financing_cash_flow

            # Store raw text blocks and unmapped fields for later enrichment
            if report.text_blocks:
                f.text_blocks_json = json.dumps(report.text_blocks, ensure_ascii=False)
            if report.unmapped_fields:
                f.unmapped_fields_json = json.dumps(
                    {k: str(v) for k, v in report.unmapped_fields.items()},
                    ensure_ascii=False,
                )

            db.session.commit()

            # Extract shareholders from text blocks (requires LLM key)
            try:
                if report.text_blocks:
                    sh_text = report.text_blocks.get('MajorShareholdersTextBlock', '')
                    if sh_text and len(sh_text) > 50:
                        shareholders = extract_shareholders_with_llm(sh_text)
                        for sh in shareholders:
                            existing_sh = Shareholder.query.filter_by(
                                edinet_code=entity.edinet_code,
                                name=sh['name'],
                                fiscal_year_end=fy,
                            ).first()
                            if not existing_sh:
                                s = Shareholder(
                                    edinet_code=entity.edinet_code,
                                    name=sh['name'],
                                    name_en=sh.get('name_en'),
                                    holding_ratio=sh['holding_ratio'],
                                    fiscal_year_end=fy,
                                )
                                db.session.add(s)
                        db.session.commit()
                        if shareholders:
                            print(f"    Extracted {len(shareholders)} shareholders")
            except Exception as e:
                print(f"    Shareholder extraction skipped: {e}")

            count += 1
            print(f"    Ingested: FY {fy}")
        except Exception as e:
            print(f"    Error on {_get_doc_id(doc)}: {e}")
            db.session.rollback()

    return count


def ingest_doc_180(entity, days_back: int = 730):
    """Ingest extraordinary reports (Doc 180)."""
    docs = _get_documents(entity, '180', days_back)

    count = 0
    for doc in docs:
        try:
            report = _parse_doc(doc)
            if not report:
                continue

            evt = MaterialEvent(
                edinet_code=entity.edinet_code,
                filing_date=str(report.filing_date) if report.filing_date else None,
                event_type=report.event_type,
                summary=report.document_title or "",
                reason_for_filing=report.reason_for_filing,
            )
            db.session.add(evt)
            db.session.commit()
            count += 1
        except Exception as e:
            print(f"    Error on {_get_doc_id(doc)}: {e}")
            db.session.rollback()

    return count


def ingest_doc_220(entity, days_back: int = 730):
    """Ingest treasury stock reports (Doc 220)."""
    docs = _get_documents(entity, '220', days_back)

    count = 0
    for doc in docs:
        try:
            report = _parse_doc(doc)
            if not report:
                continue

            bb = Buyback(
                edinet_code=entity.edinet_code,
                filing_date=str(report.filing_date) if report.filing_date else None,
                shares_acquired=None,
                total_cost=None,
                board_resolution_text=report.by_board_meeting,
                disposal_text=report.disposal_holding_text,
            )
            db.session.add(bb)
            db.session.commit()
            count += 1
        except Exception as e:
            print(f"    Error on {_get_doc_id(doc)}: {e}")
            db.session.rollback()

    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest EDINET filings into SQLite")
    parser.add_argument("tickers", nargs="+", help="Ticker codes (e.g., 7203 6758)")
    parser.add_argument("--doc-type", choices=["120", "180", "220"], default="120",
                        help="Document type to ingest (default: 120)")
    parser.add_argument("--all", action="store_true", dest="all_types",
                        help="Ingest all supported doc types")
    parser.add_argument("--days-back", type=int, default=730,
                        help="How many days back to search (default: 730, ignored if index exists)")
    args = parser.parse_args()

    doc_types = SUPPORTED_DOC_TYPES if args.all_types else [args.doc_type]

    with app.app_context():
        for ticker in args.tickers:
            print(f"\n{'='*50}")
            print(f"Ticker: {ticker}")
            entity = resolve_entity(ticker)
            if not entity:
                print(f"  Not found in EDINET entity directory. Skipping.")
                continue
            print(f"  Entity: {entity.name_jp} ({entity.edinet_code})")

            upsert_company(entity)

            for dt in doc_types:
                print(f"\n  Doc type {dt}:")
                if dt == "120":
                    n = ingest_doc_120(entity, days_back=args.days_back)
                elif dt == "180":
                    n = ingest_doc_180(entity, days_back=args.days_back)
                elif dt == "220":
                    n = ingest_doc_220(entity, days_back=args.days_back)
                else:
                    n = 0
                print(f"  Ingested {n} filings")

    print(f"\nDone. View results at http://localhost:5000/companies")


if __name__ == "__main__":
    main()
