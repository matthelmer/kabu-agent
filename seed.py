"""Generate the pre-seeded database.

Run once during development. Commits the output to data/research.db.
Requires EDINET_API_KEY.

Phase 1: Scan EDINET API day-by-day, index all filings in filing_index.
Phase 2: Ingest Doc 120 financials + shareholders for seed companies.

Usage:
    python seed.py                              # Full seed (index + ingest)
    python seed.py --rebuild-index              # Rebuild filing index only
    python seed.py --rebuild-index --days 365   # Custom lookback
"""
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
load_dotenv()

import edinet_tools
from app import app, db
from models import FilingIndex
from pipeline import resolve_entity, upsert_company, ingest_doc_120

# Seed companies — mix of profiles for screening contrast
SEED_TICKERS = [
    # ── Anchors — universally recognized large-caps ──────────────
    "7203",  # Toyota Motor — automobiles, IFRS
    "6758",  # Sony Group — entertainment/electronics, IFRS
    "6861",  # Keyence — factory automation sensors, 94% equity ratio
    "7974",  # Nintendo — gaming, cash-rich
    "4063",  # Shin-Etsu Chemical — silicones & semiconductor wafers

    # ── Mid-caps — interesting sectors ───────────────────────────
    "6869",  # Sysmex — hematology analyzers / diagnostics, IFRS
    "6268",  # Nabtesco — precision RV reducers for robot joints, IFRS
    "6951",  # JEOL — electron microscopes, scientific instruments
    "6101",  # Tsugami — CNC automatic lathes, IFRS, 19% ROE
    "4043",  # Tokuyama — semiconductor-grade polysilicon
    "7730",  # MANI — surgical needles & dental instruments, 91% eq ratio
    "1414",  # Sho-Bond Holdings — infrastructure repair specialist
    "5344",  # MARUWA — ceramic substrates for electronics
    "5702",  # Daiki Aluminium — secondary aluminium alloys
    "6016",  # Japan Engine Corp — marine diesel engines

    # ── Small / micro-caps — niche businesses ────────────────────
    "6264",  # Marumae — precision vacuum parts for semiconductor equipment
    "6327",  # Kitagawa Seiki — hot press machines, 17% ROE
    "4082",  # Daiichi Kigenso Kagaku — rare earth zirconium compounds
    "3446",  # JTEC Corp — ultra-precision optics, low ROE contrast
    "5381",  # Mipox — precision polishing films for semiconductors
    "4979",  # OAT Agrio — agrochemicals / crop protection
    "6039",  # JARM — veterinary referral hospital chain
    "5698",  # Envipro Holdings — metal recycling / circular economy
    "7727",  # Oval Corp — flow meters, low ROE contrast
    "9115",  # Meiji Shipping — bulk shipping, 10% equity ratio
]

DAYS_BACK = 1095  # 3 years

# Doc types we care about indexing
INDEX_DOC_TYPES = {'120', '180', '220'}


def main():
    api_key = os.environ.get("EDINET_API_KEY")
    if not api_key:
        print("Error: Set EDINET_API_KEY in .env")
        sys.exit(1)

    edinet_tools.configure(api_key=api_key)

    with app.app_context():
        db.create_all()

        # Resolve all entities and upsert companies
        entities = {}
        for ticker in SEED_TICKERS:
            entity = resolve_entity(ticker)
            if not entity:
                print(f"  {ticker}: not found, skipping")
                continue
            entities[entity.edinet_code] = entity
            upsert_company(entity)
            print(f"  {ticker}: {entity.name_jp} ({entity.edinet_code})")

        # Phase 1: Scan all dates, build filing index for ALL companies
        print(f"\nPhase 1: Scanning {DAYS_BACK} days to build filing index...")
        print(f"Indexing all Doc {'/'.join(INDEX_DOC_TYPES)} filings for future pipeline use.\n")

        today = date.today()
        index_count = 0
        for i in range(DAYS_BACK):
            d = today - timedelta(days=i)
            try:
                docs = edinet_tools.documents(date=d.isoformat())
            except Exception as e:
                print(f"  {d}: API error ({e})")
                continue

            for doc in docs:
                if doc.doc_type_code not in INDEX_DOC_TYPES:
                    continue
                if not doc.filer_edinet_code:
                    continue

                existing = FilingIndex.query.filter_by(doc_id=doc.doc_id).first()
                if not existing:
                    fi = FilingIndex(
                        doc_id=doc.doc_id,
                        edinet_code=doc.filer_edinet_code,
                        doc_type_code=doc.doc_type_code,
                        filing_date=d.isoformat(),
                        doc_description=getattr(doc, 'doc_description', None),
                    )
                    db.session.add(fi)
                    index_count += 1

            db.session.commit()

            if i > 0 and i % 100 == 0:
                print(f"  ...scanned {i}/{DAYS_BACK} days, {index_count} indexed")

        print(f"  Index complete: {index_count} filings indexed")

        # Phase 2: Ingest financials + shareholders for seed companies
        # Delegates to pipeline.py which handles parsing, upsert, and LLM shareholder extraction
        print(f"\nPhase 2: Ingesting financials for {len(entities)} seed companies...")
        total_filings = 0
        for edinet_code, entity in entities.items():
            n = ingest_doc_120(entity, days_back=DAYS_BACK)
            total_filings += n
            print(f"  {entity.ticker}: {n} fiscal years")

        print(f"\nSeed complete.")
        print(f"  {total_filings} filings ingested for {len(entities)} seed companies")
        print(f"  {index_count} filings indexed across all companies")
        print(f"  Database at data/research.db")


def rebuild_index(days_back: int):
    """Rebuild the filing index from EDINET API."""
    api_key = os.environ.get("EDINET_API_KEY")
    if not api_key:
        print("Error: Set EDINET_API_KEY in .env")
        sys.exit(1)

    edinet_tools.configure(api_key=api_key)

    with app.app_context():
        db.create_all()
        today = date.today()
        new_count = 0
        total_count = 0

        for i in range(days_back):
            d = today - timedelta(days=i)
            try:
                docs = edinet_tools.documents(date=d.isoformat())
            except Exception as e:
                print(f"  {d}: API error ({e})")
                continue

            for doc in docs:
                if doc.doc_type_code not in INDEX_DOC_TYPES:
                    continue
                if not doc.filer_edinet_code:
                    continue
                total_count += 1

                existing = FilingIndex.query.filter_by(doc_id=doc.doc_id).first()
                if not existing:
                    fi = FilingIndex(
                        doc_id=doc.doc_id,
                        edinet_code=doc.filer_edinet_code,
                        doc_type_code=doc.doc_type_code,
                        filing_date=d.isoformat(),
                        doc_description=getattr(doc, 'doc_description', None),
                    )
                    db.session.add(fi)
                    new_count += 1

            db.session.commit()
            if i > 0 and i % 100 == 0:
                print(f"  ...scanned {i}/{days_back} days, {new_count} new filings")

        existing_count = FilingIndex.query.count()
        print(f"\nIndex rebuilt.")
        print(f"  Scanned {days_back} days, found {total_count} filings")
        print(f"  {new_count} new, {existing_count} total in index")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Seed database or rebuild filing index")
    parser.add_argument("--rebuild-index", action="store_true",
                        help="Rebuild filing index from EDINET API")
    parser.add_argument("--days", type=int, default=DAYS_BACK,
                        help=f"Days to scan (default: {DAYS_BACK})")
    args = parser.parse_args()

    if args.rebuild_index:
        rebuild_index(args.days)
    else:
        main()
