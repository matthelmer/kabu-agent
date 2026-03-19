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

# Seed companies — 50 companies across sectors and market caps
SEED_TICKERS = [
    # ── Large-caps — recognizable names ──────────────────────────
    "7203",  # Toyota Motor — automobiles, IFRS
    "6758",  # Sony Group — entertainment/electronics, IFRS
    "6861",  # Keyence — factory automation sensors
    "7974",  # Nintendo — gaming, cash-rich
    "4063",  # Shin-Etsu Chemical — silicones & semiconductor wafers
    "8035",  # Tokyo Electron — semiconductor equipment
    "4568",  # Daiichi Sankyo — pharma, antibody-drug conjugates
    "6981",  # Murata Manufacturing — ceramic capacitors, IFRS

    # ── Mid-caps — industrials ───────────────────────────────────
    "6869",  # Sysmex — hematology analyzers, IFRS
    "6951",  # JEOL — electron microscopes
    "6101",  # Tsugami — CNC automatic lathes, IFRS
    "7730",  # MANI — surgical needles, 91% equity ratio
    "1414",  # Sho-Bond Holdings — infrastructure repair
    "5344",  # MARUWA — ceramic substrates
    "6273",  # SMC — pneumatic components, 70%+ equity
    "6146",  # DISCO — precision cutting/grinding for semis
    "6920",  # Lasertec — EUV mask inspection
    "6324",  # Harmonic Drive Systems — strain wave gears

    # ── Mid-caps — tech & consumer ───────────────────────────────
    "9697",  # Capcom — gaming, high ROE
    "2801",  # Kikkoman — soy sauce, global brand
    "2897",  # Nissin Foods — instant noodles, IFRS
    "4519",  # Chugai Pharmaceutical — pharma, Roche subsidiary
    "7453",  # Ryohin Keikaku — MUJI brand retail
    "8697",  # Japan Exchange Group — stock exchange

    # ── Small-caps — niche industrials ───────────────────────────
    "6264",  # Marumae — precision vacuum parts for semis
    "6327",  # Kitagawa Seiki — hot press machines
    "4082",  # Daiichi Kigenso Kagaku — rare earth zirconium
    "3446",  # JTEC Corp — ultra-precision optics
    "5381",  # Mipox — precision polishing films
    "4979",  # OAT Agrio — agrochemicals
    "6039",  # JARM — veterinary referral hospitals
    "5698",  # Envipro Holdings — metal recycling
    "7727",  # Oval Corp — flow meters
    "9115",  # Meiji Shipping — bulk shipping
    "4043",  # Tokuyama — semiconductor-grade polysilicon
    "5702",  # Daiki Aluminium — secondary aluminium
    "6016",  # Japan Engine Corp — marine diesel engines
    "6055",  # Japan Material — gas supply for semis

    # ── Small-caps — niche services & software ───────────────────
    "3921",  # NeoJapan — groupware (desknet's NEO)
    "2477",  # Temairazu — hotel booking engine
    "2163",  # Artner — engineering staffing
    "4058",  # Toyokumo — safety confirmation SaaS
    "9744",  # Meitec — engineering staffing
    "6532",  # Baycurrent Consulting — IT/strategy consulting

    # ── Small-caps — value / activist situations ─────────────────
    "8011",  # Sanyo Shokai — apparel, deep value
    "7294",  # Yorozu — auto suspension parts, undervalued
    "5444",  # Yamato Kogyo — electric arc furnace steel
    "7131",  # Nomura Micro Science — ultrapure water systems
    "4107",  # Ise Chemicals — iodine / rare chemicals
    "5218",  # Ohara — optical glass & glass-ceramics
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
