"""SQLAlchemy models for the local research database.

Populated by: seed.py (initial data), pipeline.py (ingestion), analyze.py (analysis).
Tables mirror EDINET document types: Doc 120 → financials + shareholders,
Doc 180 → material_events, Doc 220 → buybacks.
"""
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Company(db.Model):
    __tablename__ = "companies"
    edinet_code = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    name_en = db.Column(db.Text)
    ticker = db.Column(db.Text, index=True)
    sector = db.Column(db.Text)

    financials = db.relationship("Financial", backref="company", lazy=True)
    shareholders = db.relationship("Shareholder", backref="company", lazy=True)


class Financial(db.Model):
    __tablename__ = "financials"
    id = db.Column(db.Integer, primary_key=True)
    edinet_code = db.Column(db.Text, db.ForeignKey("companies.edinet_code"))
    fiscal_year_end = db.Column(db.Text)
    revenue = db.Column(db.BigInteger)
    operating_income = db.Column(db.BigInteger)
    net_income = db.Column(db.BigInteger)
    total_assets = db.Column(db.BigInteger)
    net_assets = db.Column(db.BigInteger)
    equity_ratio = db.Column(db.Float)
    roe = db.Column(db.Float)
    eps = db.Column(db.Float)
    bps = db.Column(db.Float)
    operating_cf = db.Column(db.BigInteger)
    investing_cf = db.Column(db.BigInteger)
    financing_cf = db.Column(db.BigInteger)
    text_blocks_json = db.Column(db.Text)      # Raw XBRL text blocks (JSON)
    unmapped_fields_json = db.Column(db.Text)   # Unmapped XBRL elements (JSON)

    __table_args__ = (
        db.UniqueConstraint("edinet_code", "fiscal_year_end"),
    )


class Shareholder(db.Model):
    __tablename__ = "shareholders"
    id = db.Column(db.Integer, primary_key=True)
    edinet_code = db.Column(db.Text, db.ForeignKey("companies.edinet_code"))
    name = db.Column(db.Text)
    name_en = db.Column(db.Text)
    holding_ratio = db.Column(db.Float)
    fiscal_year_end = db.Column(db.Text)

    __table_args__ = (
        db.UniqueConstraint("edinet_code", "name", "fiscal_year_end"),
    )


class MaterialEvent(db.Model):
    __tablename__ = "material_events"
    id = db.Column(db.Integer, primary_key=True)
    edinet_code = db.Column(db.Text, db.ForeignKey("companies.edinet_code"))
    filing_date = db.Column(db.Text)
    event_type = db.Column(db.Text)
    summary = db.Column(db.Text)
    reason_for_filing = db.Column(db.Text)  # Full text of reason/explanation


class Buyback(db.Model):
    __tablename__ = "buybacks"
    id = db.Column(db.Integer, primary_key=True)
    edinet_code = db.Column(db.Text, db.ForeignKey("companies.edinet_code"))
    filing_date = db.Column(db.Text)
    shares_acquired = db.Column(db.BigInteger)
    total_cost = db.Column(db.BigInteger)
    board_resolution_text = db.Column(db.Text)   # Board meeting buyback authorization details
    disposal_text = db.Column(db.Text)            # Treasury share disposal/cancellation


class FilingIndex(db.Model):
    """Cached EDINET filing metadata — built during seed, used by pipeline."""
    __tablename__ = "filing_index"
    id = db.Column(db.Integer, primary_key=True)
    doc_id = db.Column(db.Text, unique=True)          # EDINET document ID
    edinet_code = db.Column(db.Text, index=True)      # Filer's EDINET code
    doc_type_code = db.Column(db.Text)                 # '120', '180', '220', etc.
    filing_date = db.Column(db.Text)                   # Date filed
    doc_description = db.Column(db.Text)               # Document title


class Analysis(db.Model):
    __tablename__ = "analyses"
    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.Text, index=True)
    run_date = db.Column(db.Text)
    analyst_report = db.Column(db.Text)    # JSON: AnalystReport schema
    skeptic_report = db.Column(db.Text)    # JSON: SkepticReport schema
    outlook = db.Column(db.Text)           # JSON: OutlookSummary schema
    model_costs = db.Column(db.Text)       # JSON: per-agent cost breakdown
