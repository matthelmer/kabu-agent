"""Flask app — all routes for the research tool.

Routes: /, /companies, /company/<ticker>, /search, /queries
Data: SQLite at data/research.db
"""
import json
import math
import os

from dotenv import load_dotenv
from flask import Flask, render_template, request
import edinet_tools

load_dotenv()

# Doc type display names from edinet-tools
_DOC_TYPE_NAMES = {dt.code: dt.name_en for dt in edinet_tools.doc_types()}

def _nice_ceil(val):
    """Round up to a nice number for chart axes."""
    if val <= 0:
        return 0
    mag = 10 ** math.floor(math.log10(val))
    norm = val / mag
    if norm <= 1.0:
        nice = 1.0
    elif norm <= 2.0:
        nice = 2.0
    elif norm <= 5.0:
        nice = 5.0
    else:
        nice = 10.0
    return nice * mag


def _compact(val, is_pct=False):
    """Format a number compactly for chart axis labels."""
    if is_pct:
        return f"{val:.0f}%"
    if val == 0:
        return "0"
    a = abs(val)
    sign = "-" if val < 0 else ""
    if a >= 1_000_000:
        return f"{sign}{a / 1_000_000:.1f}M"
    if a >= 10_000:
        return f"{sign}{a / 1_000:.0f}K"
    if a >= 1_000:
        return f"{sign}{a / 1_000:.1f}K"
    return f"{val:,.0f}"


def _chart_ticks(val_min, val_max, is_pct=False):
    """Compute 3 nice round tick values with formatted labels."""
    nice_max = _nice_ceil(val_max) if val_max > 0 else 0
    nice_min = -_nice_ceil(abs(val_min)) if val_min < 0 else 0
    mid = (nice_max + nice_min) / 2
    return {
        'max': nice_max, 'mid': mid, 'min': nice_min,
        'max_label': _compact(nice_max, is_pct),
        'mid_label': _compact(mid, is_pct),
        'min_label': _compact(nice_min, is_pct),
    }


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_BASE_DIR, "data", "research.db")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{_DB_PATH}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

from models import db, Company, Financial, Shareholder, MaterialEvent, Buyback, Analysis
db.init_app(app)

with app.app_context():
    db.create_all()

# Screening queries — add your own here. Each needs a name, description, and SQL.
QUERIES = {
    "quality_compounders": {
        "name": "Quality Compounders",
        "description": "Companies with ROE above 15%, equity ratio above 50%, and positive net income",
        "sql": """
            SELECT c.ticker, COALESCE(c.name_en, c.name) as company,
                   f.roe as "ROE", f.equity_ratio as "Equity Ratio",
                   f.net_income as "Net Income"
            FROM financials f
            JOIN companies c ON f.edinet_code = c.edinet_code
            WHERE f.fiscal_year_end = (
                SELECT MAX(f2.fiscal_year_end) FROM financials f2
                WHERE f2.edinet_code = f.edinet_code
            )
            AND f.roe > 0.15
            AND f.equity_ratio > 0.50
            AND f.net_income > 0
            ORDER BY f.roe DESC
        """,
    },
    "cash_flow_machines": {
        "name": "Cash Flow Machines",
        "description": "Companies where operating cash flow exceeds net income (FCF conversion > 1.0)",
        "sql": """
            SELECT c.ticker, COALESCE(c.name_en, c.name) as company,
                   f.operating_cf as "Operating CF", f.net_income as "Net Income",
                   CAST(f.operating_cf AS REAL) / NULLIF(f.net_income, 0) as "FCF Conversion"
            FROM financials f
            JOIN companies c ON f.edinet_code = c.edinet_code
            WHERE f.fiscal_year_end = (
                SELECT MAX(f2.fiscal_year_end) FROM financials f2
                WHERE f2.edinet_code = f.edinet_code
            )
            AND f.operating_cf > 0
            AND f.net_income > 0
            AND CAST(f.operating_cf AS REAL) / NULLIF(f.net_income, 0) > 1.0
            ORDER BY "FCF Conversion" DESC
        """,
    },
}


@app.route("/")
def index():
    company_count = Company.query.count()
    return render_template("index.html", company_count=company_count)


@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    results = []
    if query:
        # If query looks like a ticker, try direct lookup first
        if query.isdigit() and len(query) <= 5:
            entity = edinet_tools.entity_by_ticker(query)
            if entity:
                results = [entity]
        # Fall back to name search
        if not results:
            results = edinet_tools.search_entities(query, limit=20)
    return render_template("search.html", query=query, results=results)


@app.route("/companies")
def companies():
    all_companies = Company.query.order_by(Company.ticker).all()
    company_data = []
    for c in all_companies:
        latest = Financial.query.filter_by(edinet_code=c.edinet_code)\
            .order_by(Financial.fiscal_year_end.desc()).first()
        has_analysis = Analysis.query.filter_by(ticker=c.ticker).count() > 0
        company_data.append({
            'company': c,
            'latest': latest,
            'has_analysis': has_analysis,
        })
    return render_template("companies.html", company_data=company_data)


@app.route("/company/<ticker>")
def company(ticker):
    company = Company.query.filter_by(ticker=ticker).first()

    if company:
        financials = Financial.query.filter_by(edinet_code=company.edinet_code)\
            .order_by(Financial.fiscal_year_end.desc()).all()
        # Shareholders from latest fiscal year only (avoid duplicates across years)
        latest_fy = db.session.query(db.func.max(Shareholder.fiscal_year_end))\
            .filter_by(edinet_code=company.edinet_code).scalar()
        shareholders = Shareholder.query.filter_by(
            edinet_code=company.edinet_code, fiscal_year_end=latest_fy
        ).order_by(Shareholder.holding_ratio.desc()).all() if latest_fy else []
        events = MaterialEvent.query.filter_by(edinet_code=company.edinet_code)\
            .order_by(MaterialEvent.filing_date.desc()).all()
        buybacks = Buyback.query.filter_by(edinet_code=company.edinet_code)\
            .order_by(Buyback.filing_date.desc()).all()
        analysis = Analysis.query.filter_by(ticker=ticker)\
            .order_by(Analysis.run_date.desc()).first()

        # Deserialize analysis JSON
        analyst_data = skeptic_data = outlook_data = cost_data = None
        if analysis:
            try:
                analyst_data = json.loads(analysis.analyst_report) if analysis.analyst_report else None
                skeptic_data = json.loads(analysis.skeptic_report) if analysis.skeptic_report else None
                outlook_data = json.loads(analysis.outlook) if analysis.outlook else None
                cost_data = json.loads(analysis.model_costs) if analysis.model_costs else None
            except json.JSONDecodeError:
                pass

        # Chart data (trends, oldest to newest)
        chart_data = None
        if financials and len(financials) >= 2:
            ordered = list(reversed(financials))
            chart_data = {
                'years': [f.fiscal_year_end[:4] for f in ordered],
                'revenue': [f.revenue // 1_000_000 if f.revenue else 0 for f in ordered],
                'net_income': [f.net_income // 1_000_000 if f.net_income else 0 for f in ordered],
                'roe': [f.roe * 100 if f.roe else 0 for f in ordered],
                'equity_ratio': [f.equity_ratio * 100 if f.equity_ratio else 0 for f in ordered],
                'ocf': [f.operating_cf // 1_000_000 if f.operating_cf else 0 for f in ordered],
                'fcf': [(f.operating_cf + f.investing_cf) // 1_000_000
                        if f.operating_cf and f.investing_cf else 0 for f in ordered],
                'bps': [f.bps if f.bps else 0 for f in ordered],
            }
            # Compute nice axis ticks for each chart
            all_vals = chart_data['revenue'] + chart_data['net_income']
            raw_min = min(0, min(all_vals)) if all_vals else 0
            raw_max = max(all_vals) if all_vals else 1
            chart_data['rev_ticks'] = _chart_ticks(raw_min, raw_max)

            roe_vals = [v for v in chart_data['roe'] if v != 0]
            eq_vals = [v for v in chart_data['equity_ratio'] if v != 0]
            all_pct = roe_vals + eq_vals
            pct_max = max(all_pct) if all_pct else 20
            pct_min = min(0, min(all_pct)) if all_pct else 0
            chart_data['roe_ticks'] = _chart_ticks(pct_min, pct_max, is_pct=True)

            cf_vals = chart_data['ocf'] + chart_data['fcf']
            cf_min = min(0, min(cf_vals)) if cf_vals else 0
            cf_max = max(cf_vals) if cf_vals else 1
            chart_data['cf_ticks'] = _chart_ticks(cf_min, cf_max)

        latest_roe = financials[0].roe if financials and financials[0].roe else None

        total_filings = len(financials) + len(events) + len(buybacks)

        return render_template("company.html",
            company=company, ticker=ticker,
            financials=financials, shareholders=shareholders,
            events=events, buybacks=buybacks,
            analysis=analysis, analyst_data=analyst_data,
            skeptic_data=skeptic_data, outlook_data=outlook_data,
            cost_data=cost_data, chart_data=chart_data,
            latest_roe=latest_roe, entity=None,
            doc_type_names=_DOC_TYPE_NAMES,
            total_filings=total_filings)
    else:
        # Try entity lookup for skeleton factsheet
        try:
            entity = edinet_tools.entity_by_ticker(ticker)
        except Exception:
            entity = None

        return render_template("company.html",
            company=None, entity=entity, ticker=ticker,
            financials=[], shareholders=[], events=[],
            buybacks=[], analysis=None,
            analyst_data=None, skeptic_data=None,
            outlook_data=None, cost_data=None,
            chart_data=None, latest_roe=None)


@app.route("/queries")
@app.route("/queries/<query_name>")
def queries(query_name=None):
    query = QUERIES.get(query_name) if query_name else None
    rows = []
    if query:
        result = db.session.execute(db.text(query["sql"]))
        rows = result.fetchall()
    return render_template("queries.html",
        queries=QUERIES, query=query, query_name=query_name, rows=rows,
    )

