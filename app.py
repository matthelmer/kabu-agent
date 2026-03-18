"""Flask app — all routes for the research tool.

Routes: /, /companies, /company/<ticker>, /search, /queries
Data: SQLite at data/research.db
"""
import json
import os

from dotenv import load_dotenv
from flask import Flask, render_template, request
import edinet_tools

load_dotenv()

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
            SELECT c.ticker, c.name, f.roe, f.equity_ratio, f.net_income
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
            SELECT c.ticker, c.name, f.operating_cf, f.net_income,
                   CAST(f.operating_cf AS REAL) / NULLIF(f.net_income, 0) as fcf_conversion
            FROM financials f
            JOIN companies c ON f.edinet_code = c.edinet_code
            WHERE f.fiscal_year_end = (
                SELECT MAX(f2.fiscal_year_end) FROM financials f2
                WHERE f2.edinet_code = f.edinet_code
            )
            AND f.operating_cf > 0
            AND f.net_income > 0
            AND CAST(f.operating_cf AS REAL) / NULLIF(f.net_income, 0) > 1.0
            ORDER BY fcf_conversion DESC
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

        # Chart data (revenue + NI trend, oldest to newest)
        chart_data = None
        if financials and len(financials) >= 2:
            ordered = list(reversed(financials))
            chart_data = {
                'years': [f.fiscal_year_end[:4] for f in ordered],
                'revenue': [f.revenue // 1_000_000 if f.revenue else 0 for f in ordered],
                'net_income': [f.net_income // 1_000_000 if f.net_income else 0 for f in ordered],
            }
            all_vals = chart_data['revenue'] + chart_data['net_income']
            chart_data['y_min'] = min(0, min(all_vals)) if all_vals else 0
            chart_data['y_max'] = max(all_vals) if all_vals else 1

        latest_roe = financials[0].roe if financials and financials[0].roe else None

        return render_template("company.html",
            company=company, ticker=ticker,
            financials=financials, shareholders=shareholders,
            events=events, buybacks=buybacks,
            analysis=analysis, analyst_data=analyst_data,
            skeptic_data=skeptic_data, outlook_data=outlook_data,
            cost_data=cost_data, chart_data=chart_data,
            latest_roe=latest_roe, entity=None)
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

