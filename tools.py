"""Agent tools for multi-agent analysis.

Each function here becomes a tool the agents can call. Add a new tool
by writing a function that takes a ticker and returns a string, then
add it to ALL_TOOLS at the bottom of this file.
"""
import json

from models import Company, Financial, Shareholder, MaterialEvent, Buyback


def _get_company(ticker: str):
    return Company.query.filter_by(ticker=ticker).first()


def get_financials(ticker: str) -> str:
    """Get financial summary for a Japanese company by ticker code (e.g., '7203').
    Returns revenue, net income, ROE, equity ratio, and cash flows."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}. Available tickers: " + \
               ", ".join(c.ticker for c in Company.query.all())

    rows = Financial.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Financial.fiscal_year_end.desc()).all()
    if not rows:
        return f"No financial data for {ticker} ({co.name})"

    result = [f"Financials for {ticker} {co.name} ({co.name_en}):"]
    for f in rows:
        if f.revenue and f.net_income:
            parts = [f"  FY {f.fiscal_year_end}: Revenue ¥{f.revenue:,.0f}, NI ¥{f.net_income:,.0f}"]
            if f.roe is not None:
                parts.append(f"ROE {f.roe*100:.1f}%")
            if f.equity_ratio is not None:
                parts.append(f"Equity {f.equity_ratio*100:.1f}%")
            if f.operating_cf is not None:
                parts.append(f"OCF ¥{f.operating_cf:,.0f}")
            result.append(", ".join(parts))
        else:
            result.append(f"  FY {f.fiscal_year_end}: (partial data)")
    return "\n".join(result)


def get_shareholders(ticker: str) -> str:
    """Get major shareholders for a Japanese company by ticker code.
    Shows name and holding percentage if enrichment has been run,
    otherwise returns raw text from the filing."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}"

    # Check for enriched (structured) shareholder data first
    rows = Shareholder.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Shareholder.holding_ratio.desc()).all()
    if rows:
        result = [f"Shareholders of {ticker} {co.name}:"]
        for s in rows:
            en = f" ({s.name_en})" if s.name_en else ""
            result.append(f"  {s.name}{en}: {s.holding_ratio:.1f}%")

        return "\n".join(result)

    # Fall back to raw text block from the filing
    latest = Financial.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Financial.fiscal_year_end.desc()).first()
    if latest and latest.text_blocks_json:
        blocks = json.loads(latest.text_blocks_json)
        raw = blocks.get('MajorShareholdersTextBlock', '')
        if raw:
            return f"Raw shareholder data for {ticker} {co.name} (not yet enriched):\n{raw[:2000]}"

    return f"No shareholder data for {ticker}"


def get_material_events(ticker: str) -> str:
    """Get material events (extraordinary reports) for a Japanese company.
    Includes event type, filing date, and the company's stated reason for filing.
    Returns empty if Doc 180 not ingested."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}"

    rows = MaterialEvent.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(MaterialEvent.filing_date.desc()).all()
    if not rows:
        return f"No material events for {ticker}. Run: python pipeline.py {ticker} --doc-type 180"

    result = [f"Material events for {ticker} {co.name}:"]
    for e in rows:
        result.append(f"\n  {e.filing_date}: {e.event_type or 'N/A'} — {e.summary or ''}")
        if e.reason_for_filing:
            result.append(f"  Reason: {e.reason_for_filing[:1500]}")
    return "\n".join(result)


def get_buyback_activity(ticker: str) -> str:
    """Get treasury stock buyback activity for a Japanese company.
    Shows board authorization details, daily execution data (shares, cost),
    and disposal/cancellation activity. Returns empty if Doc 220 not ingested."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}"

    rows = Buyback.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Buyback.filing_date.desc()).all()
    if not rows:
        return f"No buyback data for {ticker}. Run: python pipeline.py {ticker} --doc-type 220"

    result = [f"Buyback activity for {ticker} {co.name}:"]
    for b in rows:
        result.append(f"\n  Report date: {b.filing_date}")
        if b.shares_acquired:
            result.append(f"  Acquired: {b.shares_acquired:,.0f} shares, ¥{b.total_cost:,.0f}")
        if b.board_resolution_text:
            result.append(f"  Board resolution: {b.board_resolution_text[:2000]}")
        if b.disposal_text and b.disposal_text.strip() != "該当事項はありません。":
            result.append(f"  Disposal/holding: {b.disposal_text[:1000]}")
    return "\n".join(result)


def get_business_overview(ticker: str) -> str:
    """Get management's own description of the business: MD&A, strategy, risks,
    and business results — extracted from the most recent securities report.
    This is the company's voice, not numbers. Use it to understand what
    management thinks is going well, what they're worried about, and where
    they're investing."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}"

    latest = Financial.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Financial.fiscal_year_end.desc()).first()
    if not latest or not latest.text_blocks_json:
        return f"No disclosure text for {ticker}. Run: python pipeline.py {ticker}"

    blocks = json.loads(latest.text_blocks_json)

    # Most valuable blocks for understanding the business
    sections = [
        ("Business Description", "DescriptionOfBusinessTextBlock"),
        ("Business Results", "BusinessResultsOfGroupTextBlock"),
        ("MD&A", "ManagementAnalysisOfFinancialPositionOperatingResultsAndCashFlowsTextBlock"),
        ("Strategy & Issues", "BusinessPolicyBusinessEnvironmentIssuesToAddressEtcTextBlock"),
        ("Business Risks", "BusinessRisksTextBlock"),
        ("Segment Information", "NotesSegmentInformationConsolidatedFinancialStatementsIFRSTextBlock"),
        ("Capital Expenditure", "OverviewOfCapitalExpendituresEtcTextBlock"),
        ("R&D", "ResearchAndDevelopmentActivitiesTextBlock"),
    ]

    result = [f"Business overview for {ticker} {co.name} (FY {latest.fiscal_year_end}):"]
    for label, key in sections:
        text = blocks.get(key, "").strip()
        if text and len(text) > 30:
            result.append(f"\n--- {label} ---\n{text[:3000]}")

    if len(result) == 1:
        return f"No business text blocks found for {ticker}"
    return "\n".join(result)


def get_governance(ticker: str) -> str:
    """Get governance and shareholder returns info: corporate governance structure,
    officer details, remuneration, dividend policy, cross-shareholdings, and
    treasury stock activity — from the most recent securities report."""
    co = _get_company(ticker)
    if not co:
        return f"No data for ticker {ticker}"

    latest = Financial.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Financial.fiscal_year_end.desc()).first()
    if not latest or not latest.text_blocks_json:
        return f"No disclosure text for {ticker}. Run: python pipeline.py {ticker}"

    blocks = json.loads(latest.text_blocks_json)

    sections = [
        ("Corporate Governance", "OverviewOfCorporateGovernanceTextBlock"),
        ("Officers", "InformationAboutOfficersTextBlock"),
        ("Outside Directors", "OutsideDirectorsAndOutsideCorporateAuditorsTextBlock"),
        ("Remuneration", "RemunerationForDirectorsAndOtherOfficersTextBlock"),
        ("Dividend Policy", "DividendPolicyTextBlock"),
        ("Cross-Shareholdings", "ShareholdingsTextBlock"),
        ("Treasury Shares", "TreasurySharesEtcTextBlock"),
    ]

    result = [f"Governance for {ticker} {co.name} (FY {latest.fiscal_year_end}):"]
    for label, key in sections:
        text = blocks.get(key, "").strip()
        if text and len(text) > 30:
            result.append(f"\n--- {label} ---\n{text[:3000]}")

    if len(result) == 1:
        return f"No governance text blocks found for {ticker}"
    return "\n".join(result)


def search_web(query: str) -> str:
    """Search the web for information about a company or market topic.
    For Japanese companies, search in Japanese for better results.
    Example: '7203 トヨタ自動車 業績' instead of 'Toyota earnings'.
    No API key required."""
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=5))
        if not results:
            return f"No web results for: {query}"
        output = []
        for r in results:
            output.append(f"**{r['title']}**\n{r.get('body', '')[:300]}\n{r['href']}\n")
        output.append("Tip: read the full page by visiting the URL directly.")
        return "\n".join(output)
    except ImportError:
        return "Web search unavailable — pip install duckduckgo-search"
    except Exception as e:
        return f"Web search error: {e}"


# All tools as a list for the agent runner
ALL_TOOLS = [
    get_financials,
    get_shareholders,
    get_material_events,
    get_buyback_activity,
    get_business_overview,
    get_governance,
    search_web,
]
