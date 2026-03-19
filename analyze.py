"""Run multi-agent analysis on a company.

Three agents in a deliberate flow:
  Analyst (research + theses) → Skeptic (challenges) → Outlook (synthesis)

Different models for different roles. Cheap models scan well
but don't push back. Start cheap, measure quality, upgrade the bottleneck.

Usage:
    python analyze.py 7203
    python analyze.py 7203 --analyst-only
"""
import argparse
import json
import sys
from datetime import datetime
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel, Field
import llm

load_dotenv()

from app import app, db
from models import Company, Financial, Analysis
from tools import ALL_TOOLS


# --- Pydantic schemas for structured agent output ---

class InvestmentThesis(BaseModel):
    thesis: str = Field(description="One-sentence investment thesis")
    rationale: str = Field(description="Supporting evidence and reasoning")


class CounterPoint(BaseModel):
    point: str = Field(description="A specific counter-argument, risk, or flaw")
    source: str = Field(description="Where this came from, e.g. 'get_financials: ROE declined from 18% to 12%' or 'web search: Nikkei article on margin pressure'")


class ThesisChallenge(BaseModel):
    counters: List[CounterPoint] = Field(description="2-3 specific counter-arguments with sources")


class AnalystReport(BaseModel):
    theses: List[InvestmentThesis] = Field(description="2-3 investment theses")


class SkepticReport(BaseModel):
    challenges: List[ThesisChallenge] = Field(description="One challenge per thesis, in order")


class OutlookSummary(BaseModel):
    outlook: str = Field(description="2-3 sentence synthesis weighing both sides")


_now = datetime.now()
_DATE_STR = _now.strftime('%Y-%m-%d')
_YEAR = str(_now.year)

SYSTEM_BASE = f"Today is {_DATE_STR}. The current year is {_YEAR}. IMPORTANT: Always write your analysis and all output in English, even if source data is in Japanese."

TOOL_GUIDANCE = f"""
Use typed query tools for financial data, shareholders, material events,
buyback activity, business overview, and governance details.
Use search_web for recent news about the company.
Search in Japanese for Japanese companies (e.g., '手間いらず 業績 {_YEAR}' not 'Temairazu earnings').
When searching the web, use {_YEAR} for current-year results.
"""


MODEL_DISPLAY_NAMES = {
    "gemini-3-flash-preview": "Gemini Flash",
    "anthropic/claude-sonnet-4-6": "Claude Sonnet",
}


def _display_name(model_id: str) -> str:
    return MODEL_DISPLAY_NAMES.get(model_id, model_id)


def detect_models() -> dict:
    """Detect available LLM providers and assign models.

    Analyst + Outlook use cheap/fast model. Skeptic needs stronger reasoning.
    """
    available = {}
    try:
        llm.get_model("gemini-3-flash-preview")
        available["gemini"] = True
    except llm.UnknownModelError:
        available["gemini"] = False

    try:
        llm.get_model("anthropic/claude-sonnet-4-6")
        available["anthropic"] = True
    except llm.UnknownModelError:
        available["anthropic"] = False

    if available.get("gemini") and available.get("anthropic"):
        models = {
            "analyst": "gemini-3-flash-preview",
            "skeptic": "anthropic/claude-sonnet-4-6",
            "outlook": "gemini-3-flash-preview",
        }
    elif available.get("anthropic"):
        models = {
            "analyst": "anthropic/claude-sonnet-4-6",
            "skeptic": "anthropic/claude-sonnet-4-6",
            "outlook": "anthropic/claude-sonnet-4-6",
        }
    elif available.get("gemini"):
        models = {
            "analyst": "gemini-3-flash-preview",
            "skeptic": "gemini-3-flash-preview",
            "outlook": "gemini-3-flash-preview",
        }
    else:
        print("Error: No LLM providers available. Install llm-gemini or llm-anthropic.")
        sys.exit(1)

    print(f"Models: Analyst={_display_name(models['analyst'])}, "
          f"Skeptic={_display_name(models['skeptic'])}, "
          f"Outlook={_display_name(models['outlook'])}")
    return models


def run_agent(name: str, model_id: str, prompt: str, tools: list, system: str,
              schema=None) -> tuple:
    """Run an agent with tools (research phase), then optionally structure output.

    Two-phase approach: chain() for tool calling doesn't compose with schema=,
    so we do research first, then a second call to structure the output.
    """
    model = llm.get_model(model_id)
    conv = model.conversation()

    tool_calls = []

    def after_call(tool, tool_call, tool_result):
        tool_calls.append(tool.name)
        # Show the most interesting argument value (first positional)
        args = tool_call.arguments or {}
        if args:
            first_val = next(iter(args.values()))
            display_arg = repr(first_val) if isinstance(first_val, str) else str(first_val)
        else:
            display_arg = ""
        result_preview = str(tool_result)[:200]
        if "error" in result_preview.lower() or "no results" in result_preview.lower() or "SQL error" in result_preview:
            print(f"  → {tool.name}({display_arg}) ✗")
        else:
            print(f"  → {tool.name}({display_arg})")

    # Phase 1: Research with tools
    response = conv.chain(
        prompt,
        system=system,
        system_fragments=[TOOL_GUIDANCE],
        tools=tools,
        after_call=after_call,
        chain_limit=15,
    )
    research_text = response.text()

    # Collect token usage from research phase
    input_tokens = 0
    output_tokens = 0
    try:
        for r in conv.responses:
            usage = r.usage()
            if usage:
                input_tokens += getattr(usage, "input", 0) or 0
                output_tokens += getattr(usage, "output", 0) or 0
    except Exception:
        pass

    # Phase 2: Structure output (if schema provided)
    if schema:
        structure_prompt = (
            f"Based on your research, produce a structured output. "
            f"Write everything in English.\n\n"
            f"Research:\n{research_text}"
        )
        structured_response = model.prompt(
            structure_prompt, schema=schema,
            system="Output must be in English. Translate any Japanese content."
        )
        structured_text = structured_response.text()
        try:
            usage = structured_response.usage()
            if usage:
                input_tokens += getattr(usage, "input", 0) or 0
                output_tokens += getattr(usage, "output", 0) or 0
        except Exception:
            pass
        print(f"  {name}: {len(tool_calls)} tool calls ({_display_name(model_id)})")
        return structured_text, {"name": name, "model": model_id, "tool_calls": len(tool_calls), "tokens": input_tokens + output_tokens}

    print(f"  {name}: {len(tool_calls)} tool calls ({_display_name(model_id)})")
    return research_text, {"name": name, "model": model_id, "tool_calls": len(tool_calls), "tokens": input_tokens + output_tokens}


def build_company_context(ticker: str) -> str:
    """Build context string from local data."""
    co = Company.query.filter_by(ticker=ticker).first()
    if not co:
        return f"Ticker {ticker} not found in database."

    lines = [f"Company: {co.ticker} {co.name} ({co.name_en})", f"Sector: {co.sector or 'N/A'}"]

    financials = Financial.query.filter_by(edinet_code=co.edinet_code)\
        .order_by(Financial.fiscal_year_end.desc()).limit(5).all()
    if financials:
        lines.append("\nRecent financials:")
        for f in financials:
            if f.revenue and f.net_income:
                parts = [f"  FY {f.fiscal_year_end}: Rev ¥{f.revenue:,.0f}, NI ¥{f.net_income:,.0f}"]
                if f.roe is not None:
                    parts.append(f"ROE {f.roe*100:.1f}%")
                lines.append(", ".join(parts))
            else:
                lines.append(f"  FY {f.fiscal_year_end}: partial data")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Run multi-agent analysis")
    parser.add_argument("ticker", help="Ticker code (e.g., 7203)")
    parser.add_argument("--analyst-only", action="store_true",
                        help="Run only the Analyst agent")
    args = parser.parse_args()

    models = detect_models()

    with app.app_context():
        context = build_company_context(args.ticker)
        if "not found" in context:
            print(f"Error: {args.ticker} not in database. Run: python pipeline.py {args.ticker}")
            sys.exit(1)

        co = Company.query.filter_by(ticker=args.ticker).first()

        # === ANALYST ===
        print(f"\n{'='*50}")
        print(f"▸ Analyst — {args.ticker} {co.name}")
        print(f"{'='*50}")

        analyst_text, analyst_cost = run_agent(
            name="Analyst",
            model_id=models["analyst"],
            prompt=(
                f"Research {co.name} ({args.ticker}) and produce 2-3 investment theses. "
                f"Each thesis should be a clear claim with supporting rationale. "
                f"Use the tools to gather financial data, check shareholders, and search for recent news. "
                f"You can also research competitors using SQL queries or web search.\n\n"
                f"Company context:\n{context}"
            ),
            tools=ALL_TOOLS,
            system=(
                f"You are an equity analyst covering Japanese companies. {SYSTEM_BASE}"
            ),
            schema=AnalystReport,
        )

        analyst_report = AnalystReport.model_validate_json(analyst_text)
        print(f"\n  Theses:")
        for t in analyst_report.theses:
            print(f"    • {t.thesis}")

        if args.analyst_only:
            print("\n--analyst-only: skipping Skeptic and Outlook.")
            return

        # === SKEPTIC ===
        print(f"\n{'='*50}")
        print(f"▸ Skeptic — challenging {len(analyst_report.theses)} theses")
        print(f"{'='*50}")

        theses_text = "\n\n".join(
            f"Thesis {i+1}: {t.thesis}\nRationale: {t.rationale}"
            for i, t in enumerate(analyst_report.theses)
        )

        skeptic_text, skeptic_cost = run_agent(
            name="Skeptic",
            model_id=models["skeptic"],
            prompt=(
                f"The Analyst has produced these investment theses for {co.name} ({args.ticker}). "
                f"Challenge each one with 2-3 specific counter-arguments. For each thesis, find concrete risks, "
                f"flaws, or contradicting evidence. Use the tools to verify claims. "
                f"Return one challenge per thesis, in the same order.\n\n"
                f"{theses_text}\n\n"
                f"Company context:\n{context}"
            ),
            tools=ALL_TOOLS,
            system=(
                f"You are a skeptical equity analyst. Your job is to challenge investment theses — "
                f"find what's wrong, what's missing, what could go wrong. {SYSTEM_BASE}"
            ),
            schema=SkepticReport,
        )

        skeptic_report = SkepticReport.model_validate_json(skeptic_text)
        print(f"\n  Challenges:")
        for i, c in enumerate(skeptic_report.challenges):
            thesis_label = analyst_report.theses[i].thesis if i < len(analyst_report.theses) else f"Thesis {i+1}"
            print(f"    • {thesis_label[:80]}")
            for cp in c.counters[:2]:
                print(f"      ↳ {cp.point[:100]}")

        # === OUTLOOK ===
        print(f"\n{'='*50}")
        print(f"▸ Outlook — synthesizing")
        print(f"{'='*50}")

        outlook_input = f"Analyst theses:\n{theses_text}\n\nSkeptic challenges:\n"
        for i, c in enumerate(skeptic_report.challenges):
            thesis_label = analyst_report.theses[i].thesis if i < len(analyst_report.theses) else f"Thesis {i+1}"
            outlook_input += f"\nOn '{thesis_label}':\n"
            for cp in c.counters:
                outlook_input += f"  - {cp.point} [{cp.source}]\n"

        outlook_text, outlook_cost = run_agent(
            name="Outlook",
            model_id=models["outlook"],
            prompt=(
                f"Weigh the Analyst's theses against the Skeptic's challenges for "
                f"{co.name} ({args.ticker}). Use tools to verify any claims you're unsure about. "
                f"Then produce a 2-3 sentence outlook. For sources, cite specific facts from the "
                f"Analyst and Skeptic inputs above — do not generate your own attributions.\n\n{outlook_input}\n\n"
                f"Company context:\n{context}"
            ),
            tools=ALL_TOOLS,
            system=(
                f"You are a senior portfolio manager synthesizing competing analyst views. "
                f"You can use tools to spot-check facts before forming your view. "
                f"Be concise and decisive. {SYSTEM_BASE}"
            ),
            schema=OutlookSummary,
        )

        outlook_summary = OutlookSummary.model_validate_json(outlook_text)

        print(f"\n  {outlook_summary.outlook}")

        # === SAVE TO DB ===
        analysis = Analysis(
            ticker=args.ticker,
            run_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
            analyst_report=analyst_report.model_dump_json(),
            skeptic_report=skeptic_report.model_dump_json(),
            outlook=outlook_summary.model_dump_json(),
            model_costs=json.dumps([analyst_cost, skeptic_cost, outlook_cost]),
        )
        db.session.add(analysis)
        db.session.commit()

        total_tools = analyst_cost["tool_calls"] + skeptic_cost["tool_calls"] + outlook_cost["tool_calls"]
        print(f"\n{'='*50}")
        print(f"Done — {len(analyst_report.theses)} theses, "
              f"{len(skeptic_report.challenges)} challenges, "
              f"{total_tools} tool calls")
        print(f"View at: http://localhost:5000/company/{args.ticker}")


if __name__ == "__main__":
    main()
