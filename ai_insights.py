"""
AI-powered portfolio insights using Claude.

Requires ANTHROPIC_API_KEY environment variable to be set in the Render dashboard:
  Render Dashboard → statement-parser-api → Environment → Add ANTHROPIC_API_KEY
"""

import os
import json
import re
import logging

logger = logging.getLogger(__name__)

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

SYSTEM_PROMPT = """You are a professional portfolio analyst providing clear, educational insights. \
You are NOT a financial advisor and must NOT give personalized investment advice.

Every insight you generate must:
- Be educational and general in nature
- Include framing such as "generally," "historically," or "many investors consider"
- Never say "you should" — instead say "investors may want to consider" or similar
- End with a brief disclaimer if discussing specific actions

Respond ONLY with a JSON array of 3-5 insight objects. Each object must have:
- "title": a short headline (5-8 words)
- "text": a concise paragraph (2-3 sentences max)
- "type": one of "info", "warning", or "positive"

Do not include any text outside the JSON array."""


def generate_ai_insights(portfolio_data):
    """
    Generate AI-powered portfolio insights using Claude.

    Args:
        portfolio_data: dict with keys like 'positions', 'total_value',
                        'asset_allocation', 'sector_exposure', 'concentration', etc.

    Returns:
        list of dicts with 'title', 'text', 'type' keys, or empty list on failure.
    """
    if not ANTHROPIC_AVAILABLE:
        logger.info("anthropic package not installed, skipping AI insights")
        return []

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set, skipping AI insights")
        return []

    # Build a concise summary for Claude
    summary = _build_portfolio_summary(portfolio_data)

    try:
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": f"Analyze this portfolio and provide 3-5 educational insights covering concentration risk, sector exposure, diversification, and any notable patterns.\n\n{summary}"
                }
            ]
        )

        response_text = message.content[0].text

        # Strip markdown code fences and any surrounding text
        fence_match = re.search(r'```(?:json)?\s*(\[[\s\S]*?\])\s*```', response_text)
        if fence_match:
            response_text = fence_match.group(1)
        else:
            # Try to extract a bare JSON array from the response
            array_match = re.search(r'\[[\s\S]*\]', response_text)
            if array_match:
                response_text = array_match.group(0)

        insights = json.loads(response_text)

        # Validate structure
        validated = []
        for item in insights:
            if isinstance(item, dict) and 'title' in item and 'text' in item:
                validated.append({
                    'category': 'ai',
                    'title': item['title'],
                    'text': item['text'],
                    'type': item.get('type', 'info')
                })
        return validated

    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON response for AI insights")
        return []
    except Exception as e:
        logger.warning(f"AI insights generation failed: {e}")
        return []


def _build_portfolio_summary(data):
    """Build a concise text summary of the portfolio for the prompt."""
    lines = []

    total = data.get('total_value', 0)
    lines.append(f"Total portfolio value: ${total:,.0f}")

    positions = data.get('positions', [])
    lines.append(f"Number of holdings: {len(positions)}")

    # Top holdings
    sorted_pos = sorted(positions, key=lambda p: p.get('value', 0), reverse=True)
    top = sorted_pos[:10]
    if top:
        holdings = ", ".join(
            f"{p.get('symbol', '?')} (${p.get('value', 0):,.0f})"
            for p in top
        )
        lines.append(f"Top holdings: {holdings}")

    # Asset allocation
    alloc = data.get('asset_allocation', {})
    if alloc:
        alloc_str = ", ".join(f"{k}: {v:.1f}%" for k, v in alloc.items() if v > 0)
        lines.append(f"Asset allocation: {alloc_str}")

    # Sector exposure
    sectors = data.get('sector_exposure', {})
    if sectors:
        sector_str = ", ".join(f"{k}: {v:.1f}%" for k, v in sectors.items() if v > 0)
        lines.append(f"Sector exposure: {sector_str}")

    # Concentration
    conc = data.get('concentration', {})
    if conc:
        lines.append(f"Top 10 holdings weight: {conc.get('top_10_weight', 0):.1f}%")
        lines.append(f"Herfindahl index: {conc.get('herfindahl_index', 0):.4f}")

    # Geography
    geo = data.get('geography', {})
    if geo:
        geo_str = ", ".join(f"{k}: {v:.1f}%" for k, v in geo.items() if v > 0)
        lines.append(f"Geographic exposure: {geo_str}")

    return "\n".join(lines)
