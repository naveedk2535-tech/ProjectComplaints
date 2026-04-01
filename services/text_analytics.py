"""
Text analytics service for analyzing complaint narratives.
Extracts word frequencies, sentiment, and themes from complaint text.
Uses in-memory caching to avoid re-processing 35K+ narratives on every request.
"""

import re
import statistics
import time
from collections import Counter
from functools import lru_cache

from models.database import db, Complaint

# Simple time-based cache for expensive text processing
_cache = {}
_CACHE_TTL = 300  # 5 minutes


def _cached(key, func):
    """Return cached result or compute and cache."""
    now = time.time()
    if key in _cache and now - _cache[key][1] < _CACHE_TTL:
        return _cache[key][0]
    result = func()
    _cache[key] = (result, now)
    return result

# ---------------------------------------------------------------------------
# Stop words – common English words plus CFPB redaction placeholders
# ---------------------------------------------------------------------------
STOP_WORDS = frozenset({
    "the", "a", "an", "is", "was", "to", "of", "for", "in", "on", "at",
    "by", "with", "from", "that", "this", "it", "and", "or", "but", "not",
    "are", "be", "been", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "can", "may", "might", "i", "my", "me",
    "we", "our", "you", "your", "they", "their", "them", "he", "she", "his",
    "her", "its", "who", "which", "what", "where", "when", "how", "all",
    "each", "every", "no", "any", "some", "than", "then", "also", "about",
    "just", "only", "very", "more", "most", "other", "so", "if", "up",
    "out", "as", "into", "over", "after", "before", "between", "these",
    "those", "through", "during", "against", "because", "until", "while",
    "xxxx", "xx", "xxxxxxxx",
})

# ---------------------------------------------------------------------------
# Sentiment keyword lists
# ---------------------------------------------------------------------------
POSITIVE_WORDS = frozenset({
    "resolved", "helped", "satisfied", "fixed", "corrected", "refund", "credited",
})

NEGATIVE_WORDS = frozenset({
    "fraud", "unauthorized", "refused", "denied", "failed", "violation",
    "illegal", "scam", "stolen", "harassment", "misleading", "deceptive", "unfair",
})

# ---------------------------------------------------------------------------
# Theme definitions
# ---------------------------------------------------------------------------
THEME_KEYWORDS = {
    "Billing & Fees": ["fee", "charge", "interest", "rate", "billing", "payment", "balance", "overcharge"],
    "Fraud & Identity": ["fraud", "unauthorized", "identity", "theft", "stolen", "phishing", "scam"],
    "Account Management": ["account", "closed", "opened", "access", "locked", "frozen", "restricted"],
    "Credit Reporting": ["credit", "report", "score", "bureau", "inaccurate", "dispute", "error"],
    "Customer Service": ["call", "representative", "hold", "response", "ignored", "rude", "unhelpful"],
    "Debt Collection": ["debt", "collection", "collector", "harassment", "calls", "threaten", "sue"],
}

# Pre-compile the regex used to tokenize narratives
_TOKEN_RE = re.compile(r"[a-z]+")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _base_query(company=None):
    """Return a base query filtering to rows that have a non-empty narrative."""
    q = Complaint.query.filter(
        Complaint.narrative.isnot(None),
        Complaint.narrative != "",
    )
    if company:
        q = q.filter(Complaint.company == company)
    return q


def _sampled_narratives(company=None):
    """Get narratives, sampled to 2000 most recent for industry mode. Cached 5 min."""
    def _fetch():
        q = _base_query(company).with_entities(Complaint.narrative)
        if not company:
            q = q.order_by(Complaint.date_received.desc()).limit(2000)
        return q.all()
    return _cached(f'narratives_{company}', _fetch)


def _tokenize(text):
    """Lowercase and extract alpha-only tokens, filtering stop words."""
    tokens = _TOKEN_RE.findall(text.lower())
    return [t for t in tokens if len(t) > 1 and t not in STOP_WORDS and not t.isdigit()]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _build_word_counter(company=None):
    """Build a word counter from narratives (sampled for industry mode)."""
    def _compute():
        rows = _sampled_narratives(company)
        counter = Counter()
        for (narrative,) in rows:
            counter.update(_tokenize(narrative))
        return counter
    return _cached(f'word_counter_{company}', _compute)


def get_top_words(company=None, limit=50):
    """
    Extract the most frequent words from complaint narratives.

    Returns a list of dicts: [{"word": str, "count": int}, ...]
    """
    counter = _build_word_counter(company)
    return [{"word": word, "count": count} for word, count in counter.most_common(limit)]


def get_word_frequency_by_product(company=None, limit=20):
    """
    Top words broken down by product category.

    Returns a dict: {product: [{"word": str, "count": int}, ...]}
    """
    rows = (
        _base_query(company)
        .with_entities(Complaint.product, Complaint.narrative)
        .all()
    )

    product_counters = {}
    for product, narrative in rows:
        if not product:
            continue
        if product not in product_counters:
            product_counters[product] = Counter()
        product_counters[product].update(_tokenize(narrative))

    result = {}
    for product, counter in product_counters.items():
        result[product] = [
            {"word": word, "count": count}
            for word, count in counter.most_common(limit)
        ]
    return result


def get_sentiment_summary(company=None):
    """
    Simple keyword-based sentiment analysis.

    Returns a dict with positive_count, negative_count, neutral_count,
    sentiment_score (-1 to 1), top_positive_words, and top_negative_words.
    """
    def _compute():
        return _compute_sentiment(company)
    return _cached(f'sentiment_{company}', _compute)


def _compute_sentiment(company=None):
    rows = _sampled_narratives(company)

    positive_counter = Counter()
    negative_counter = Counter()
    positive_count = 0
    negative_count = 0
    neutral_count = 0

    for (narrative,) in rows:
        tokens = set(_TOKEN_RE.findall(narrative.lower()))
        pos_hits = tokens & POSITIVE_WORDS
        neg_hits = tokens & NEGATIVE_WORDS

        if pos_hits and not neg_hits:
            positive_count += 1
        elif neg_hits and not pos_hits:
            negative_count += 1
        elif pos_hits and neg_hits:
            # Both present – classify by which has more matches
            if len(pos_hits) >= len(neg_hits):
                positive_count += 1
            else:
                negative_count += 1
        else:
            neutral_count += 1

        for w in pos_hits:
            positive_counter[w] += 1
        for w in neg_hits:
            negative_counter[w] += 1

    total = positive_count + negative_count + neutral_count
    if total > 0:
        sentiment_score = round((positive_count - negative_count) / total, 4)
    else:
        sentiment_score = 0.0

    # Clamp to [-1, 1]
    sentiment_score = max(-1.0, min(1.0, sentiment_score))

    return {
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "sentiment_score": sentiment_score,
        "top_positive_words": [
            {"word": w, "count": c} for w, c in positive_counter.most_common()
        ],
        "top_negative_words": [
            {"word": w, "count": c} for w, c in negative_counter.most_common()
        ],
    }


def get_complaint_themes(company=None, limit=10):
    """
    Identify complaint themes using keyword clustering.

    Returns a list of dicts:
    [{"theme": str, "count": int, "percentage": float,
      "sample_words": [{"word": str, "count": int}]}, ...]
    """
    def _compute():
        return _compute_themes(company, limit)
    return _cached(f'themes_{company}_{limit}', _compute)


def _compute_themes(company=None, limit=10):
    rows = _sampled_narratives(company)
    total_narratives = len(rows)
    if total_narratives == 0:
        return []

    # Count how many narratives match each theme, and track per-keyword hits
    theme_counts = {}
    theme_word_counters = {}
    for theme, keywords in THEME_KEYWORDS.items():
        theme_counts[theme] = 0
        theme_word_counters[theme] = Counter()

    keyword_set = set()
    for keywords in THEME_KEYWORDS.values():
        keyword_set.update(keywords)

    for (narrative,) in rows:
        tokens = set(_TOKEN_RE.findall(narrative.lower()))
        for theme, keywords in THEME_KEYWORDS.items():
            matched = tokens & set(keywords)
            if matched:
                theme_counts[theme] += 1
                for w in matched:
                    theme_word_counters[theme][w] += 1

    results = []
    for theme, count in theme_counts.items():
        percentage = round((count / total_narratives) * 100, 2)
        top_words = theme_word_counters[theme].most_common(5)
        results.append({
            "theme": theme,
            "count": count,
            "percentage": percentage,
            "sample_words": [{"word": w, "count": c} for w, c in top_words],
        })

    # Sort by count descending and apply limit
    results.sort(key=lambda x: x["count"], reverse=True)
    return results[:limit]


def get_narrative_stats(company=None):
    """
    Basic statistics about complaint narratives.

    Returns a dict with total_with_narrative, avg_length, longest,
    shortest, and median_length.
    """
    rows = _sampled_narratives(company)

    if not rows:
        return {
            "total_with_narrative": 0,
            "avg_length": 0,
            "longest": 0,
            "shortest": 0,
            "median_length": 0,
        }

    lengths = [len(narrative) for (narrative,) in rows]

    return {
        "total_with_narrative": len(lengths),
        "avg_length": round(sum(lengths) / len(lengths), 2),
        "longest": max(lengths),
        "shortest": min(lengths),
        "median_length": round(statistics.median(lengths), 2),
    }


def _get_monthly_word_data(company=None, months=6):
    """Shared helper: fetch narratives by month, tokenize, and cache.
    Returns (sorted_months, month_counters) where month_counters = {month: Counter}.
    Used by both get_monthly_word_trends and get_trending_words."""
    def _compute():
        q = _base_query(company).with_entities(
            Complaint.date_received, Complaint.narrative
        ).filter(Complaint.date_received.isnot(None))
        # Sample for industry mode (limit to 2000 most recent)
        if not company:
            q = q.order_by(Complaint.date_received.desc()).limit(2000)
        rows = q.all()
        if not rows:
            return [], {}
        month_narratives = {}
        for date_received, narrative in rows:
            month_key = date_received.strftime("%Y-%m")
            month_narratives.setdefault(month_key, []).append(narrative)
        sorted_months = sorted(month_narratives.keys(), reverse=True)[:months]
        sorted_months.sort()
        month_counters = {}
        for month_key in sorted_months:
            counter = Counter()
            for narrative in month_narratives[month_key]:
                counter.update(_tokenize(narrative))
            month_counters[month_key] = counter
        return sorted_months, month_counters
    return _cached(f'monthly_word_data_{company}_{months}', _compute)


def get_monthly_word_trends(company=None, words=None, months=6):
    """
    Track specific words month over month in complaint narratives.

    If *words* is None the top 8 words are auto-detected.
    Returns a list of dicts:
    [{"word": str, "months": [{"month": "YYYY-MM", "count": int}, ...]}, ...]
    """
    sorted_months, month_counters = _get_monthly_word_data(company, months)
    if not sorted_months:
        return []

    # Auto-detect top words if none provided
    if words is None:
        overall_counter = Counter()
        for m in sorted_months:
            overall_counter += month_counters[m]
        words = [w for w, _ in overall_counter.most_common(8)]

    if not words:
        return []

    result = []
    for word in words:
        month_data = [
            {"month": m, "count": month_counters[m].get(word, 0)}
            for m in sorted_months
        ]
        result.append({"word": word, "months": month_data})

    return result


def get_word_comparison(company=None, compare_company=None, limit=15):
    """
    Compare top words between two companies, or between a company and
    the overall average.

    Returns a list of dicts:
    [{"word": str, "company_count": int, "compare_count": int,
      "difference": int}, ...]
    """
    # Word counts for the primary company
    rows_a = _base_query(company).with_entities(Complaint.narrative).all()
    counter_a = Counter()
    for (narrative,) in rows_a:
        counter_a.update(_tokenize(narrative))

    # Word counts for the comparison target
    rows_b = _base_query(compare_company).with_entities(Complaint.narrative).all()
    counter_b = Counter()
    for (narrative,) in rows_b:
        counter_b.update(_tokenize(narrative))

    # Collect the union of top words from both sides
    top_words_a = {w for w, _ in counter_a.most_common(limit)}
    top_words_b = {w for w, _ in counter_b.most_common(limit)}
    all_words = top_words_a | top_words_b

    result = []
    for word in all_words:
        company_count = counter_a.get(word, 0)
        compare_count = counter_b.get(word, 0)
        result.append({
            "word": word,
            "company_count": company_count,
            "compare_count": compare_count,
            "difference": company_count - compare_count,
        })

    # Sort by absolute difference descending, then limit
    result.sort(key=lambda x: abs(x["difference"]), reverse=True)
    return result[:limit]


def get_trending_words(company=None, months=3):
    """
    Find words that are increasing or decreasing in frequency over the
    last *months* months.  Compares the most recent month against the
    average of the preceding months.

    Returns a dict:
    {"trending_up": [{"word": str, "current": int, "previous_avg": float,
                      "change_pct": float}, ...],
     "trending_down": [...]}
    """
    # Reuse the same cached monthly word data (avoids duplicate DB fetch + tokenization)
    sorted_months, month_counters = _get_monthly_word_data(company, months)

    if len(sorted_months) < 2:
        return {"trending_up": [], "trending_down": []}

    current_month = sorted_months[-1]
    previous_months = sorted_months[:-1]
    current_counter = month_counters[current_month]

    # Compute previous-month averages for every word seen in any month
    all_words = set(current_counter.keys())
    for m in previous_months:
        all_words |= set(month_counters[m].keys())

    trending_up = []
    trending_down = []

    for word in all_words:
        current = current_counter.get(word, 0)
        prev_counts = [month_counters[m].get(word, 0) for m in previous_months]
        previous_avg = sum(prev_counts) / len(prev_counts) if prev_counts else 0

        if previous_avg == 0 and current == 0:
            continue

        if previous_avg > 0:
            change_pct = round(((current - previous_avg) / previous_avg) * 100, 2)
        elif current > 0:
            change_pct = 100.0
        else:
            change_pct = 0.0

        entry = {
            "word": word,
            "current": current,
            "previous_avg": round(previous_avg, 2),
            "change_pct": change_pct,
        }

        if change_pct > 0:
            trending_up.append(entry)
        elif change_pct < 0:
            trending_down.append(entry)

    # Sort by magnitude of change
    trending_up.sort(key=lambda x: x["change_pct"], reverse=True)
    trending_down.sort(key=lambda x: x["change_pct"])

    return {"trending_up": trending_up, "trending_down": trending_down}
