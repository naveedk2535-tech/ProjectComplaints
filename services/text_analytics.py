"""
Text analytics service for analyzing complaint narratives.
Extracts word frequencies, sentiment, and themes from complaint text.
"""

import re
import statistics
from collections import Counter

from models.database import db, Complaint

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


def _tokenize(text):
    """Lowercase and extract alpha-only tokens, filtering stop words."""
    tokens = _TOKEN_RE.findall(text.lower())
    return [t for t in tokens if len(t) > 1 and t not in STOP_WORDS and not t.isdigit()]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_top_words(company=None, limit=50):
    """
    Extract the most frequent words from complaint narratives.

    Returns a list of dicts: [{"word": str, "count": int}, ...]
    """
    rows = _base_query(company).with_entities(Complaint.narrative).all()
    counter = Counter()
    for (narrative,) in rows:
        counter.update(_tokenize(narrative))
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
    rows = _base_query(company).with_entities(Complaint.narrative).all()

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
    rows = _base_query(company).with_entities(Complaint.narrative).all()
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
    rows = _base_query(company).with_entities(Complaint.narrative).all()

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
