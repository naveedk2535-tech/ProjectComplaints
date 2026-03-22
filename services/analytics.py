from models.database import db, Complaint, BankProfile
from sqlalchemy import func, extract, case, desc
from datetime import datetime, timedelta


def get_kpis(company=None, date_from=None, date_to=None):
    q = Complaint.query
    if company:
        q = q.filter(Complaint.company == company)
    if date_from:
        q = q.filter(Complaint.date_received >= date_from)
    if date_to:
        q = q.filter(Complaint.date_received <= date_to)

    total = q.count()
    if total == 0:
        return {'total': 0, 'resolution_rate': 0, 'monetary_relief_rate': 0,
                'timely_rate': 0, 'in_progress': 0, 'with_narrative': 0}

    closed = q.filter(Complaint.company_response.like('Closed%')).count()
    monetary = q.filter(Complaint.company_response == 'Closed with monetary relief').count()
    timely = q.filter(Complaint.timely_response == True).count()
    in_progress = q.filter(Complaint.company_response == 'In progress').count()
    with_narrative = q.filter(Complaint.narrative.isnot(None), Complaint.narrative != '').count()

    return {
        'total': total,
        'resolution_rate': round(closed / total * 100, 1) if total else 0,
        'monetary_relief_rate': round(monetary / total * 100, 1) if total else 0,
        'timely_rate': round(timely / total * 100, 1) if total else 0,
        'in_progress': in_progress,
        'with_narrative': with_narrative,
        'closed': closed,
        'monetary': monetary,
    }


def get_monthly_trend(company=None, product=None, months=24):
    q = db.session.query(
        func.strftime('%Y-%m', Complaint.date_received).label('month'),
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    if product:
        q = q.filter(Complaint.product == product)

    q = q.group_by('month').order_by('month')
    results = q.all()

    if months and len(results) > months:
        results = results[-months:]

    return [{'month': r.month, 'count': r.count} for r in results]


def get_product_breakdown(company=None):
    q = db.session.query(
        Complaint.product,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.group_by(Complaint.product).order_by(desc('count'))
    return [{'product': r.product, 'count': r.count} for r in q.all()]


def get_issue_breakdown(company=None, product=None, limit=15):
    q = db.session.query(
        Complaint.issue,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    if product:
        q = q.filter(Complaint.product == product)
    q = q.group_by(Complaint.issue).order_by(desc('count')).limit(limit)
    return [{'issue': r.issue, 'count': r.count} for r in q.all()]


def get_response_breakdown(company=None):
    q = db.session.query(
        Complaint.company_response,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.group_by(Complaint.company_response).order_by(desc('count'))
    return [{'response': r.company_response, 'count': r.count} for r in q.all()]


def get_state_breakdown(company=None, limit=50):
    q = db.session.query(
        Complaint.state,
        func.count().label('count'),
        func.sum(case(
            (Complaint.company_response == 'Closed with monetary relief', 1),
            else_=0
        )).label('monetary_count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.filter(Complaint.state.isnot(None), Complaint.state != '')
    q = q.group_by(Complaint.state).order_by(desc('count')).limit(limit)

    results = []
    for r in q.all():
        results.append({
            'state': r.state,
            'count': r.count,
            'monetary_count': r.monetary_count or 0,
            'monetary_rate': round((r.monetary_count or 0) / r.count * 100, 1) if r.count else 0
        })
    return results


def get_submission_channels(company=None):
    q = db.session.query(
        Complaint.submitted_via,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.group_by(Complaint.submitted_via).order_by(desc('count'))
    return [{'channel': r.submitted_via, 'count': r.count} for r in q.all()]


def get_tags_analysis(company=None):
    q = db.session.query(
        Complaint.tags,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.filter(Complaint.tags.isnot(None), Complaint.tags != '', Complaint.tags != 'None')
    q = q.group_by(Complaint.tags).order_by(desc('count'))
    return [{'tag': r.tags, 'count': r.count} for r in q.all()]


def get_health_score(company=None):
    kpis = get_kpis(company=company)
    if kpis['total'] == 0:
        return {'score': 0, 'grade': 'N/A', 'components': {}}

    resolution_score = min(kpis['resolution_rate'], 100)
    monetary_score = min(kpis['monetary_relief_rate'] * 5, 100)  # 20% = 100
    timely_score = min(kpis['timely_rate'], 100)

    # Trend score: fewer complaints recently = better
    trend = get_monthly_trend(company=company, months=6)
    trend_score = 50
    if len(trend) >= 3:
        recent = sum(t['count'] for t in trend[-3:])
        older = sum(t['count'] for t in trend[:3])
        if older > 0:
            change = (recent - older) / older
            trend_score = max(0, min(100, 50 - change * 50))

    score = (
        resolution_score * 0.30 +
        monetary_score * 0.20 +
        timely_score * 0.15 +
        trend_score * 0.20 +
        (100 - min(kpis['with_narrative'] / max(kpis['total'], 1) * 100, 100)) * 0.15
    )
    score = round(score, 1)

    if score >= 80:
        grade = 'A'
    elif score >= 65:
        grade = 'B'
    elif score >= 50:
        grade = 'C'
    elif score >= 35:
        grade = 'D'
    else:
        grade = 'F'

    return {
        'score': score,
        'grade': grade,
        'components': {
            'resolution': round(resolution_score, 1),
            'monetary_relief': round(monetary_score, 1),
            'timely_response': round(timely_score, 1),
            'trend': round(trend_score, 1),
        }
    }


def get_companies():
    q = db.session.query(
        Complaint.company,
        func.count().label('count')
    ).group_by(Complaint.company).order_by(desc('count'))
    return [{'company': r.company, 'count': r.count} for r in q.all()]


def get_bank_comparison():
    companies = get_companies()[:20]
    results = []
    for c in companies:
        kpis = get_kpis(company=c['company'])
        health = get_health_score(company=c['company'])
        results.append({
            'company': c['company'],
            'total_complaints': c['count'],
            'resolution_rate': kpis['resolution_rate'],
            'monetary_relief_rate': kpis['monetary_relief_rate'],
            'timely_rate': kpis['timely_rate'],
            'health_score': health['score'],
            'health_grade': health['grade'],
        })
    return results


def get_product_response_crosstab(company=None):
    q = db.session.query(
        Complaint.product,
        Complaint.company_response,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.group_by(Complaint.product, Complaint.company_response)
    rows = q.all()

    products = {}
    responses = set()
    for r in rows:
        if r.product not in products:
            products[r.product] = {}
        products[r.product][r.company_response] = r.count
        responses.add(r.company_response)

    return {'products': products, 'responses': sorted(responses)}


# ── Client Details tab functions ──────────────────────────────────────────


def get_monthly_trend_by_product(company, months=12):
    """Monthly complaint counts broken down by product for a specific company."""
    q = db.session.query(
        func.strftime('%Y-%m', Complaint.date_received).label('month'),
        Complaint.product,
        func.count().label('count')
    ).filter(Complaint.company == company)

    q = q.group_by('month', Complaint.product).order_by('month')
    results = q.all()

    # Trim to requested number of months
    months_seen = sorted(set(r.month for r in results))
    if months and len(months_seen) > months:
        cutoff = months_seen[-months]
        results = [r for r in results if r.month >= cutoff]

    return [{'month': r.month, 'product': r.product, 'count': r.count} for r in results]


def get_monthly_trend_by_response(company, months=12):
    """Monthly counts broken down by company_response type."""
    q = db.session.query(
        func.strftime('%Y-%m', Complaint.date_received).label('month'),
        Complaint.company_response,
        func.count().label('count')
    ).filter(Complaint.company == company)

    q = q.group_by('month', Complaint.company_response).order_by('month')
    results = q.all()

    months_seen = sorted(set(r.month for r in results))
    if months and len(months_seen) > months:
        cutoff = months_seen[-months]
        results = [r for r in results if r.month >= cutoff]

    return [{'month': r.month, 'response': r.company_response, 'count': r.count} for r in results]


def get_sub_product_breakdown(company=None, product=None):
    """Sub-product counts, optionally filtered by company and/or product."""
    q = db.session.query(
        Complaint.sub_product,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    if product:
        q = q.filter(Complaint.product == product)
    q = q.filter(Complaint.sub_product.isnot(None), Complaint.sub_product != '')
    q = q.group_by(Complaint.sub_product).order_by(desc('count'))
    return [{'sub_product': r.sub_product, 'count': r.count} for r in q.all()]


def get_issue_resolution_mix(company=None, limit=15):
    """For top issues, show breakdown of response types."""
    # First get top issues
    top_issues_q = db.session.query(
        Complaint.issue,
        func.count().label('count')
    )
    if company:
        top_issues_q = top_issues_q.filter(Complaint.company == company)
    top_issues_q = top_issues_q.group_by(Complaint.issue).order_by(desc('count')).limit(limit)
    top_issues = [r.issue for r in top_issues_q.all()]

    # Now get response breakdown for each issue
    q = db.session.query(
        Complaint.issue,
        Complaint.company_response,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.filter(Complaint.issue.in_(top_issues))
    q = q.group_by(Complaint.issue, Complaint.company_response)
    rows = q.all()

    result = {}
    for r in rows:
        if r.issue not in result:
            result[r.issue] = {}
        result[r.issue][r.company_response] = r.count

    return result


def get_mom_changes(company=None):
    """Month-over-month changes for key metrics. Compare most recent full month vs previous."""
    # Find the most recent full month (not current partial month)
    today = datetime.utcnow().date()
    # End of last full month
    first_of_current = today.replace(day=1)
    last_month_end = first_of_current - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    # Previous month
    prev_month_end = last_month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    def _query_month(start, end):
        q = Complaint.query.filter(
            Complaint.date_received >= start,
            Complaint.date_received <= end
        )
        if company:
            q = q.filter(Complaint.company == company)
        return q

    current_q = _query_month(last_month_start, last_month_end)
    prev_q = _query_month(prev_month_start, prev_month_end)

    current_total = current_q.count()
    prev_total = prev_q.count()

    current_monetary = current_q.filter(
        Complaint.company_response == 'Closed with monetary relief'
    ).count()
    prev_monetary = prev_q.filter(
        Complaint.company_response == 'Closed with monetary relief'
    ).count()

    volume_change_pct = round((current_total - prev_total) / prev_total * 100, 1) if prev_total else 0
    current_mr_rate = round(current_monetary / current_total * 100, 1) if current_total else 0
    prev_mr_rate = round(prev_monetary / prev_total * 100, 1) if prev_total else 0
    monetary_relief_change_pct = round(current_mr_rate - prev_mr_rate, 1)

    # Issue-level changes
    def _issue_counts(start, end):
        q = db.session.query(
            Complaint.issue,
            func.count().label('count')
        ).filter(
            Complaint.date_received >= start,
            Complaint.date_received <= end
        )
        if company:
            q = q.filter(Complaint.company == company)
        q = q.group_by(Complaint.issue)
        return {r.issue: r.count for r in q.all()}

    current_issues = _issue_counts(last_month_start, last_month_end)
    prev_issues = _issue_counts(prev_month_start, prev_month_end)

    all_issues = set(current_issues.keys()) | set(prev_issues.keys())
    changes = []
    for issue in all_issues:
        cur = current_issues.get(issue, 0)
        prev = prev_issues.get(issue, 0)
        pct = round((cur - prev) / prev * 100, 1) if prev else (100.0 if cur else 0)
        changes.append({'issue': issue, 'current': cur, 'previous': prev, 'change_pct': pct})

    growing = sorted([c for c in changes if c['change_pct'] > 0], key=lambda x: -x['change_pct'])[:5]
    declining = sorted([c for c in changes if c['change_pct'] < 0], key=lambda x: x['change_pct'])[:5]

    return {
        'volume_change_pct': volume_change_pct,
        'monetary_relief_change_pct': monetary_relief_change_pct,
        'top_growing_issues': growing,
        'top_declining_issues': declining,
    }


def get_tags_trend(company=None, months=12):
    """Monthly trend of complaints by tag (Older American, Servicemember, etc.)."""
    q = db.session.query(
        func.strftime('%Y-%m', Complaint.date_received).label('month'),
        Complaint.tags,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.filter(Complaint.tags.isnot(None), Complaint.tags != '', Complaint.tags != 'None')
    q = q.group_by('month', Complaint.tags).order_by('month')
    results = q.all()

    months_seen = sorted(set(r.month for r in results))
    if months and len(months_seen) > months:
        cutoff = months_seen[-months]
        results = [r for r in results if r.month >= cutoff]

    return [{'month': r.month, 'tag': r.tags, 'count': r.count} for r in results]


def get_channel_trend(company=None, months=12):
    """Monthly trend by submission channel."""
    q = db.session.query(
        func.strftime('%Y-%m', Complaint.date_received).label('month'),
        Complaint.submitted_via,
        func.count().label('count')
    )
    if company:
        q = q.filter(Complaint.company == company)
    q = q.filter(Complaint.submitted_via.isnot(None), Complaint.submitted_via != '')
    q = q.group_by('month', Complaint.submitted_via).order_by('month')
    results = q.all()

    months_seen = sorted(set(r.month for r in results))
    if months and len(months_seen) > months:
        cutoff = months_seen[-months]
        results = [r for r in results if r.month >= cutoff]

    return [{'month': r.month, 'channel': r.submitted_via, 'count': r.count} for r in results]


def get_peer_companies(company, limit=5):
    """Find top N companies with similar product mix and highest complaint volumes.

    Uses local DB data. If only one company exists locally, falls back to
    CFPB external data for top companies.
    """
    # Get products offered by the target company
    target_products = db.session.query(Complaint.product).filter(
        Complaint.company == company
    ).distinct().all()
    target_product_set = {r.product for r in target_products}

    if not target_product_set:
        return []

    # Find other companies that share at least one product, ranked by complaint volume
    q = db.session.query(
        Complaint.company,
        func.count().label('count')
    ).filter(
        Complaint.company != company,
        Complaint.product.in_(target_product_set)
    ).group_by(Complaint.company).order_by(desc('count')).limit(limit)

    peers = [{'company': r.company, 'count': r.count} for r in q.all()]

    # Fallback to external CFPB data if we have very few local peers
    if len(peers) < 2:
        try:
            from services.external_apis import cfpb_get_top_companies
            external = cfpb_get_top_companies(limit=limit + 5)
            for ext in external:
                if ext['company'] != company and len(peers) < limit:
                    if not any(p['company'] == ext['company'] for p in peers):
                        peers.append({'company': ext['company'], 'count': ext.get('complaints', 0)})
        except Exception:
            pass

    return peers[:limit]


def get_peer_comparison(company, peers):
    """Compare a company against a list of peer companies across key metrics.

    Args:
        company: The target company name.
        peers: List of peer company names (strings).

    Returns:
        List of dicts, one per company (target + peers), each with:
        total_complaints, resolution_rate, monetary_relief_rate, top_product, top_issue.
    """
    all_companies = [company] + [p for p in peers if p != company]
    results = []

    for comp in all_companies:
        kpis = get_kpis(company=comp)

        # Top product
        top_product_q = db.session.query(
            Complaint.product,
            func.count().label('count')
        ).filter(Complaint.company == comp).group_by(
            Complaint.product
        ).order_by(desc('count')).limit(1).first()

        # Top issue
        top_issue_q = db.session.query(
            Complaint.issue,
            func.count().label('count')
        ).filter(Complaint.company == comp).group_by(
            Complaint.issue
        ).order_by(desc('count')).limit(1).first()

        results.append({
            'company': comp,
            'total_complaints': kpis['total'],
            'resolution_rate': kpis['resolution_rate'],
            'monetary_relief_rate': kpis['monetary_relief_rate'],
            'top_product': top_product_q.product if top_product_q else None,
            'top_issue': top_issue_q.issue if top_issue_q else None,
        })

    return results


def get_issue_sub_issue_tree(company=None, limit=10):
    """Top issues with their sub-issues nested."""
    # Get top issues
    top_q = db.session.query(
        Complaint.issue,
        func.count().label('count')
    )
    if company:
        top_q = top_q.filter(Complaint.company == company)
    top_q = top_q.group_by(Complaint.issue).order_by(desc('count')).limit(limit)
    top_issues = top_q.all()

    issue_names = [r.issue for r in top_issues]

    # Get sub-issues for those top issues
    sub_q = db.session.query(
        Complaint.issue,
        Complaint.sub_issue,
        func.count().label('count')
    )
    if company:
        sub_q = sub_q.filter(Complaint.company == company)
    sub_q = sub_q.filter(
        Complaint.issue.in_(issue_names),
        Complaint.sub_issue.isnot(None),
        Complaint.sub_issue != ''
    ).group_by(Complaint.issue, Complaint.sub_issue).order_by(Complaint.issue, desc('count'))
    sub_rows = sub_q.all()

    # Build lookup
    sub_map = {}
    for r in sub_rows:
        if r.issue not in sub_map:
            sub_map[r.issue] = []
        sub_map[r.issue].append({'sub_issue': r.sub_issue, 'count': r.count})

    return [
        {
            'issue': r.issue,
            'count': r.count,
            'sub_issues': sub_map.get(r.issue, [])
        }
        for r in top_issues
    ]
