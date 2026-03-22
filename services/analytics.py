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
