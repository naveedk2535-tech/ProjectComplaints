import requests
from datetime import datetime, date
from models.database import db, Complaint


CFPB_BASE_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"


def fetch_complaints(company=None, date_min=None, size=100, offset=0):
    params = {
        'size': size,
        'frm': offset,
        'sort': 'created_date_desc',
        'no_aggs': 'true',
    }
    if company:
        params['company'] = company
    if date_min:
        params['date_received_min'] = date_min.strftime('%Y-%m-%d')

    try:
        resp = requests.get(CFPB_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('hits', {}).get('hits', []), data.get('hits', {}).get('total', {}).get('value', 0)
    except Exception as e:
        print(f"CFPB API error: {e}")
        return [], 0


def map_cfpb_record(hit):
    s = hit.get('_source', {})

    def parse_date(d):
        if not d:
            return None
        try:
            return datetime.strptime(d[:10], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None

    return {
        'complaint_id': int(s.get('complaint_id', 0)),
        'date_received': parse_date(s.get('date_received')),
        'product': s.get('product', ''),
        'sub_product': s.get('sub_product', ''),
        'issue': s.get('issue', ''),
        'sub_issue': s.get('sub_issue', ''),
        'narrative': s.get('complaint_what_happened', ''),
        'company_public_response': s.get('company_public_response', ''),
        'company': s.get('company', ''),
        'state': s.get('state', ''),
        'zip_code': s.get('zip_code', ''),
        'tags': s.get('tags', ''),
        'consumer_consent': s.get('consumer_consent_provided', ''),
        'submitted_via': s.get('submitted_via', ''),
        'date_sent_to_company': parse_date(s.get('date_sent_to_company')),
        'company_response': s.get('company_response', ''),
        'timely_response': s.get('timely', 'Yes') == 'Yes',
        'consumer_disputed': s.get('consumer_disputed', 'N/A'),
    }


def refresh_data(company=None):
    # Find the most recent complaint date
    latest = db.session.query(db.func.max(Complaint.date_received)).scalar()
    date_min = latest if latest else date(2020, 1, 1)

    total_added = 0
    offset = 0

    while True:
        hits, total = fetch_complaints(company=company, date_min=date_min, size=100, offset=offset)
        if not hits:
            break

        for hit in hits:
            record = map_cfpb_record(hit)
            if not record['complaint_id']:
                continue

            existing = Complaint.query.filter_by(complaint_id=record['complaint_id']).first()
            if existing:
                continue

            complaint = Complaint(**record)
            db.session.add(complaint)
            total_added += 1

        db.session.commit()
        offset += 100

        if offset >= total or offset >= 5000:  # Safety limit
            break

    return {'added': total_added, 'total_in_db': Complaint.query.count()}


def search_all_banks(limit=20):
    """Fetch complaint counts for top banks from CFPB API"""
    try:
        params = {
            'size': 0,
            'date_received_min': '2023-01-01',
        }
        resp = requests.get(CFPB_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        company_aggs = data.get('aggregations', {}).get('company', {}).get('company', {}).get('buckets', [])
        return [{'company': b['key'], 'count': b['doc_count']} for b in company_aggs[:limit]]
    except Exception as e:
        print(f"CFPB search error: {e}")
        return []
