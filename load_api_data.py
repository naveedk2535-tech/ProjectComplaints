#!/usr/bin/env python3
"""
Load complaints data from CFPB API for all major financial institutions.
Uses date-range windowing to work around API pagination limits.
"""

import os
import sys
import time
import requests
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dashboard.app import create_app
from models.database import db, Complaint, MonthlyVolume

CFPB_API = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

TARGET_COMPANIES = [
    "CAPITAL ONE FINANCIAL CORPORATION",
    "JPMORGAN CHASE & CO.",
    "WELLS FARGO & COMPANY",
    "BANK OF AMERICA, NATIONAL ASSOCIATION",
    "CITIBANK, N.A.",
    "SYNCHRONY FINANCIAL",
    "NAVY FEDERAL CREDIT UNION",
    "AMERICAN EXPRESS COMPANY",
    "DISCOVER BANK",
    "Bread Financial Holdings, Inc.",
    "U.S. BANCORP",
    "ALLY FINANCIAL INC.",
    "PNC Bank N.A.",
    "TD BANK US HOLDING COMPANY",
    "TRUIST FINANCIAL CORPORATION",
    "Paypal Holdings, Inc",
    "Block, Inc.",
    "Chime Financial Inc",
    "USAA SAVINGS BANK",
    "GOLDMAN SACHS BANK USA",
]

START_DATE = date(2023, 4, 1)  # Latest 36 months


def parse_date(d):
    if not d:
        return None
    try:
        return datetime.strptime(d[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def fetch_month(company, year, month):
    """Fetch up to 100 complaints for a company in a specific month."""
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)

    try:
        resp = requests.get(CFPB_API, params={
            'company': company,
            'size': 100,
            'sort': 'created_date_desc',
            'no_aggs': 'true',
            'date_received_min': month_start.isoformat(),
            'date_received_max': month_end.isoformat(),
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('hits', {}).get('hits', []), data.get('hits', {}).get('total', {}).get('value', 0)
    except Exception as e:
        print(f"    API error {year}-{month:02d}: {e}")
        return [], 0


def load_company(company):
    """Load complaints for a company by iterating month by month."""
    existing_ids = set(
        r[0] for r in db.session.query(Complaint.complaint_id).filter(
            Complaint.company == company
        ).all()
    )
    print(f"  Existing: {len(existing_ids):,}")

    added = 0
    today = date.today()
    current = START_DATE

    while current <= today:
        year, month = current.year, current.month
        hits, total_in_month = fetch_month(company, year, month)

        batch = []
        for hit in hits:
            s = hit.get('_source', {})
            cid = int(s.get('complaint_id', 0))
            if not cid or cid in existing_ids:
                continue
            existing_ids.add(cid)
            tags = s.get('tags', '') or None
            if tags == 'None':
                tags = None
            batch.append(Complaint(
                complaint_id=cid,
                date_received=parse_date(s.get('date_received')),
                product=s.get('product', ''),
                sub_product=s.get('sub_product', ''),
                issue=s.get('issue', ''),
                sub_issue=s.get('sub_issue', ''),
                narrative=s.get('complaint_what_happened', '') or None,
                company_public_response=s.get('company_public_response', ''),
                company=s.get('company', ''),
                state=s.get('state', ''),
                zip_code=s.get('zip_code', ''),
                tags=tags,
                consumer_consent=s.get('consumer_consent_provided', ''),
                submitted_via=s.get('submitted_via', ''),
                date_sent_to_company=parse_date(s.get('date_sent_to_company')),
                company_response=s.get('company_response', ''),
                timely_response=s.get('timely', 'Yes') == 'Yes',
                consumer_disputed=s.get('consumer_disputed', 'N/A'),
            ))

        # Store actual total for this month (from CFPB API, not our sample)
        if total_in_month > 0:
            month_key = f"{year}-{month:02d}"
            mv = MonthlyVolume.query.filter_by(company=company, month=month_key).first()
            if mv:
                mv.total_complaints = total_in_month
                mv.last_updated = datetime.utcnow()
            else:
                mv = MonthlyVolume(company=company, month=month_key, total_complaints=total_in_month)
                db.session.add(mv)

        if batch:
            db.session.add_all(batch)
            db.session.commit()
            added += len(batch)

        if total_in_month > 0:
            print(f"    {year}-{month:02d}: {len(batch)} new / {total_in_month} total in month")

        # Next month
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)

        time.sleep(0.15)

    final = Complaint.query.filter_by(company=company).count()
    print(f"  Done! +{added:,} | Total: {final:,}")
    return added


def main():
    app = create_app()
    with app.app_context():
        total_before = Complaint.query.count()
        print(f"Database: {total_before:,} complaints")
        print(f"Loading {len(TARGET_COMPANIES)} companies | {START_DATE} to present")
        sys.stdout.flush()

        grand_total = 0
        for i, company in enumerate(TARGET_COMPANIES):
            print(f"\n[{i+1}/{len(TARGET_COMPANIES)}] {company}")
            sys.stdout.flush()
            added = load_company(company)
            grand_total += added

        total_after = Complaint.query.count()
        companies = db.session.query(
            Complaint.company, db.func.count()
        ).group_by(Complaint.company).order_by(db.func.count().desc()).all()

        print(f"\n{'='*60}")
        print(f"DONE! +{grand_total:,} new | Total: {total_after:,}")
        print(f"{'='*60}")
        for c, cnt in companies:
            print(f"  {c}: {cnt:,}")
        sys.stdout.flush()


if __name__ == '__main__':
    main()
