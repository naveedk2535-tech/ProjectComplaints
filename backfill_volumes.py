#!/usr/bin/env python3
"""Backfill actual monthly complaint volumes from CFPB API for all tracked companies."""
import os, sys, time, requests
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dashboard.app import create_app
from models.database import db, MonthlyVolume

CFPB_API = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

COMPANIES = [
    "CAPITAL ONE FINANCIAL CORPORATION", "JPMORGAN CHASE & CO.", "WELLS FARGO & COMPANY",
    "BANK OF AMERICA, NATIONAL ASSOCIATION", "CITIBANK, N.A.", "SYNCHRONY FINANCIAL",
    "NAVY FEDERAL CREDIT UNION", "AMERICAN EXPRESS COMPANY", "DISCOVER BANK",
    "Bread Financial Holdings, Inc.", "U.S. BANCORP", "ALLY FINANCIAL INC.",
    "PNC Bank N.A.", "TD BANK US HOLDING COMPANY", "TRUIST FINANCIAL CORPORATION",
    "Paypal Holdings, Inc", "Block, Inc.", "Chime Financial Inc", "GOLDMAN SACHS BANK USA",
]

def get_month_total(company, year, month):
    month_start = date(year, month, 1)
    month_end = date(year + (1 if month == 12 else 0), (month % 12) + 1, 1) - timedelta(days=1)
    try:
        r = requests.get(CFPB_API, params={
            'company': company, 'size': 0, 'no_aggs': 'true',
            'date_received_min': month_start.isoformat(),
            'date_received_max': month_end.isoformat(),
        }, timeout=15)
        r.raise_for_status()
        return r.json().get('hits', {}).get('total', {}).get('value', 0)
    except:
        return 0

def main():
    app = create_app()
    with app.app_context():
        print(f"Backfilling monthly volumes for {len(COMPANIES)} companies...")
        sys.stdout.flush()

        today = date.today()
        start = date(2023, 4, 1)

        for i, company in enumerate(COMPANIES):
            print(f"\n[{i+1}/{len(COMPANIES)}] {company}")
            sys.stdout.flush()
            current = start
            while current <= today:
                y, m = current.year, current.month
                month_key = f"{y}-{m:02d}"

                # Skip if already have this month
                existing = MonthlyVolume.query.filter_by(company=company, month=month_key).first()
                if existing and existing.total_complaints > 0:
                    current = date(y + (1 if m == 12 else 0), (m % 12) + 1, 1)
                    continue

                total = get_month_total(company, y, m)
                if total > 0:
                    if existing:
                        existing.total_complaints = total
                    else:
                        db.session.add(MonthlyVolume(company=company, month=month_key, total_complaints=total))
                    print(f"  {month_key}: {total:,}")
                    sys.stdout.flush()

                current = date(y + (1 if m == 12 else 0), (m % 12) + 1, 1)
                time.sleep(0.15)

            db.session.commit()

        # Summary
        total_records = MonthlyVolume.query.count()
        print(f"\nDone! {total_records} monthly volume records saved.")
        sys.stdout.flush()

if __name__ == '__main__':
    main()
