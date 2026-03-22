#!/usr/bin/env python3
"""Load complaints CSV data into the SQLite database."""

import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dashboard.app import create_app
from models.database import db, Complaint

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'ai', 'complaintssyf.csv')
# Fallback: check in data/ directory
CSV_PATH_ALT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'complaintssyf.csv')


def parse_date(date_str):
    if not date_str:
        return None
    for fmt in ('%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def load_csv(csv_path=None):
    if csv_path is None:
        if os.path.exists(CSV_PATH):
            csv_path = CSV_PATH
        elif os.path.exists(CSV_PATH_ALT):
            csv_path = CSV_PATH_ALT
        else:
            print(f"CSV not found at {CSV_PATH} or {CSV_PATH_ALT}")
            print("Please provide the CSV path as an argument.")
            sys.exit(1)

    app = create_app()

    with app.app_context():
        existing_count = Complaint.query.count()
        print(f"Existing complaints in DB: {existing_count}")

        added = 0
        skipped = 0

        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            batch = []

            for row in reader:
                complaint_id = row.get('Complaint ID', '').strip()
                if not complaint_id:
                    skipped += 1
                    continue

                try:
                    cid = int(complaint_id)
                except ValueError:
                    skipped += 1
                    continue

                # Check for duplicate
                if Complaint.query.filter_by(complaint_id=cid).first():
                    skipped += 1
                    continue

                tags = row.get('Tags', '').strip()
                if tags == 'None' or tags == '':
                    tags = None

                narrative = row.get('Consumer complaint narrative', '').strip()
                if not narrative:
                    narrative = None

                complaint = Complaint(
                    complaint_id=cid,
                    date_received=parse_date(row.get('Date received', '')),
                    product=row.get('Product', '').strip(),
                    sub_product=row.get('Sub-product', '').strip(),
                    issue=row.get('Issue', '').strip(),
                    sub_issue=row.get('Sub-issue', '').strip(),
                    narrative=narrative,
                    company_public_response=row.get('Company public response', '').strip(),
                    company=row.get('Company', '').strip(),
                    state=row.get('State', '').strip(),
                    zip_code=row.get('ZIP code', '').strip(),
                    tags=tags,
                    consumer_consent=row.get('Consumer consent provided?', '').strip(),
                    submitted_via=row.get('Submitted via', '').strip(),
                    date_sent_to_company=parse_date(row.get('Date sent to company', '')),
                    company_response=row.get('Company response to consumer', '').strip(),
                    timely_response=row.get('Timely response?', '').strip() == 'Yes',
                    consumer_disputed=row.get('Consumer disputed?', '').strip(),
                )
                batch.append(complaint)

                if len(batch) >= 500:
                    db.session.add_all(batch)
                    db.session.commit()
                    added += len(batch)
                    print(f"  Imported {added} complaints...")
                    batch = []

            if batch:
                db.session.add_all(batch)
                db.session.commit()
                added += len(batch)

        total = Complaint.query.count()
        print(f"\nDone! Added: {added}, Skipped (duplicates): {skipped}, Total in DB: {total}")


if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else None
    load_csv(path)
