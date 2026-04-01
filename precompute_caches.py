#!/usr/bin/env python3
"""Pre-compute dashboard file caches for all companies.

Run after data refresh or deployment to make all dropdown switches instant.
Usage: python precompute_caches.py

On PythonAnywhere, set up as a scheduled task:
  cd /home/zziai39/ProjectComplaints && python precompute_caches.py
"""
import os
import sys
import time
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dashboard.app import create_app

app = create_app()

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'cache')
os.makedirs(CACHE_DIR, exist_ok=True)


def precompute():
    with app.app_context():
        from services.analytics import get_companies

        # Clear old caches
        for f in os.listdir(CACHE_DIR):
            if f.startswith('dash_') and f.endswith('.json'):
                os.remove(os.path.join(CACHE_DIR, f))
        print("Cleared old caches")

        companies = get_companies()
        print(f"Found {len(companies)} companies")

        # Precompute industry view + each company for months=12 (default)
        targets = [('', 'Industry')] + [(c['company'], c['company']) for c in companies]

        with app.test_client() as client:
            # Need to login first
            from models.database import User
            user = User.query.filter_by(role='admin').first()
            if not user:
                user = User.query.first()
            if not user:
                print("ERROR: No users found, cannot authenticate")
                return
            client.post('/login', data={'email': user.email, 'password': '!admin123!'})

            total_start = time.time()
            for company, label in targets:
                t0 = time.time()
                params = {'months': '12'}
                if company:
                    params['company'] = company
                r = client.get('/api/dashboard-data', query_string=params)
                elapsed = (time.time() - t0) * 1000
                status = 'OK' if r.status_code == 200 else f'FAIL({r.status_code})'
                print(f"  {status} {label:45s} {elapsed:6.0f}ms")

            total = time.time() - total_start
            print(f"\nDone! Precomputed {len(targets)} caches in {total:.1f}s")
            print(f"Cache dir: {CACHE_DIR}")
            print(f"Files: {len([f for f in os.listdir(CACHE_DIR) if f.endswith('.json')])}")


if __name__ == '__main__':
    precompute()
