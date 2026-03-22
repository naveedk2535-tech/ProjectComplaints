"""
External Data Source Integrations for ComplaintsHoo
Pulls data from multiple free government and regulatory APIs to build
comprehensive financial institution profiles.
"""

import requests
import json
from datetime import datetime, date
from models.database import db, BankProfile


# ══════════════════════════════════════════════════════════════════
# 1. FDIC BankFind API — Free, no key needed
#    Bank institution data, financials, branch counts
#    Docs: https://banks.data.fdic.gov/docs/
# ══════════════════════════════════════════════════════════════════

FDIC_BASE = "https://banks.data.fdic.gov/api"
FDIC_BASE_ALT = "https://api.fdic.gov/banks"  # Alternative endpoint


def _fdic_request(endpoint, params, timeout=15):
    """Try both FDIC API endpoints (primary and alternative)"""
    for base in [FDIC_BASE_ALT, FDIC_BASE]:
        try:
            resp = requests.get(f"{base}/{endpoint}", params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            continue
    return None


def fdic_search_institutions(name=None, state=None, limit=50):
    """Search FDIC for bank institutions"""
    params = {
        'filters': 'ACTIVE:1',
        'limit': limit,
        'sort_by': 'ASSET',
        'sort_order': 'DESC',
        'fields': 'REPDTE,CERT,NAME,CITY,STNAME,STALP,ASSET,DEP,NETINC,ROA,ROE,OFFDOM,ACTIVE',
    }
    if name:
        params['search'] = name
        params['filters'] = 'ACTIVE:1'
    if state:
        params['filters'] += f' AND STALP:"{state}"'

    try:
        data = _fdic_request('financials', params)
        if not data:
            # Fallback: try institutions endpoint
            data = _fdic_request('institutions', params)
        if not data:
            return []

        results = []
        for item in data.get('data', []):
            d = item.get('data', {})
            results.append({
                'cert': d.get('CERT'),
                'name': d.get('NAME', ''),
                'city': d.get('CITY', ''),
                'state': d.get('STNAME', ''),
                'total_assets': d.get('ASSET', 0),
                'total_deposits': d.get('DEP', 0),
                'net_income': d.get('NETINC', 0),
                'roa': d.get('ROA', 0),
                'roe': d.get('ROE', 0),
                'offices': d.get('OFFDOM', 0),
                'active': d.get('ACTIVE', 1),
                'report_date': d.get('REPDTE', ''),
            })
        return results
    except Exception as e:
        print(f"FDIC API error: {e}")
        return []


def fdic_get_institution(cert_number):
    """Get detailed info for a specific FDIC-insured institution"""
    try:
        data = _fdic_request('financials', {
            'filters': f'CERT:{cert_number}',
            'limit': 1,
            'sort_by': 'REPDTE',
            'sort_order': 'DESC',
            'fields': 'REPDTE,CERT,NAME,CITY,STNAME,ASSET,DEP,NETINC,ROA,ROE,OFFDOM,NTLNLS,NCLNLS',
        })
        if not data:
            return None
        items = data.get('data', [])
        if items:
            d = items[0].get('data', {})
            return {
                'cert': d.get('CERT'),
                'name': d.get('NAME', ''),
                'city': d.get('CITY', ''),
                'state': d.get('STNAME', ''),
                'total_assets': d.get('ASSET', 0),
                'total_deposits': d.get('DEP', 0),
                'net_income': d.get('NETINC', 0),
                'roa': d.get('ROA', 0),
                'roe': d.get('ROE', 0),
                'offices': d.get('OFFDOM', 0),
                'noncurrent_loans_ratio': d.get('NTLNLS', 0),
                'net_chargeoffs_ratio': d.get('NCLNLS', 0),
                'report_date': d.get('REPDTE', ''),
            }
        return None
    except Exception as e:
        print(f"FDIC institution error: {e}")
        return None


def fdic_get_failures(limit=100):
    """Get recent bank failures from FDIC"""
    try:
        data = _fdic_request('failures', {
            'limit': limit,
            'sort_by': 'FAILDATE',
            'sort_order': 'DESC',
            'fields': 'CERT,NAME,CITY,PSTALP,FAILDATE,SAVR,RESTYPE,COST,QBFASSET,QBFDEP',
        })
        if not data:
            return []
        results = []
        for item in data.get('data', []):
            d = item.get('data', {})
            results.append({
                'cert': d.get('CERT'),
                'name': d.get('NAME', ''),
                'city': d.get('CITY', ''),
                'state': d.get('PSTALP', d.get('ST', '')),
                'fail_date': d.get('FAILDATE', ''),
                'acquiring_institution': d.get('SAVR', ''),
                'resolution_type': d.get('RESTYPE', ''),
                'estimated_loss': d.get('COST', 0),
                'total_assets': d.get('QBFASSET', 0),
                'total_deposits': d.get('QBFDEP', 0),
            })
        return results
    except Exception as e:
        print(f"FDIC failures error: {e}")
        return []


def fdic_get_history(cert_number, limit=20):
    """Get financial history for an institution over time"""
    try:
        data = _fdic_request('financials', {
            'filters': f'CERT:{cert_number}',
            'limit': limit,
            'sort_by': 'REPDTE',
            'sort_order': 'DESC',
            'fields': 'REPDTE,ASSET,DEP,NETINC,ROA,ROE,NTLNLS,NCLNLS',
        })
        if not data:
            return []
        results = []
        for item in data.get('data', []):
            d = item.get('data', {})
            results.append({
                'report_date': d.get('REPDTE', ''),
                'total_assets': d.get('ASSET', 0),
                'total_deposits': d.get('DEP', 0),
                'net_income': d.get('NETINC', 0),
                'roa': d.get('ROA', 0),
                'roe': d.get('ROE', 0),
                'noncurrent_loans_ratio': d.get('NTLNLS', 0),
                'net_chargeoffs_ratio': d.get('NCLNLS', 0),
            })
        return results
    except Exception as e:
        print(f"FDIC history error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 2. SEC EDGAR API — Free, requires User-Agent header
#    Company filings, enforcement actions
#    Docs: https://www.sec.gov/search-filings/edgar-full-text-search
# ══════════════════════════════════════════════════════════════════

SEC_HEADERS = {'User-Agent': 'ComplaintsHoo/1.0 (complaints-research)'}
SEC_BASE = "https://efts.sec.gov/LATEST"


def sec_search_company(company_name, limit=10):
    """Search SEC EDGAR for company filings"""
    try:
        resp = requests.get(
            f"{SEC_BASE}/search-index",
            params={
                'q': company_name,
                'dateRange': 'custom',
                'startdt': '2023-01-01',
                'enddt': date.today().isoformat(),
                'forms': '10-K,10-Q,8-K',
            },
            headers=SEC_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get('hits', {}).get('hits', [])
        results = []
        for hit in hits[:limit]:
            s = hit.get('_source', {})
            results.append({
                'company': s.get('display_names', [''])[0] if s.get('display_names') else '',
                'form_type': s.get('form_type', ''),
                'filed_at': s.get('file_date', ''),
                'description': s.get('display_date_filed', ''),
                'url': f"https://www.sec.gov/Archives/edgar/data/{s.get('file_num', '')}" if s.get('file_num') else '',
            })
        return results
    except Exception as e:
        print(f"SEC search error: {e}")
        return []


def sec_get_enforcement_actions(limit=20):
    """Get recent SEC enforcement actions (litigation releases)"""
    try:
        resp = requests.get(
            f"{SEC_BASE}/search-index",
            params={
                'q': 'enforcement action bank financial',
                'forms': 'LR',  # Litigation Releases
                'dateRange': 'custom',
                'startdt': '2023-01-01',
                'enddt': date.today().isoformat(),
            },
            headers=SEC_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get('hits', {}).get('hits', [])
        results = []
        for hit in hits[:limit]:
            s = hit.get('_source', {})
            results.append({
                'title': s.get('display_names', [''])[0] if s.get('display_names') else s.get('file_description', ''),
                'form_type': s.get('form_type', ''),
                'date': s.get('file_date', ''),
                'description': s.get('file_description', ''),
            })
        return results
    except Exception as e:
        print(f"SEC enforcement error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 3. NCUA Credit Union API — Free, no key
#    Credit union financial data
#    Docs: https://www.ncua.gov/analysis/credit-union-corporate-call-report-data
# ══════════════════════════════════════════════════════════════════

NCUA_BASE = "https://www.ncua.gov/files/publications"


def ncua_search_credit_unions(name=None, state=None, limit=50):
    """Search NCUA for credit unions via their API"""
    try:
        # NCUA has a mapping/search endpoint
        params = {'limit': limit}
        url = "https://mapping.ncua.gov/api/SearchCU"
        if name:
            params['name'] = name
        if state:
            params['state'] = state

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data if isinstance(data, list) else []:
            results.append({
                'charter_number': item.get('CharterNumber', ''),
                'name': item.get('CUName', ''),
                'city': item.get('City', ''),
                'state': item.get('State', ''),
                'total_assets': item.get('TotalAssets', 0),
                'members': item.get('NumberOfMembers', 0),
            })
        return results
    except Exception as e:
        print(f"NCUA search error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 4. CFPB Complaint Trends API — Free, no key
#    Aggregate complaint trends across all companies
# ══════════════════════════════════════════════════════════════════

CFPB_BASE = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"


def cfpb_get_trends(company=None, product=None):
    """Get complaint trends from CFPB with aggregations"""
    params = {
        'size': 0,
        'date_received_min': '2023-01-01',
    }
    if company:
        params['company'] = company
    if product:
        params['product'] = product

    try:
        resp = requests.get(CFPB_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        aggs = data.get('aggregations', {})

        result = {
            'total': data.get('hits', {}).get('total', {}).get('value', 0),
            'products': [],
            'companies': [],
            'states': [],
            'issues': [],
        }

        # Product aggregation
        prod_buckets = aggs.get('product', {}).get('product', {}).get('buckets', [])
        result['products'] = [{'name': b['key'], 'count': b['doc_count']} for b in prod_buckets]

        # Company aggregation
        comp_buckets = aggs.get('company', {}).get('company', {}).get('buckets', [])
        result['companies'] = [{'name': b['key'], 'count': b['doc_count']} for b in comp_buckets]

        # State aggregation
        state_buckets = aggs.get('state', {}).get('state', {}).get('buckets', [])
        result['states'] = [{'name': b['key'], 'count': b['doc_count']} for b in state_buckets]

        # Issue aggregation
        issue_buckets = aggs.get('issue', {}).get('issue', {}).get('buckets', [])
        result['issues'] = [{'name': b['key'], 'count': b['doc_count']} for b in issue_buckets]

        return result
    except Exception as e:
        print(f"CFPB trends error: {e}")
        return {'total': 0, 'products': [], 'companies': [], 'states': [], 'issues': []}


def cfpb_get_top_companies(limit=25):
    """Get companies with most complaints from CFPB"""
    params = {
        'size': 0,
        'date_received_min': '2023-01-01',
    }
    try:
        resp = requests.get(CFPB_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        buckets = data.get('aggregations', {}).get('company', {}).get('company', {}).get('buckets', [])
        return [{'company': b['key'], 'complaints': b['doc_count']} for b in buckets[:limit]]
    except Exception as e:
        print(f"CFPB top companies error: {e}")
        return []


def cfpb_get_company_detail(company_name):
    """Get detailed complaint breakdown for a specific company"""
    params = {
        'size': 0,
        'company': company_name,
        'date_received_min': '2023-01-01',
    }
    try:
        resp = requests.get(CFPB_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        aggs = data.get('aggregations', {})

        return {
            'company': company_name,
            'total_complaints': data.get('hits', {}).get('total', {}).get('value', 0),
            'products': [{'name': b['key'], 'count': b['doc_count']}
                        for b in aggs.get('product', {}).get('product', {}).get('buckets', [])],
            'issues': [{'name': b['key'], 'count': b['doc_count']}
                      for b in aggs.get('issue', {}).get('issue', {}).get('buckets', [])],
            'states': [{'name': b['key'], 'count': b['doc_count']}
                      for b in aggs.get('state', {}).get('state', {}).get('buckets', [])],
            'timely_responses': aggs.get('timely', {}).get('timely', {}).get('buckets', []),
            'company_responses': [{'name': b['key'], 'count': b['doc_count']}
                                 for b in aggs.get('company_response', {}).get('company_response', {}).get('buckets', [])],
        }
    except Exception as e:
        print(f"CFPB company detail error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 5. FINRA BrokerCheck — Free, no key
#    Broker/firm disclosures and complaints
# ══════════════════════════════════════════════════════════════════

FINRA_BASE = "https://api.brokercheck.finra.org"


def finra_search_firm(name, limit=10):
    """Search FINRA BrokerCheck for a firm"""
    try:
        resp = requests.get(
            f"{FINRA_BASE}/search/firm",
            params={'query': name, 'hl': 'true', 'nrows': limit, 'start': 0, 'r': 25, 'wt': 'json'},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get('hits', {}).get('hits', [])
        results = []
        for hit in hits:
            s = hit.get('_source', {})
            results.append({
                'firm_name': s.get('bc_firm_name', ''),
                'crd_number': s.get('bc_firm_bc_crd_nb', ''),
                'sec_number': s.get('bc_firm_bc_sec_nb', ''),
                'city': s.get('bc_firm_bc_city', ''),
                'state': s.get('bc_firm_bc_state', ''),
                'branch_count': s.get('bc_firm_bc_branch_cnt', 0),
                'disclosure_count': s.get('bc_firm_bc_disclosure_cnt', 0),
                'broker_count': s.get('bc_firm_bc_ia_individuals_cnt', 0),
            })
        return results
    except Exception as e:
        print(f"FINRA search error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 6. FTC Sentinel / Do Not Call Reports — Free data
#    Consumer fraud and complaint data
# ══════════════════════════════════════════════════════════════════

def ftc_get_do_not_call_reports():
    """Get FTC Do Not Call complaint data (annual aggregates from data.gov)"""
    try:
        # FTC publishes data via data.gov CKAN API
        resp = requests.get(
            "https://catalog.data.gov/api/3/action/package_search",
            params={'q': 'FTC consumer sentinel', 'rows': 5},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get('result', {}).get('results', [])
        return [{
            'title': r.get('title', ''),
            'notes': r.get('notes', '')[:200],
            'url': r.get('url', ''),
            'resources': len(r.get('resources', [])),
        } for r in results]
    except Exception as e:
        print(f"FTC data error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# 7. OCC Complaint Data — Annual CSVs
#    Customer complaints about national banks
# ══════════════════════════════════════════════════════════════════

def occ_get_complaint_summary():
    """Return OCC complaint summary info (they publish annual reports)"""
    # OCC doesn't have a live API but publishes annual data
    return {
        'source': 'Office of the Comptroller of the Currency',
        'description': 'Annual customer complaint data for national banks',
        'data_url': 'https://www.occ.gov/topics/consumers-and-communities/consumer-protection/consumer-complaints/index-consumer-complaints.html',
        'note': 'OCC publishes complaint volumes by category annually. Data is integrated via annual report download.',
    }


# ══════════════════════════════════════════════════════════════════
# Master aggregation function
# ══════════════════════════════════════════════════════════════════

def build_comprehensive_bank_profile(bank_name):
    """Build a comprehensive profile for a bank from all available sources"""
    profile = {
        'bank_name': bank_name,
        'last_updated': datetime.utcnow().isoformat(),
        'sources': {},
    }

    # 1. CFPB Complaints
    cfpb = cfpb_get_company_detail(bank_name)
    if cfpb:
        profile['sources']['cfpb'] = {
            'total_complaints': cfpb['total_complaints'],
            'top_products': cfpb['products'][:5],
            'top_issues': cfpb['issues'][:5],
            'top_states': cfpb['states'][:10],
            'company_responses': cfpb.get('company_responses', []),
        }

    # 2. FDIC data
    fdic = fdic_search_institutions(name=bank_name, limit=3)
    if fdic:
        best = fdic[0]
        profile['sources']['fdic'] = {
            'cert': best['cert'],
            'total_assets': best['total_assets'],
            'total_deposits': best['total_deposits'],
            'net_income': best['net_income'],
            'roa': best['roa'],
            'roe': best['roe'],
            'offices': best['offices'],
            'report_date': best['report_date'],
        }
        # Get financial history
        if best['cert']:
            history = fdic_get_history(best['cert'], limit=8)
            if history:
                profile['sources']['fdic']['history'] = history

    # 3. FINRA data
    finra = finra_search_firm(bank_name, limit=3)
    if finra:
        best = finra[0]
        profile['sources']['finra'] = {
            'firm_name': best['firm_name'],
            'crd_number': best['crd_number'],
            'disclosure_count': best['disclosure_count'],
            'branch_count': best['branch_count'],
            'broker_count': best['broker_count'],
        }

    # 4. SEC filings
    sec = sec_search_company(bank_name, limit=5)
    if sec:
        profile['sources']['sec'] = {
            'recent_filings': sec[:5],
        }

    return profile


def get_industry_overview():
    """Get industry-wide overview from multiple sources"""
    overview = {
        'last_updated': datetime.utcnow().isoformat(),
    }

    # CFPB industry-wide
    cfpb = cfpb_get_trends()
    overview['cfpb'] = {
        'total_complaints_since_2023': cfpb['total'],
        'top_companies': cfpb['companies'][:15],
        'top_products': cfpb['products'],
        'top_states': cfpb['states'][:15],
        'top_issues': cfpb['issues'][:10],
    }

    # FDIC bank failures
    failures = fdic_get_failures(limit=20)
    overview['fdic_failures'] = failures[:10]

    # Top banks by assets
    top_banks = fdic_search_institutions(limit=20)
    overview['fdic_top_banks'] = top_banks[:15]

    return overview
