import os
import sys
from functools import wraps
from datetime import datetime, timedelta, date

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from dotenv import load_dotenv

load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.database import db, init_db, User, Complaint, AICommentary


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-key-change-in-production')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'complaints.db'
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.permanent_session_lifetime = timedelta(days=7)

    # Ensure data directory exists
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    os.makedirs(data_dir, exist_ok=True)

    init_db(app)

    # Auto-load CSV if database is empty
    with app.app_context():
        if Complaint.query.count() == 0:
            csv_paths = [
                os.path.join(data_dir, 'complaintssyf.csv'),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'ai', 'complaintssyf.csv'),
            ]
            for csv_path in csv_paths:
                if os.path.exists(csv_path):
                    try:
                        import csv as csv_mod
                        from datetime import datetime as dt_parse
                        added = 0
                        with open(csv_path, 'r', encoding='utf-8-sig') as f:
                            reader = csv_mod.DictReader(f)
                            batch = []
                            for row in reader:
                                cid_str = row.get('Complaint ID', '').strip()
                                if not cid_str:
                                    continue
                                try:
                                    cid = int(cid_str)
                                except ValueError:
                                    continue
                                dr = row.get('Date received', '').strip()
                                date_recv = None
                                for fmt in ('%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d'):
                                    try:
                                        date_recv = dt_parse.strptime(dr, fmt).date()
                                        break
                                    except (ValueError, TypeError):
                                        continue
                                ds = row.get('Date sent to company', '').strip()
                                date_sent = None
                                for fmt in ('%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d'):
                                    try:
                                        date_sent = dt_parse.strptime(ds, fmt).date()
                                        break
                                    except (ValueError, TypeError):
                                        continue
                                tags = row.get('Tags', '').strip()
                                if tags in ('None', ''):
                                    tags = None
                                narrative = row.get('Consumer complaint narrative', '').strip() or None
                                batch.append(Complaint(
                                    complaint_id=cid, date_received=date_recv,
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
                                    date_sent_to_company=date_sent,
                                    company_response=row.get('Company response to consumer', '').strip(),
                                    timely_response=row.get('Timely response?', '').strip() == 'Yes',
                                    consumer_disputed=row.get('Consumer disputed?', '').strip(),
                                ))
                                if len(batch) >= 500:
                                    db.session.add_all(batch)
                                    db.session.commit()
                                    added += len(batch)
                                    batch = []
                            if batch:
                                db.session.add_all(batch)
                                db.session.commit()
                                added += len(batch)
                        app.logger.info(f'Auto-loaded {added} complaints from CSV')
                    except Exception as e:
                        app.logger.error(f'CSV auto-load failed: {e}')
                    break

    # Create default admin + test user
    with app.app_context():
        admin = User.query.filter_by(email=os.getenv('ADMIN_EMAIL', 'admin@complaintshoo.com')).first()
        if not admin:
            admin = User(
                email=os.getenv('ADMIN_EMAIL', 'admin@complaintshoo.com'),
                name='Admin',
                role='admin',
                subscription_status='active',
                is_active=True,
            )
            admin.set_password(os.getenv('ADMIN_PASSWORD', '!admin123!'))
            db.session.add(admin)
            db.session.commit()
        else:
            # Update admin password if it changed
            admin.set_password(os.getenv('ADMIN_PASSWORD', '!admin123!'))
            db.session.commit()

        # Create test user with 30-day trial
        test_user = User.query.filter_by(email='test@complaintshoo.com').first()
        if not test_user:
            test_user = User(
                email='test@complaintshoo.com',
                name='Test User',
                role='user',
                is_active=True,
            )
            test_user.set_password('test123')
            test_user.start_trial()
            db.session.add(test_user)
            db.session.commit()

    # ── Decorators ──────────────────────────────────────────────
    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user_id = session.get('user_id')
            if not user_id:
                if request.is_json or request.path.startswith('/api/'):
                    return jsonify({'error': 'Login required'}), 401
                return redirect(url_for('login'))
            user = User.query.get(user_id)
            if not user or not user.is_active:
                session.clear()
                return redirect(url_for('login'))
            if not user.has_access:
                return redirect(url_for('pricing'))
            return f(*args, **kwargs)
        return decorated

    def admin_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user_id = session.get('user_id')
            if not user_id:
                return redirect(url_for('login'))
            user = User.query.get(user_id)
            if not user or user.role != 'admin':
                return jsonify({'error': 'Admin access required'}), 403
            return f(*args, **kwargs)
        return decorated

    def get_current_user():
        user_id = session.get('user_id')
        if user_id:
            return User.query.get(user_id)
        return None

    @app.context_processor
    def inject_user():
        return {'current_user': get_current_user()}

    # ── Public Pages ────────────────────────────────────────────
    @app.route('/')
    def home():
        if session.get('user_id'):
            return redirect(url_for('overview'))
        return render_template('home.html')

    @app.route('/pricing')
    def pricing():
        return render_template('pricing.html')

    # ── Auth Routes ─────────────────────────────────────────────
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            password = request.form.get('password', '')
            user = User.query.filter_by(email=email).first()
            if user and user.check_password(password) and user.is_active:
                session.permanent = True
                session['user_id'] = user.id
                user.last_login = datetime.utcnow()
                db.session.commit()
                if user.role == 'admin':
                    return redirect(url_for('admin_panel'))
                return redirect(url_for('overview'))
            flash('Invalid email or password', 'error')
        return render_template('login.html')

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        if request.method == 'POST':
            name = request.form.get('name', '').strip()
            email = request.form.get('email', '').strip().lower()
            password = request.form.get('password', '')
            confirm = request.form.get('confirm_password', '')

            if not name or not email or not password:
                flash('All fields are required', 'error')
                return render_template('register.html')
            if password != confirm:
                flash('Passwords do not match', 'error')
                return render_template('register.html')
            if len(password) < 6:
                flash('Password must be at least 6 characters', 'error')
                return render_template('register.html')
            if User.query.filter_by(email=email).first():
                flash('Email already registered', 'error')
                return render_template('register.html')

            user = User(email=email, name=name, role='user', is_active=True)
            user.set_password(password)
            user.start_trial()
            db.session.add(user)
            db.session.commit()

            session.permanent = True
            session['user_id'] = user.id
            return redirect(url_for('overview'))
        return render_template('register.html')

    @app.route('/logout')
    def logout():
        session.clear()
        return redirect(url_for('home'))

    # ── User Profile ────────────────────────────────────────────
    @app.route('/profile', methods=['GET', 'POST'])
    @login_required
    def profile():
        user = get_current_user()
        if request.method == 'POST':
            action = request.form.get('action')
            if action == 'update_profile':
                user.name = request.form.get('name', user.name)
                db.session.commit()
                flash('Profile updated', 'success')
            elif action == 'change_password':
                current = request.form.get('current_password', '')
                new_pw = request.form.get('new_password', '')
                confirm = request.form.get('confirm_password', '')
                if not user.check_password(current):
                    flash('Current password is incorrect', 'error')
                elif new_pw != confirm:
                    flash('New passwords do not match', 'error')
                elif len(new_pw) < 6:
                    flash('Password must be at least 6 characters', 'error')
                else:
                    user.set_password(new_pw)
                    db.session.commit()
                    flash('Password changed successfully', 'success')
        return render_template('profile.html', user=user)

    # ── Admin Panel ─────────────────────────────────────────────
    @app.route('/admin')
    @admin_required
    def admin_panel():
        users = User.query.order_by(User.created_at.desc()).all()
        total_users = len(users)
        active_trials = sum(1 for u in users if u.subscription_status == 'trial' and u.trial_days_remaining > 0)
        premium_users = sum(1 for u in users if u.subscription_status == 'active')
        total_complaints = Complaint.query.count()
        return render_template('admin.html', users=users, total_users=total_users,
                             active_trials=active_trials, premium_users=premium_users,
                             total_complaints=total_complaints)

    @app.route('/api/admin/user/<int:user_id>', methods=['POST'])
    @admin_required
    def admin_update_user(user_id):
        user = User.query.get_or_404(user_id)
        action = request.json.get('action')
        if action == 'extend_trial':
            days = request.json.get('days', 30)
            user.trial_end = date.today() + timedelta(days=days)
            user.subscription_status = 'trial'
        elif action == 'activate':
            user.subscription_status = 'active'
        elif action == 'deactivate':
            user.is_active = False
        elif action == 'reactivate':
            user.is_active = True
        db.session.commit()
        return jsonify({'success': True})

    # ── Dashboard Pages ─────────────────────────────────────────
    @app.route('/dashboard')
    @app.route('/overview')
    @login_required
    def overview():
        return render_template('overview.html')

    @app.route('/segmentation')
    @login_required
    def segmentation():
        return render_template('segmentation.html')

    @app.route('/geographic')
    @login_required
    def geographic():
        return render_template('geographic.html')

    @app.route('/ai-insights')
    @login_required
    def ai_insights():
        return render_template('ai_insights.html')

    @app.route('/bank-profiles')
    @login_required
    def bank_profiles():
        return render_template('bank_profiles.html')

    # ── Dashboard API ───────────────────────────────────────────
    @app.route('/api/kpis')
    @login_required
    def api_kpis():
        from services.analytics import get_kpis
        company = request.args.get('company')
        return jsonify(get_kpis(company=company))

    @app.route('/api/monthly-trend')
    @login_required
    def api_monthly_trend():
        from services.analytics import get_monthly_trend
        company = request.args.get('company')
        product = request.args.get('product')
        return jsonify(get_monthly_trend(company=company, product=product))

    @app.route('/api/product-breakdown')
    @login_required
    def api_product_breakdown():
        from services.analytics import get_product_breakdown
        company = request.args.get('company')
        return jsonify(get_product_breakdown(company=company))

    @app.route('/api/issue-breakdown')
    @login_required
    def api_issue_breakdown():
        from services.analytics import get_issue_breakdown
        company = request.args.get('company')
        product = request.args.get('product')
        return jsonify(get_issue_breakdown(company=company, product=product))

    @app.route('/api/response-breakdown')
    @login_required
    def api_response_breakdown():
        from services.analytics import get_response_breakdown
        company = request.args.get('company')
        return jsonify(get_response_breakdown(company=company))

    @app.route('/api/state-breakdown')
    @login_required
    def api_state_breakdown():
        from services.analytics import get_state_breakdown
        company = request.args.get('company')
        return jsonify(get_state_breakdown(company=company))

    @app.route('/api/submission-channels')
    @login_required
    def api_submission_channels():
        from services.analytics import get_submission_channels
        company = request.args.get('company')
        return jsonify(get_submission_channels(company=company))

    @app.route('/api/tags-analysis')
    @login_required
    def api_tags_analysis():
        from services.analytics import get_tags_analysis
        company = request.args.get('company')
        return jsonify(get_tags_analysis(company=company))

    @app.route('/api/health-score')
    @login_required
    def api_health_score():
        from services.analytics import get_health_score
        company = request.args.get('company')
        return jsonify(get_health_score(company=company))

    @app.route('/api/companies')
    @login_required
    def api_companies():
        from services.analytics import get_companies
        return jsonify(get_companies())

    @app.route('/api/bank-comparison')
    @login_required
    def api_bank_comparison():
        from services.analytics import get_bank_comparison
        return jsonify(get_bank_comparison())

    # ── Client Details APIs ───────────────────────────────────
    @app.route('/api/monthly-trend-by-product')
    @login_required
    def api_monthly_trend_by_product():
        from services.analytics import get_monthly_trend_by_product
        company = request.args.get('company')
        return jsonify(get_monthly_trend_by_product(company=company))

    @app.route('/api/monthly-trend-by-response')
    @login_required
    def api_monthly_trend_by_response():
        from services.analytics import get_monthly_trend_by_response
        company = request.args.get('company')
        return jsonify(get_monthly_trend_by_response(company=company))

    @app.route('/api/sub-product-breakdown')
    @login_required
    def api_sub_product_breakdown():
        from services.analytics import get_sub_product_breakdown
        company = request.args.get('company')
        product = request.args.get('product')
        return jsonify(get_sub_product_breakdown(company=company, product=product))

    @app.route('/api/issue-resolution-mix')
    @login_required
    def api_issue_resolution_mix():
        from services.analytics import get_issue_resolution_mix
        company = request.args.get('company')
        return jsonify(get_issue_resolution_mix(company=company))

    @app.route('/api/mom-changes')
    @login_required
    def api_mom_changes():
        from services.analytics import get_mom_changes
        company = request.args.get('company')
        return jsonify(get_mom_changes(company=company))

    @app.route('/api/tags-trend')
    @login_required
    def api_tags_trend():
        from services.analytics import get_tags_trend
        company = request.args.get('company')
        return jsonify(get_tags_trend(company=company))

    @app.route('/api/channel-trend')
    @login_required
    def api_channel_trend():
        from services.analytics import get_channel_trend
        company = request.args.get('company')
        return jsonify(get_channel_trend(company=company))

    @app.route('/api/peer-comparison')
    @login_required
    def api_peer_comparison():
        from services.analytics import get_peer_comparison, get_peer_companies
        company = request.args.get('company', 'SYNCHRONY FINANCIAL')
        peers = get_peer_companies(company, limit=5)
        peer_names = [p['company'] for p in peers]
        return jsonify({
            'target': company,
            'peers': peers,
            'comparison': get_peer_comparison(company, peer_names)
        })

    @app.route('/api/issue-tree')
    @login_required
    def api_issue_tree():
        from services.analytics import get_issue_sub_issue_tree
        company = request.args.get('company')
        return jsonify(get_issue_sub_issue_tree(company=company))

    @app.route('/api/product-response-crosstab')
    @login_required
    def api_product_response_crosstab():
        from services.analytics import get_product_response_crosstab
        company = request.args.get('company')
        return jsonify(get_product_response_crosstab(company=company))

    # ── AI API ──────────────────────────────────────────────────
    @app.route('/api/ai/generate', methods=['POST'])
    @login_required
    def api_ai_generate():
        from services.groq_ai import generate_commentary
        from services.analytics import get_kpis, get_monthly_trend, get_product_breakdown, get_state_breakdown, get_response_breakdown

        user = get_current_user()
        section = request.json.get('section', 'executive_summary')

        # Gather stats for the AI
        company = request.json.get('company')
        stats = {
            'kpis': get_kpis(company=company),
            'monthly_trend': get_monthly_trend(company=company, months=12),
            'products': get_product_breakdown(company=company),
            'states': get_state_breakdown(company=company)[:10],
            'responses': get_response_breakdown(company=company),
        }

        result = generate_commentary(section, stats)
        return jsonify(result)

    @app.route('/api/ai/all-sections', methods=['POST'])
    @login_required
    def api_ai_all_sections():
        from services.groq_ai import generate_commentary
        from services.analytics import get_kpis, get_monthly_trend, get_product_breakdown, get_state_breakdown, get_response_breakdown

        company = request.json.get('company') if request.json else None
        stats = {
            'kpis': get_kpis(company=company),
            'monthly_trend': get_monthly_trend(company=company, months=12),
            'products': get_product_breakdown(company=company),
            'states': get_state_breakdown(company=company)[:10],
            'responses': get_response_breakdown(company=company),
        }

        sections = ['executive_summary', 'trend_analysis', 'anomaly_detection', 'sentiment_analysis', 'recommendations']
        results = {}
        for s in sections:
            results[s] = generate_commentary(s, stats)

        return jsonify(results)

    # ── CFPB Refresh ────────────────────────────────────────────
    @app.route('/api/cfpb/refresh', methods=['POST'])
    @admin_required
    def api_cfpb_refresh():
        from services.cfpb_api import refresh_data
        company = request.json.get('company') if request.json else None
        result = refresh_data(company=company)
        return jsonify(result)

    # ── Stripe (Placeholder) ────────────────────────────────────
    @app.route('/api/stripe/create-checkout-session', methods=['POST'])
    @login_required
    def stripe_create_checkout():
        stripe_key = os.getenv('STRIPE_SECRET_KEY')
        if not stripe_key:
            return jsonify({
                'error': 'Stripe not configured yet',
                'message': 'Premium subscriptions coming soon! Enjoy your free trial.',
                'redirect': url_for('pricing')
            }), 503

        # When Stripe is configured, create actual checkout session
        return jsonify({'error': 'Stripe integration pending'}), 503

    @app.route('/stripe/success')
    @login_required
    def stripe_success():
        user = get_current_user()
        user.subscription_status = 'active'
        db.session.commit()
        flash('Welcome to Premium! Full access unlocked.', 'success')
        return redirect(url_for('overview'))

    @app.route('/stripe/cancel')
    @login_required
    def stripe_cancel():
        flash('Subscription not completed. You can upgrade anytime.', 'info')
        return redirect(url_for('pricing'))

    @app.route('/api/stripe/webhook', methods=['POST'])
    def stripe_webhook():
        # Placeholder for Stripe webhook handling
        return jsonify({'received': True})

    # ── External Data APIs ──────────────────────────────────────
    @app.route('/api/external/bank-profile/<path:bank_name>')
    @login_required
    def api_external_bank_profile(bank_name):
        from services.external_apis import build_comprehensive_bank_profile
        return jsonify(build_comprehensive_bank_profile(bank_name))

    @app.route('/api/external/industry-overview')
    @login_required
    def api_external_industry_overview():
        from services.external_apis import get_industry_overview
        return jsonify(get_industry_overview())

    @app.route('/api/external/fdic/search')
    @login_required
    def api_fdic_search():
        from services.external_apis import fdic_search_institutions
        name = request.args.get('name')
        state = request.args.get('state')
        return jsonify(fdic_search_institutions(name=name, state=state))

    @app.route('/api/external/fdic/failures')
    @login_required
    def api_fdic_failures():
        from services.external_apis import fdic_get_failures
        return jsonify(fdic_get_failures())

    @app.route('/api/external/cfpb/top-companies')
    @login_required
    def api_cfpb_top_companies():
        from services.external_apis import cfpb_get_top_companies
        return jsonify(cfpb_get_top_companies())

    @app.route('/api/external/cfpb/trends')
    @login_required
    def api_cfpb_trends():
        from services.external_apis import cfpb_get_trends
        company = request.args.get('company')
        product = request.args.get('product')
        return jsonify(cfpb_get_trends(company=company, product=product))

    @app.route('/api/external/finra/search')
    @login_required
    def api_finra_search():
        from services.external_apis import finra_search_firm
        name = request.args.get('name', '')
        return jsonify(finra_search_firm(name))

    @app.route('/api/external/sec/search')
    @login_required
    def api_sec_search():
        from services.external_apis import sec_search_company
        name = request.args.get('name', '')
        return jsonify(sec_search_company(name))

    @app.route('/api/external/sec/enforcement')
    @login_required
    def api_sec_enforcement():
        from services.external_apis import sec_get_enforcement_actions
        return jsonify(sec_get_enforcement_actions())

    # ── Data Export (Premium Only) ──────────────────────────────
    def premium_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user = get_current_user()
            if not user or not user.is_premium:
                return jsonify({'error': 'Premium subscription required', 'upgrade_url': url_for('pricing')}), 403
            return f(*args, **kwargs)
        return decorated

    @app.route('/api/export/monthly-data')
    @login_required
    @premium_required
    def api_export_monthly():
        """Export monthly complaint data as tab-separated text"""
        from services.analytics import get_monthly_trend, get_product_breakdown, get_response_breakdown
        from flask import Response

        company = request.args.get('company')
        product = request.args.get('product')

        lines = []
        lines.append(f"ComplaintsHoo - Monthly Data Export")
        lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        if company:
            lines.append(f"Company: {company}")
        if product:
            lines.append(f"Product: {product}")
        lines.append("")

        # Monthly trend
        lines.append("=" * 60)
        lines.append("MONTHLY COMPLAINT VOLUME")
        lines.append("=" * 60)
        lines.append(f"{'Month':<12}{'Complaints':>12}")
        lines.append("-" * 24)
        trend = get_monthly_trend(company=company, product=product)
        for t in trend:
            lines.append(f"{t['month']:<12}{t['count']:>12,}")
        lines.append("")

        # Product breakdown
        lines.append("=" * 60)
        lines.append("COMPLAINTS BY PRODUCT")
        lines.append("=" * 60)
        lines.append(f"{'Product':<45}{'Count':>12}")
        lines.append("-" * 57)
        products = get_product_breakdown(company=company)
        for p in products:
            lines.append(f"{p['product'][:44]:<45}{p['count']:>12,}")
        lines.append("")

        # Response breakdown
        lines.append("=" * 60)
        lines.append("COMPANY RESPONSE BREAKDOWN")
        lines.append("=" * 60)
        lines.append(f"{'Response Type':<45}{'Count':>12}")
        lines.append("-" * 57)
        responses = get_response_breakdown(company=company)
        for r in responses:
            lines.append(f"{r['response'][:44]:<45}{r['count']:>12,}")
        lines.append("")

        content = "\n".join(lines)
        return Response(
            content,
            mimetype='text/plain',
            headers={'Content-Disposition': 'attachment; filename=complaintshoo_monthly_data.txt'}
        )

    @app.route('/api/export/segmentation-data')
    @login_required
    @premium_required
    def api_export_segmentation():
        """Export segmentation data as tab-separated text"""
        from services.analytics import get_issue_breakdown, get_state_breakdown, get_submission_channels, get_tags_analysis
        from flask import Response

        company = request.args.get('company')

        lines = []
        lines.append(f"ComplaintsHoo - Segmentation Data Export")
        lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        if company:
            lines.append(f"Company: {company}")
        lines.append("")

        # Issues
        lines.append("=" * 60)
        lines.append("TOP ISSUES")
        lines.append("=" * 60)
        lines.append(f"{'Issue':<50}{'Count':>8}")
        lines.append("-" * 58)
        issues = get_issue_breakdown(company=company, limit=30)
        for i in issues:
            lines.append(f"{i['issue'][:49]:<50}{i['count']:>8,}")
        lines.append("")

        # States
        lines.append("=" * 60)
        lines.append("COMPLAINTS BY STATE")
        lines.append("=" * 60)
        lines.append(f"{'State':<8}{'Complaints':>12}{'Monetary Relief':>18}{'Relief Rate':>14}")
        lines.append("-" * 52)
        states = get_state_breakdown(company=company)
        for s in states:
            lines.append(f"{s['state']:<8}{s['count']:>12,}{s['monetary_count']:>18,}{s['monetary_rate']:>13.1f}%")
        lines.append("")

        # Channels
        lines.append("=" * 60)
        lines.append("SUBMISSION CHANNELS")
        lines.append("=" * 60)
        lines.append(f"{'Channel':<25}{'Count':>12}")
        lines.append("-" * 37)
        channels = get_submission_channels(company=company)
        for c in channels:
            lines.append(f"{c['channel']:<25}{c['count']:>12,}")
        lines.append("")

        # Tags
        lines.append("=" * 60)
        lines.append("CONSUMER TAGS")
        lines.append("=" * 60)
        tags = get_tags_analysis(company=company)
        for t in tags:
            lines.append(f"{t['tag']:<30}{t['count']:>8,}")

        content = "\n".join(lines)
        return Response(
            content,
            mimetype='text/plain',
            headers={'Content-Disposition': 'attachment; filename=complaintshoo_segmentation.txt'}
        )

    @app.route('/api/export/ai-insights')
    @login_required
    @premium_required
    def api_export_ai_insights():
        """Export AI-generated insights as text"""
        from models.database import AICommentary
        from flask import Response

        lines = []
        lines.append(f"ComplaintsHoo - AI Insights Export")
        lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append("")

        section_titles = {
            'executive_summary': 'EXECUTIVE SUMMARY',
            'trend_analysis': 'TREND ANALYSIS',
            'anomaly_detection': 'ANOMALY DETECTION',
            'sentiment_analysis': 'SENTIMENT ANALYSIS',
            'recommendations': 'RECOMMENDATIONS',
        }

        for section_key, title in section_titles.items():
            commentary = AICommentary.query.filter_by(section=section_key).order_by(AICommentary.generated_at.desc()).first()
            lines.append("=" * 60)
            lines.append(title)
            lines.append("=" * 60)
            if commentary:
                lines.append(f"(Generated: {commentary.generated_at.strftime('%Y-%m-%d %H:%M UTC')})")
                lines.append("")
                lines.append(commentary.content)
            else:
                lines.append("Not yet generated. Click 'Generate All Insights' on the AI Insights page first.")
            lines.append("")

        content = "\n".join(lines)
        return Response(
            content,
            mimetype='text/plain',
            headers={'Content-Disposition': 'attachment; filename=complaintshoo_ai_insights.txt'}
        )

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5000)
