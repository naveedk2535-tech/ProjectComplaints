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

    @app.route('/api/admin/decompress-db', methods=['POST'])
    @admin_required
    def admin_decompress_db():
        """One-time: decompress uploaded .gz database"""
        import gzip, shutil
        gz_path = os.path.join(data_dir, 'complaints.db.gz')
        db_path = os.path.join(data_dir, 'complaints.db')
        if os.path.exists(gz_path):
            with gzip.open(gz_path, 'rb') as f_in:
                with open(db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(gz_path)
            return jsonify({'success': True, 'size': os.path.getsize(db_path)})
        return jsonify({'error': 'No .gz file found'}), 404

    # ── Admin User Edit ─────────────────────────────────────────
    @app.route('/api/admin/user/<int:user_id>/edit', methods=['POST'])
    @admin_required
    def admin_edit_user(user_id):
        user = User.query.get_or_404(user_id)
        data = request.json
        if 'name' in data:
            user.name = data['name']
        if 'email' in data:
            existing = User.query.filter(User.email == data['email'], User.id != user_id).first()
            if existing:
                return jsonify({'error': 'Email already in use'}), 400
            user.email = data['email']
        if 'role' in data and data['role'] in ('user', 'admin'):
            user.role = data['role']
        if 'subscription_status' in data and data['subscription_status'] in ('trial', 'active', 'expired', 'cancelled'):
            user.subscription_status = data['subscription_status']
        if 'is_active' in data:
            user.is_active = bool(data['is_active'])
        if 'password' in data and data['password']:
            user.set_password(data['password'])
        if 'trial_days' in data:
            user.trial_end = date.today() + timedelta(days=int(data['trial_days']))
            if user.subscription_status != 'active':
                user.subscription_status = 'trial'
        db.session.commit()
        return jsonify({'success': True})

    @app.route('/api/admin/user/<int:user_id>/details')
    @admin_required
    def admin_get_user(user_id):
        user = User.query.get_or_404(user_id)
        return jsonify({
            'id': user.id, 'name': user.name, 'email': user.email,
            'role': user.role, 'subscription_status': user.subscription_status,
            'is_active': user.is_active,
            'trial_start': user.trial_start.isoformat() if user.trial_start else None,
            'trial_end': user.trial_end.isoformat() if user.trial_end else None,
            'trial_days_remaining': user.trial_days_remaining,
            'created_at': user.created_at.isoformat() if user.created_at else None,
            'last_login': user.last_login.isoformat() if user.last_login else None,
        })

    # ── Admin Add/Delete Users ──────────────────────────────────
    @app.route('/api/admin/user/add', methods=['POST'])
    @admin_required
    def admin_add_user():
        data = request.json
        if not data.get('email') or not data.get('name') or not data.get('password'):
            return jsonify({'error': 'Name, email, and password required'}), 400
        if User.query.filter_by(email=data['email'].lower()).first():
            return jsonify({'error': 'Email already exists'}), 400
        user = User(
            email=data['email'].lower(),
            name=data['name'],
            role=data.get('role', 'user'),
            subscription_status=data.get('subscription_status', 'trial'),
            is_active=True,
        )
        user.set_password(data['password'])
        if data.get('subscription_status') != 'active':
            user.start_trial()
            if data.get('trial_days'):
                user.trial_end = date.today() + timedelta(days=int(data['trial_days']))
        db.session.add(user)
        db.session.commit()
        return jsonify({'success': True, 'id': user.id})

    @app.route('/api/admin/user/<int:user_id>/delete', methods=['POST'])
    @admin_required
    def admin_delete_user(user_id):
        user = User.query.get_or_404(user_id)
        if user.role == 'admin':
            admin_count = User.query.filter_by(role='admin').count()
            if admin_count <= 1:
                return jsonify({'error': 'Cannot delete the last admin'}), 400
        db.session.delete(user)
        db.session.commit()
        return jsonify({'success': True})

    # ── Monthly Volume (actual CFPB totals) ─────────────────────
    @app.route('/api/monthly-volume')
    @login_required
    def api_monthly_volume():
        from models.database import MonthlyVolume
        company = request.args.get('company')
        months_back = request.args.get('months', type=int)
        compare = request.args.get('compare')  # company name to compare

        q = db.session.query(
            MonthlyVolume.month,
            db.func.sum(MonthlyVolume.total_complaints).label('total')
        )
        if company:
            q = q.filter(MonthlyVolume.company == company)
        q = q.group_by(MonthlyVolume.month).order_by(MonthlyVolume.month)
        raw = q.all()

        results = [{'month': r.month, 'count': r.total} for r in raw]

        # Normalize outliers (if a month is >2.5x the median of neighbors, cap it)
        if len(results) > 2:
            for i in range(1, len(results) - 1):
                prev_val = results[i-1]['count']
                next_val = results[i+1]['count']
                avg_neighbors = (prev_val + next_val) / 2
                if results[i]['count'] > avg_neighbors * 2.5:
                    results[i]['count'] = int(avg_neighbors)
                    results[i]['normalized'] = True

        # Filter to last N months if requested
        if months_back and len(results) > months_back:
            results = results[-months_back:]

        # Mark last month as partial and compute smart forecast
        if results:
            today = date.today()
            current_month = f"{today.year}-{today.month:02d}"
            if results[-1]['month'] == current_month:
                results[-1]['partial'] = True
                # Smart forecast: blend of extrapolation + avg of last 3 full months
                full_months = [r for r in results[:-1] if not r.get('normalized')]
                if len(full_months) >= 3:
                    avg_last3 = sum(m['count'] for m in full_months[-3:]) / 3
                elif len(full_months) >= 1:
                    avg_last3 = sum(m['count'] for m in full_months[-3:]) / len(full_months[-3:])
                else:
                    avg_last3 = results[-1]['count']
                # Weight: 70% historical avg + 30% extrapolation
                days_in_month = 31
                days_elapsed = today.day
                extrapolated = int(results[-1]['count'] * days_in_month / max(days_elapsed, 1))
                forecast = int(avg_last3 * 0.7 + extrapolated * 0.3)
                results[-1]['forecast'] = forecast
                results[-1]['actual_so_far'] = results[-1]['count']

        # Comparison company data
        compare_data = None
        if compare:
            cq = db.session.query(
                MonthlyVolume.month,
                db.func.sum(MonthlyVolume.total_complaints).label('total')
            ).filter(MonthlyVolume.company == compare)
            cq = cq.group_by(MonthlyVolume.month).order_by(MonthlyVolume.month)
            compare_data = [{'month': r.month, 'count': r.total} for r in cq.all()]
            if months_back and len(compare_data) > months_back:
                compare_data = compare_data[-months_back:]

        return jsonify({'data': results, 'compare': compare_data})

    @app.route('/api/monthly-volume-by-company')
    @login_required
    def api_monthly_volume_by_company():
        from models.database import MonthlyVolume
        q = db.session.query(
            MonthlyVolume.company,
            MonthlyVolume.month,
            MonthlyVolume.total_complaints
        ).order_by(MonthlyVolume.month)
        results = q.all()
        return jsonify([{'company': r.company, 'month': r.month, 'count': r.total_complaints} for r in results])

    # ── Text Analytics APIs ─────────────────────────────────────
    @app.route('/api/text/top-words')
    @login_required
    def api_top_words():
        from services.text_analytics import get_top_words
        company = request.args.get('company')
        return jsonify(get_top_words(company=company))

    @app.route('/api/text/sentiment')
    @login_required
    def api_sentiment():
        from services.text_analytics import get_sentiment_summary
        company = request.args.get('company')
        return jsonify(get_sentiment_summary(company=company))

    @app.route('/api/text/themes')
    @login_required
    def api_themes():
        from services.text_analytics import get_complaint_themes
        company = request.args.get('company')
        return jsonify(get_complaint_themes(company=company))

    @app.route('/api/text/narrative-stats')
    @login_required
    def api_narrative_stats():
        from services.text_analytics import get_narrative_stats
        company = request.args.get('company')
        return jsonify(get_narrative_stats(company=company))

    _top5_cache = {'data': {}, 'time': {}}

    @app.route('/api/top5-comparison')
    @login_required
    def api_top5_comparison():
        """Top 5 analysis with batch MoM comparison. Cached 5 min."""
        import time as _time
        from sqlalchemy import func, desc

        company = request.args.get('company') or ''
        cache_key = company
        if cache_key in _top5_cache['data'] and _time.time() - _top5_cache['time'].get(cache_key, 0) < 300:
            return jsonify(_top5_cache['data'][cache_key])

        today = date.today()
        prev_end = today.replace(day=1) - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        prev2_end = prev_start - timedelta(days=1)
        prev2_start = prev2_end.replace(day=1)

        def _batch_top5(field, filter_null=True):
            """Single function: get top 5 overall + batch MoM in 3 queries total."""
            col = getattr(Complaint, field)
            # Overall top 5
            q = db.session.query(col, func.count().label('count'))
            if company:
                q = q.filter(Complaint.company == company)
            if filter_null:
                q = q.filter(col.isnot(None), col != '')
            q = q.group_by(col).order_by(desc('count')).limit(5)
            items = [{'name': r[0] or '', 'count': r[1]} for r in q.all()]

            names = [it['name'] for it in items]
            if not names:
                return items

            # Prev month counts (1 batch query)
            pq = db.session.query(col, func.count().label('c')).filter(
                Complaint.date_received >= prev_start, Complaint.date_received <= prev_end, col.in_(names))
            if company:
                pq = pq.filter(Complaint.company == company)
            prev_counts = {r[0]: r[1] for r in pq.group_by(col).all()}

            # Prev2 month counts (1 batch query)
            p2q = db.session.query(col, func.count().label('c')).filter(
                Complaint.date_received >= prev2_start, Complaint.date_received <= prev2_end, col.in_(names))
            if company:
                p2q = p2q.filter(Complaint.company == company)
            prev2_counts = {r[0]: r[1] for r in p2q.group_by(col).all()}

            for it in items:
                n = it['name']
                cur = prev_counts.get(n, 0)
                prev = prev2_counts.get(n, 0)
                it['cur_month'] = cur
                it['prev_month'] = prev
                it['mom_change'] = round((cur - prev) / prev * 100, 0) if prev else (100 if cur else 0)
            return items

        result = {
            'products': _batch_top5('product'),
            'issues': _batch_top5('issue'),
            'sub_products': _batch_top5('sub_product'),
            'sub_issues': _batch_top5('sub_issue'),
            'states': _batch_top5('state'),
            'responses': _batch_top5('company_response'),
            'channels': _batch_top5('submitted_via'),
            'tags': _batch_top5('tags'),
            'public_responses': _batch_top5('company_public_response'),
        }

        # Fix key names for consistency with frontend
        for item in result['products']:
            item['product'] = item['name']
        for item in result['issues']:
            item['issue'] = item['name']
        for item in result['states']:
            item['state'] = item['name']
        for item in result['responses']:
            item['response'] = item['name']
        for item in result['channels']:
            item['channel'] = item['name']

        # Add peer/industry comparison if a company is selected
        if company:
            def _get_peer_pct(field):
                """Get % distribution across all OTHER companies for comparison."""
                col = getattr(Complaint, field)
                total_q = Complaint.query.filter(Complaint.company != company).count()
                if not total_q:
                    return {}
                q = db.session.query(col, func.count().label('c')).filter(
                    Complaint.company != company, col.isnot(None), col != ''
                ).group_by(col).all()
                return {r[0]: round(r[1] / total_q * 100, 1) for r in q}

            peer_products = _get_peer_pct('product')
            peer_issues = _get_peer_pct('issue')
            peer_states = _get_peer_pct('state')

            for item in result['products']:
                item['peer_pct'] = peer_products.get(item['name'], 0)
            for item in result['issues']:
                item['peer_pct'] = peer_issues.get(item['name'], 0)
            for item in result['states']:
                item['peer_pct'] = peer_states.get(item['name'], 0)

        _top5_cache['data'][cache_key] = result
        _top5_cache['time'][cache_key] = _time.time()
        return jsonify(result)

    # ── MEGA ENDPOINT: All dashboard data in one request ──────
    _dashboard_cache = {}

    @app.route('/api/dashboard-data')
    @login_required
    def api_dashboard_data():
        """Returns ALL dashboard data in a single response. Cached 5 min per company."""
        import time as _t
        from services.analytics import (get_kpis, get_health_score, get_mom_changes,
            get_product_breakdown, get_issue_breakdown, get_response_breakdown,
            get_state_breakdown, get_submission_channels, get_sub_product_breakdown,
            get_issue_resolution_mix, get_bank_comparison)
        from models.database import MonthlyVolume

        company = request.args.get('company') or ''
        months_back = request.args.get('months', type=int)
        cache_key = f'{company}_{months_back}'

        if cache_key in _dashboard_cache and _t.time() - _dashboard_cache[cache_key][1] < 300:
            return jsonify(_dashboard_cache[cache_key][0])

        # Build everything
        kpis = get_kpis(company=company or None)
        health = get_health_score(company=company or None)
        mom = get_mom_changes(company=company or None)

        # Monthly volume
        vol_q = db.session.query(MonthlyVolume.month, db.func.sum(MonthlyVolume.total_complaints).label('total'))
        if company:
            vol_q = vol_q.filter(MonthlyVolume.company == company)
        vol_q = vol_q.group_by(MonthlyVolume.month).order_by(MonthlyVolume.month)
        vol_raw = [{'month': r.month, 'count': r.total} for r in vol_q.all()]
        # Normalize outliers
        if len(vol_raw) > 2:
            for i in range(1, len(vol_raw) - 1):
                avg_n = (vol_raw[i-1]['count'] + vol_raw[i+1]['count']) / 2
                if vol_raw[i]['count'] > avg_n * 2.5:
                    vol_raw[i]['count'] = int(avg_n)
        if months_back and len(vol_raw) > months_back:
            vol_raw = vol_raw[-months_back:]
        # Forecast
        if vol_raw:
            today = date.today()
            cur_m = f"{today.year}-{today.month:02d}"
            if vol_raw[-1]['month'] == cur_m:
                vol_raw[-1]['partial'] = True
                full = [r for r in vol_raw[:-1]]
                avg3 = sum(m['count'] for m in full[-3:]) / max(len(full[-3:]), 1) if full else vol_raw[-1]['count']
                ext = int(vol_raw[-1]['count'] * 31 / max(today.day, 1))
                vol_raw[-1]['forecast'] = int(avg3 * 0.7 + ext * 0.3)
                vol_raw[-1]['actual_so_far'] = vol_raw[-1]['count']

        responses = get_response_breakdown(company=company or None)
        products = get_product_breakdown(company=company or None)
        issues = get_issue_breakdown(company=company or None)
        sub_products = get_sub_product_breakdown(company=company or None)
        states = get_state_breakdown(company=company or None, limit=15)
        channels = get_submission_channels(company=company or None)
        issue_res = get_issue_resolution_mix(company=company or None)

        # Peer average
        peer = None
        if company:
            from sqlalchemy import func as sqf, case as sqcase
            prods = [r[0] for r in db.session.query(Complaint.product).filter(Complaint.company == company).distinct().all()]
            if prods:
                pq = db.session.query(
                    sqf.count().label('t'),
                    sqf.sum(sqcase((Complaint.company_response.like('Closed%'), 1), else_=0)).label('cl'),
                    sqf.sum(sqcase((Complaint.company_response == 'Closed with monetary relief', 1), else_=0)).label('mo'),
                    sqf.sum(sqcase((Complaint.timely_response == True, 1), else_=0)).label('ti'),
                ).filter(Complaint.company != company, Complaint.product.in_(prods))
                pr = pq.first()
                pt = pr[0] or 0
                if pt:
                    pcl = pr[1] or 0
                    pmo = pr[2] or 0
                    pti = pr[3] or 0
                    peer = {
                        'resolution_rate': round(pcl / pt * 100, 1),
                        'monetary_relief_rate': round(pmo / pt * 100, 1),
                        'timely_rate': round(pti / pt * 100, 1),
                        'peer_count': db.session.query(Complaint.company).filter(Complaint.company != company, Complaint.product.in_(prods)).distinct().count(),
                        'total': pt,
                    }

        result = {
            'kpis': kpis, 'health': health, 'mom': mom,
            'monthly_volume': vol_raw, 'responses': responses,
            'products': products, 'issues': issues,
            'sub_products': sub_products, 'states': states,
            'channels': channels, 'issue_resolution': issue_res,
            'peer': peer,
        }

        _dashboard_cache[cache_key] = (result, _t.time())
        return jsonify(result)

    @app.route('/api/data-sources')
    @login_required
    def api_data_sources():
        """Return info about all data sources and what data we have."""
        from models.database import MonthlyVolume
        complaint_count = Complaint.query.count()
        company_count = db.session.query(Complaint.company).distinct().count()
        mv_count = MonthlyVolume.query.count()
        date_range = db.session.query(
            db.func.min(Complaint.date_received),
            db.func.max(Complaint.date_received)
        ).first()
        return jsonify({
            'cfpb': {
                'status': 'active',
                'complaints': complaint_count,
                'companies': company_count,
                'monthly_volumes': mv_count,
                'date_range': [str(date_range[0]) if date_range[0] else None, str(date_range[1]) if date_range[1] else None],
                'fields': ['date_received','product','sub_product','issue','sub_issue','narrative','company_public_response',
                           'company','state','zip_code','tags','consumer_consent','submitted_via','date_sent_to_company',
                           'company_response','timely_response','consumer_disputed'],
            },
            'fdic': {'status': 'active', 'description': 'Bank financials, assets, deposits, ROA/ROE via live API'},
            'sec_edgar': {'status': 'active', 'description': 'Company filings (10-K, 10-Q, 8-K) via live API'},
            'finra': {'status': 'limited', 'description': 'Broker disclosures via search API (best effort)'},
        })

    @app.route('/api/text/word-trends')
    @login_required
    def api_word_trends():
        from services.text_analytics import get_monthly_word_trends
        company = request.args.get('company')
        return jsonify(get_monthly_word_trends(company=company))

    @app.route('/api/text/trending-words')
    @login_required
    def api_trending_words():
        from services.text_analytics import get_trending_words
        company = request.args.get('company')
        return jsonify(get_trending_words(company=company))

    @app.route('/api/peer-average')
    @login_required
    def api_peer_average():
        """Get average KPIs across all companies sharing same products as the selected company."""
        from services.analytics import get_kpis, get_companies
        company = request.args.get('company')
        if not company:
            return jsonify({})
        # Get products for this company
        products = db.session.query(Complaint.product).filter(
            Complaint.company == company
        ).distinct().all()
        product_set = [p[0] for p in products]
        # Get all companies with those products (excluding target)
        peer_companies = db.session.query(Complaint.company).filter(
            Complaint.company != company,
            Complaint.product.in_(product_set)
        ).distinct().all()
        peer_names = [p[0] for p in peer_companies]
        if not peer_names:
            return jsonify({})
        # Calculate aggregate KPIs for peers
        from sqlalchemy import func, case
        q = Complaint.query.filter(Complaint.company.in_(peer_names))
        total = q.count()
        if total == 0:
            return jsonify({})
        closed = q.filter(Complaint.company_response.like('Closed%')).count()
        monetary = q.filter(Complaint.company_response == 'Closed with monetary relief').count()
        timely = q.filter(Complaint.timely_response == True).count()
        return jsonify({
            'peer_count': len(peer_names),
            'total': total,
            'resolution_rate': round(closed / total * 100, 1),
            'monetary_relief_rate': round(monetary / total * 100, 1),
            'timely_rate': round(timely / total * 100, 1),
        })

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
