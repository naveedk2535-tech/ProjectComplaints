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

    # Create default admin user
    with app.app_context():
        admin = User.query.filter_by(email=os.getenv('ADMIN_EMAIL', 'admin@complaintswhoo.com')).first()
        if not admin:
            admin = User(
                email=os.getenv('ADMIN_EMAIL', 'admin@complaintswhoo.com'),
                name='Admin',
                role='admin',
                subscription_status='active',
                is_active=True,
            )
            admin.set_password(os.getenv('ADMIN_PASSWORD', 'admin123'))
            db.session.add(admin)
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

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5000)
