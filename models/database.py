from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    role = db.Column(db.String(10), default='user')  # 'user' or 'admin'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    trial_start = db.Column(db.Date)
    trial_end = db.Column(db.Date)
    subscription_status = db.Column(db.String(20), default='trial')  # trial, active, expired, cancelled
    stripe_customer_id = db.Column(db.String(100))
    stripe_subscription_id = db.Column(db.String(100))
    is_active = db.Column(db.Boolean, default=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def start_trial(self):
        from datetime import date
        self.trial_start = date.today()
        self.trial_end = date.today() + timedelta(days=30)
        self.subscription_status = 'trial'

    @property
    def trial_days_remaining(self):
        from datetime import date
        if self.trial_end and self.subscription_status == 'trial':
            remaining = (self.trial_end - date.today()).days
            return max(0, remaining)
        return 0

    @property
    def has_access(self):
        if self.role == 'admin':
            return True
        if self.subscription_status == 'active':
            return True
        if self.subscription_status == 'trial' and self.trial_days_remaining > 0:
            return True
        return False

    @property
    def is_premium(self):
        return self.subscription_status == 'active' or self.role == 'admin'


class Complaint(db.Model):
    __tablename__ = 'complaints'
    id = db.Column(db.Integer, primary_key=True)
    complaint_id = db.Column(db.Integer, unique=True, index=True)
    date_received = db.Column(db.Date, index=True)
    product = db.Column(db.String(100), index=True)
    sub_product = db.Column(db.String(150))
    issue = db.Column(db.String(200), index=True)
    sub_issue = db.Column(db.String(250))
    narrative = db.Column(db.Text)
    company_public_response = db.Column(db.String(200))
    company = db.Column(db.String(100), index=True)
    state = db.Column(db.String(5), index=True)
    zip_code = db.Column(db.String(10))
    tags = db.Column(db.String(50))
    consumer_consent = db.Column(db.String(30))
    submitted_via = db.Column(db.String(20))
    date_sent_to_company = db.Column(db.Date)
    company_response = db.Column(db.String(60), index=True)
    timely_response = db.Column(db.Boolean)
    consumer_disputed = db.Column(db.String(10))

    __table_args__ = (
        db.Index('idx_company_date', 'company', 'date_received'),
    )


class MonthlyVolume(db.Model):
    """Stores actual CFPB monthly complaint totals (not sampled)."""
    __tablename__ = 'monthly_volumes'
    id = db.Column(db.Integer, primary_key=True)
    company = db.Column(db.String(100), index=True)
    month = db.Column(db.String(7), index=True)  # YYYY-MM
    total_complaints = db.Column(db.Integer, default=0)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('company', 'month', name='uq_company_month'),
    )


class AICommentary(db.Model):
    __tablename__ = 'ai_commentary'
    id = db.Column(db.Integer, primary_key=True)
    section = db.Column(db.String(50), index=True)
    generated_at = db.Column(db.DateTime, default=datetime.utcnow)
    content = db.Column(db.Text)
    parameters_hash = db.Column(db.String(64))


class BankProfile(db.Model):
    __tablename__ = 'bank_profiles'
    id = db.Column(db.Integer, primary_key=True)
    bank_name = db.Column(db.String(100), index=True)
    source = db.Column(db.String(20))  # cfpb, fdic, ncua
    total_complaints = db.Column(db.Integer, default=0)
    resolution_rate = db.Column(db.Float, default=0.0)
    avg_response_days = db.Column(db.Float, default=0.0)
    health_score = db.Column(db.Float, default=0.0)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)


def init_db(app):
    db.init_app(app)
    with app.app_context():
        db.create_all()
