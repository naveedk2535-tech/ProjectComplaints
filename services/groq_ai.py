import os
import hashlib
import json
from datetime import datetime
from groq import Groq


def get_client():
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        return None
    return Groq(api_key=api_key)


def generate_hash(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


def generate_commentary(section, stats_data):
    from models.database import db, AICommentary

    client = get_client()
    if not client:
        return {'error': 'No API key configured', 'content': None}

    data_hash = generate_hash(stats_data)

    # Check cache
    cached = AICommentary.query.filter_by(
        section=section, parameters_hash=data_hash
    ).first()
    if cached:
        return {'content': cached.content, 'cached': True, 'generated_at': cached.generated_at.isoformat()}

    prompts = {
        'executive_summary': f"""You are a financial complaints analyst. Based on the following CFPB complaint statistics, write a concise 3-paragraph executive summary highlighting key patterns, areas of concern, and positive trends.

Data:
{json.dumps(stats_data, indent=2, default=str)}

Write in a professional, analytical tone. Be specific with numbers. Format with clear paragraphs.""",

        'trend_analysis': f"""You are a financial data analyst. Analyze the following monthly complaint trend data and identify:
1. Overall trend direction (increasing/decreasing)
2. Seasonal patterns if any
3. Notable spikes or dips and possible causes
4. Month-over-month acceleration or deceleration

Data:
{json.dumps(stats_data, indent=2, default=str)}

Be specific with dates and percentages. Format with bullet points.""",

        'anomaly_detection': f"""You are a risk analyst. Examine these complaint distribution breakdowns and flag any anomalies:
- Products or categories with disproportionate complaint volumes
- States with unusually high complaint rates
- Issues growing faster than overall complaint growth
- Any patterns that warrant immediate attention

Data:
{json.dumps(stats_data, indent=2, default=str)}

Use a "RED FLAG" / "AMBER FLAG" / "WATCH" rating system. Be concise.""",

        'sentiment_analysis': f"""You are a consumer sentiment analyst. Based on these complaint categories and resolution data, provide:
1. Overall consumer satisfaction assessment
2. Most concerning product areas
3. Resolution effectiveness analysis
4. Key themes in consumer grievances

Data:
{json.dumps(stats_data, indent=2, default=str)}

Write in clear, accessible language with specific data points.""",

        'recommendations': f"""You are a banking compliance consultant. Based on this complaint analysis data, provide exactly 5 actionable recommendations to:
1. Reduce complaint volume
2. Improve resolution rates
3. Increase monetary relief where appropriate
4. Address the highest-volume complaint categories
5. Improve overall bank health score

Data:
{json.dumps(stats_data, indent=2, default=str)}

Number each recommendation. Include expected impact. Be specific and practical.""",
    }

    prompt = prompts.get(section, prompts['executive_summary'])

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a professional financial complaints analyst providing insights for a dashboard. Be concise, data-driven, and actionable."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000,
        )
        content = response.choices[0].message.content

        # Cache the result
        commentary = AICommentary(
            section=section,
            content=content,
            parameters_hash=data_hash,
            generated_at=datetime.utcnow()
        )
        db.session.add(commentary)
        db.session.commit()

        return {'content': content, 'cached': False, 'generated_at': datetime.utcnow().isoformat()}

    except Exception as e:
        return {'error': str(e), 'content': None}
