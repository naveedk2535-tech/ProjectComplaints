import os
import hashlib
import json
import requests
from datetime import datetime

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


def generate_hash(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


def generate_commentary(section, stats_data):
    from models.database import db, AICommentary

    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
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

Write in a professional, analytical tone. Be specific with numbers.""",

        'trend_analysis': f"""You are a financial data analyst. Analyze the following monthly complaint trend data and identify:
1. Overall trend direction
2. Seasonal patterns
3. Notable spikes or dips
4. Month-over-month acceleration or deceleration

Data:
{json.dumps(stats_data, indent=2, default=str)}

Be specific with dates and percentages. Use bullet points.""",

        'anomaly_detection': f"""You are a risk analyst. Examine these complaint distributions and flag anomalies:
- Products with disproportionate volumes
- States with unusually high rates
- Issues growing faster than overall growth

Data:
{json.dumps(stats_data, indent=2, default=str)}

Use RED FLAG / AMBER FLAG / WATCH ratings. Be concise.""",

        'sentiment_analysis': f"""You are a consumer sentiment analyst. Based on these complaint categories and resolution data, provide:
1. Overall satisfaction assessment
2. Most concerning product areas
3. Resolution effectiveness
4. Key consumer grievances

Data:
{json.dumps(stats_data, indent=2, default=str)}

Write in clear, accessible language.""",

        'recommendations': f"""You are a banking compliance consultant. Based on this data, provide 5 actionable recommendations to reduce complaints and improve resolution.

Data:
{json.dumps(stats_data, indent=2, default=str)}

Number each recommendation. Include expected impact.""",
    }

    prompt = prompts.get(section, prompts['executive_summary'])

    try:
        response = requests.post(
            GROQ_API_URL,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'llama-3.3-70b-versatile',
                'messages': [
                    {'role': 'system', 'content': 'You are a professional financial complaints analyst. Be concise, data-driven, and actionable.'},
                    {'role': 'user', 'content': prompt},
                ],
                'temperature': 0.3,
                'max_tokens': 800,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        content = data['choices'][0]['message']['content']

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
