import os
from flask import jsonify

def handler(request):
    """Check Gemini API key count"""
    try:
        # Count actual API keys
        key_count = 0
        
        # Check comma-separated keys
        multi_keys = os.getenv('GEMINI_API_KEYS')
        if multi_keys:
            key_count = len([k.strip() for k in multi_keys.split(',') if k.strip()])
        
        # Check numbered keys if no comma-separated found
        if key_count == 0:
            for i in range(1, 101):
                key = os.getenv(f'GEMINI_API_KEY{i}')
                if key:
                    # Remove quotes if accidentally included
                    key = key.strip().strip('"').strip("'")
                    if key:  # Valid key exists
                        key_count += 1
        
        # Check single key as fallback
        if key_count == 0 and os.getenv('GEMINI_API_KEY'):
            key_count = 1
        
        # Debug: List all GEMINI-related env vars (without values for security)
        gemini_vars = [k for k in os.environ.keys() if 'GEMINI' in k.upper()]
            
        return jsonify({
            'ok': True,
            'key_count': key_count,
            'gemini_env_vars': gemini_vars,
            'message': f'Found {key_count} Gemini API key(s)'
        })
    except Exception as e:
        return jsonify({
            'ok': False,
            'error': str(e),
            'key_count': 0
        }), 500
