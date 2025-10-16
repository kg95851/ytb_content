from http.server import BaseHTTPRequestHandler
import json
import os

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Check Gemini API key count"""
        try:
            # Count actual API keys
            key_count = 0
            found_keys = []
            
            # Check numbered keys (GEMINI_API_KEY1, GEMINI_API_KEY2, etc.)
            for i in range(1, 101):
                env_name = f'GEMINI_API_KEY{i}'
                key = os.getenv(env_name)
                if key:
                    # Remove quotes if accidentally included
                    key = key.strip().strip('"').strip("'")
                    if key and len(key) > 10:  # Valid key exists
                        key_count += 1
                        found_keys.append(env_name)
            
            # Check comma-separated keys
            if key_count == 0:
                multi_keys = os.getenv('GEMINI_API_KEYS')
                if multi_keys:
                    keys = [k.strip().strip('"').strip("'") for k in multi_keys.split(',') if k.strip()]
                    valid_keys = [k for k in keys if len(k) > 10]
                    if valid_keys:
                        key_count = len(valid_keys)
                        found_keys.append(f'GEMINI_API_KEYS ({key_count} keys)')
            
            # Check single key as fallback
            if key_count == 0:
                single_key = os.getenv('GEMINI_API_KEY')
                if single_key:
                    single_key = single_key.strip().strip('"').strip("'")
                    if single_key and len(single_key) > 10:
                        key_count = 1
                        found_keys.append('GEMINI_API_KEY')
            
            # Debug: List all GEMINI-related env vars (without values for security)
            all_env_keys = list(os.environ.keys())
            gemini_vars = [k for k in all_env_keys if 'GEMINI' in k.upper()]
            
            response = {
                'ok': True,
                'key_count': key_count,
                'found_keys': found_keys,
                'gemini_env_vars': gemini_vars,
                'total_env_vars': len(all_env_keys),
                'message': f'Found {key_count} Gemini API key(s)'
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            response = {
                'ok': False,
                'error': str(e),
                'key_count': 0
            }
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
