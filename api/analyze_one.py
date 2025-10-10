import json
from flask import Flask, request, jsonify

try:
    # Reuse logic from cron_analyze
    from cron_analyze import _load_sb, _analyze_video
except Exception:
    _load_sb = None
    _analyze_video = None

app = Flask(__name__)

@app.after_request
def add_cors_headers(resp):
    try:
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    except Exception:
        pass
    return resp


@app.route('/', methods=['POST'])
@app.route('/analyze_one', methods=['POST'])
@app.route('/api/analyze_one', methods=['POST'])
def analyze_one():
    if _load_sb is None or _analyze_video is None:
        return jsonify({ 'ok': False, 'error': 'server_not_ready' }), 500
    try:
        sb = _load_sb()
        body = {}
        try:
            body = request.get_json(force=True) or {}
        except Exception:
            body = {}
        vid = str(body.get('id') or '').strip()
        if not vid:
            return jsonify({ 'ok': False, 'error': 'missing id' }), 400
        row = sb.table('videos').select('*').eq('id', vid).limit(1).execute()
        rows = getattr(row, 'data', []) or []
        if not rows:
            return jsonify({ 'ok': False, 'error': 'not_found' }), 404
        video = rows[0]
        updated = _analyze_video(video) or {}
        if updated:
            # 스키마에 없는 컬럼은 제거
            allowed = set(video.keys())
            payload = { k: v for k, v in updated.items() if k in allowed }
            if payload:
                sb.table('videos').update(payload).eq('id', vid).execute()
        return jsonify({ 'ok': True, 'updated': bool(updated) })
    except Exception as e:
        return jsonify({ 'ok': False, 'error': str(e) }), 500


@app.route('/health', methods=['GET'])
def health():
    return ('ok', 200)


