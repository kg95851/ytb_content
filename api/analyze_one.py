import json
import os
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify

try:
    # Reuse logic from cron_analyze
    from cron_analyze import _load_sb, _analyze_video
except Exception:
    _load_sb = None
    _analyze_video = None

# --- Fallback implementations if import fails ---
if _load_sb is None or _analyze_video is None:
    try:
        from supabase import create_client
    except Exception:
        create_client = None

    import requests

    def _load_sb():  # type: ignore
        if create_client is None:
            raise RuntimeError('supabase client not available')
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')
        if not url or not key:
            raise RuntimeError('Missing SUPABASE_URL or SUPABASE_*_KEY env')
        return create_client(url, key)

    def _call_gemini(system_prompt: str, user_content: str) -> str:
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise RuntimeError('GEMINI_API_KEY not set')
        prefer = os.getenv('GEMINI_MODEL')
        candidates = [
            *( [prefer] if prefer else [] ),
            'models/gemini-2.5-flash',
            'models/gemini-2.0-flash-exp',
            'models/gemini-1.5-flash-latest',
            'models/gemini-1.5-pro-latest',
        ]
        payload = {
            'contents': [
                { 'role': 'user', 'parts': [{ 'text': f"{system_prompt}\n\n{user_content}" }] }
            ],
            'generationConfig': { 'temperature': 0.3 }
        }
        base = 'https://generativelanguage.googleapis.com'
        errors = []
        for api_ver in ('v1', 'v1beta'):
            for model in candidates:
                if not model:
                    continue
                url = f"{base}/{api_ver}/{model}:generateContent?key={api_key}"
                try:
                    res = requests.post(url, json=payload, timeout=90)
                    if res.status_code == 404:
                        errors.append(f"{api_ver}/{model}:404")
                        continue
                    res.raise_for_status()
                    data = res.json()
                    text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                    if text:
                        return text
                except Exception as e:
                    errors.append(f"{api_ver}/{model}:{str(e)[:80]}")
                    continue
        raise RuntimeError('Gemini request failed: ' + '; '.join(errors))

    def _persona() -> str:
        return "너는 최신 트렌드와 미디어 콘텐츠를 분석하는 '콘텐츠 전략 분석가'야. 클라이언트로부터 받은 아래 유튜브 대본을 심층 분석해서 '콘텐츠 소재 분석 보고서'를 작성해 줘."

    def _build_material_prompt() -> str:
        return (
            _persona() + '\n\n'
            '아래 대본을 읽고 다음 형식으로만 출력하세요. 다른 텍스트 금지.\n'
            '메인 아이디어 (Main Idea): (영상이 전달하려는 핵심 메시지를 1문장으로)\n\n'
            '핵심 소재 (Core Materials):\n- 항목은 3~7개, 간결한 명사구로 불릿 리스트 작성\n- 불필요한 수식/이모지/코드블록 금지'
        )

    def _build_hooking_prompt() -> str:
        return (
            _persona() + '\n\n'
            '대본의 시작부에서 궁금증을 유발하는 "대사 원문" 1줄을 그대로 추출하고, 사용된 후킹 기법을 표로 제시하세요. 요약/의역 금지.\n'
            '출력은 아래 마크다운 표 한 개만(열 머리 포함), 다른 텍스트 금지.\n'
            '| 🤔 궁금증 유발 (Hook) | 기법 |\n| :--- | :--- |\n| (대사 원문 1줄) | (예: 의문제시형, 어그로, 모순 제시, 충격 비주얼 등) |'
        )

    def _build_structure_prompt() -> str:
        return (
            _persona() + '\n\n'
            '대본에서 기·승·전·결에 해당하는 "대사 원문"을 각 1줄씩 그대로 추출하세요(요약/의역 금지).\n'
            '출력은 다음 마크다운 표 한 개만, 다른 텍스트 금지.\n'
            '| 구분 | 대사(원문 그대로 1줄) |\n| :--- | :--- |\n| 기 (상황 도입) | ... |\n| 승 (사건 전개) | ... |\n| 전 (위기/전환) | ... |\n| 결 (결말) | ... |'
        )

    def _clean_sentences_ko(text: str):
        t = (text or '')
        # remove brackets and markers
        import re
        t = re.sub(r"\[[^\]]*\]", " ", t)
        t = re.sub(r"^\s*>>.*", " ", t, flags=re.MULTILINE)
        t = re.sub(r"\b(음악|박수|웃음|침묵|배경음|기침)\b", " ", t, flags=re.IGNORECASE)
        t = t.replace('\r', '\n')
        t = re.sub(r"\n{2,}", "\n", t)
        t = re.sub(r"[\t ]{2,}", " ", t)
        # Korean sentence ending hints
        t = re.sub(r"(요|다|죠|네|습니다|습니까|네요|군요)([.!?])", r"\1\2\n", t)
        # split
        parts = re.split(r"(?<=[.!?…]|\n)\s+", t)
        parts = [p.strip() for p in parts if p and p.strip()]
        # merge short
        out = []
        buf = ''
        MIN = 10
        for p in parts:
            cur = (buf + ' ' + p).strip() if buf else p
            if len(cur) < MIN:
                buf = cur
                continue
            out.append(cur)
            buf = ''
        if buf:
            out.append(buf)
        # remove noise
        out = [s for s in out if len(re.sub(r"[^\w\d가-힣]", "", s)) >= 3]
        return out[:300]

    def _estimate_dopamine(sentence: str) -> int:
        s = (sentence or '').lower()
        score = 3
        if any(k in s for k in ['충격', '반전', '경악', '미친', '대폭', '폭로', '소름', '!', '?']):
            score += 5
        return max(1, min(10, score))

    def _analyze_video_fast(doc):  # type: ignore
        transcript = (doc or {}).get('transcript_text') or ''
        if not transcript:
            raise RuntimeError('no transcript_text in DB')
        # Trim overly long transcripts to improve latency
        max_chars = int(os.getenv('GEMINI_MAX_CHARS') or '12000')
        tshort = transcript if len(transcript) <= max_chars else transcript[:max_chars]
        jobs = {
            'material': (_build_material_prompt(), tshort),
            'hooking': (_build_hooking_prompt(), tshort),
            'structure': (_build_structure_prompt(), tshort)
        }
        results = { 'material': '', 'hooking': '', 'structure': '' }
        with ThreadPoolExecutor(max_workers=3) as ex:
            fut_map = { ex.submit(_call_gemini, p, txt): name for name, (p, txt) in jobs.items() }
            for fut in as_completed(fut_map):
                name = fut_map[fut]
                try:
                    results[name] = (fut.result() or '').strip()
                except Exception as e:
                    results[name] = ''
        # dopamine graph (local)
        sentences = _clean_sentences_ko(tshort)
        dopamine_graph = [{ 'sentence': s, 'level': _estimate_dopamine(s), 'reason': 'heuristic' } for s in sentences]
        # Fallbacks to reduce partial-missing fields
        if not results['material']:
            top = sorted(dopamine_graph, key=lambda x: x['level'], reverse=True)
            cand = (top[0]['sentence'] if top else (sentences[0] if sentences else ''))
            results['material'] = (cand or '')[:120]
        if not results['hooking']:
            # user preference: first sentence is the hook
            first = sentences[0] if sentences else tshort.split('\n')[0]
            results['hooking'] = (first or '')[:200]
        if not results['structure']:
            if sentences:
                n = len(sentences)
                q1 = sentences[0]
                q2 = sentences[min(n//3, n-1)]
                q3 = sentences[min(2*n//3, n-1)]
                q4 = sentences[-1]
                results['structure'] = f"기: {q1}\n승: {q2}\n전: {q3}\n결: {q4}"
        return {
            'material': results['material'][:2000],
            'hooking': results['hooking'][:2000],
            'narrative_structure': results['structure'][:4000],
            'dopamine_graph': dopamine_graph,
            'analysis_transcript_len': len(transcript),
            'last_modified': int(time.time()*1000)
        }

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


@app.route('/', methods=['POST', 'OPTIONS'])
@app.route('/analyze_one', methods=['POST', 'OPTIONS'])
@app.route('/api/analyze_one', methods=['POST', 'OPTIONS'])
def analyze_one():
    if request.method == 'OPTIONS':
        return ('', 204)
    # Readiness: _analyze_video는 사용하지 않으므로 _load_sb만 확인
    if _load_sb is None:
        return jsonify({ 'ok': False, 'error': 'server_not_ready' }), 500
    try:
        stage = 'load_sb'
        sb = _load_sb()
        body = {}
        try:
            body = request.get_json(force=True) or {}
        except Exception:
            body = {}
        vid = str(body.get('id') or '').strip()
        if not vid:
            return jsonify({ 'ok': False, 'error': 'missing id' }), 400
        stage = 'fetch_video'
        row = sb.table('videos').select('*').eq('id', vid).limit(1).execute()
        rows = getattr(row, 'data', []) or []
        if not rows:
            return jsonify({ 'ok': False, 'error': 'not_found' }), 404
        video = rows[0]
        stage = 'analyze'
        # use faster analyzer
        updated = _analyze_video_fast(video) or {}
        if updated:
            # 스키마에 없는 컬럼은 제거
            allowed = set(video.keys())
            payload = { k: v for k, v in updated.items() if k in allowed }
            if payload:
                stage = 'update'
                sb.table('videos').update(payload).eq('id', vid).execute()
        wanted = list(updated.keys()) if updated else []
        saved = list(payload.keys()) if updated else []
        skipped = [k for k in wanted if k not in (saved or [])]
        return jsonify({ 'ok': True, 'updated': bool(updated), 'saved_keys': saved, 'skipped_keys': skipped })
    except Exception as e:
        app.logger.exception('analyze_one failed')
        return jsonify({ 'ok': False, 'error': str(e), 'stage': locals().get('stage', 'unknown'), 'trace': traceback.format_exc()[:2000] }), 500


@app.route('/health', methods=['GET'])
def health():
    return ('ok', 200)

@app.route('/debug', methods=['GET'])
@app.route('/api/analyze_one/debug', methods=['GET'])
def debug():
    try:
        info = {
            'has_SUPABASE_URL': bool(os.getenv('SUPABASE_URL')),
            'has_SUPABASE_SERVICE_ROLE_KEY': bool(os.getenv('SUPABASE_SERVICE_ROLE_KEY')),
            'has_SUPABASE_ANON_KEY': bool(os.getenv('SUPABASE_ANON_KEY')),
            'has_GEMINI_API_KEY': bool(os.getenv('GEMINI_API_KEY')),
            'routes': ['/api/analyze_one', '/api/analyze_one/debug', '/api/health']
        }
        return jsonify({ 'ok': True, 'env': info })
    except Exception as e:
        return jsonify({ 'ok': False, 'error': str(e) }), 500


