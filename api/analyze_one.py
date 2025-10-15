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
                    res = requests.post(url, json=payload, timeout=240)
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
        return (
            "너는 이제 내 유튜브 채널의 서브작가야. 내가 만든 유튜브 쇼츠 영상 중 100만 조회수 이상 영상만 추려내서 "
            "분석하려고 해. 내 고조회수 영상 대본을 꼼꼼하게 분석해서 내 채널의 정체성을 파악하고 결을 잡아갈 거야. "
            "항상 '고조회수 성공 패턴'의 관점에서 요약과 분류를 해줘."
        )

    def _build_material_prompt() -> str:
        return (
            _persona() + '\n\n'
            '아래 대본을 읽고 반드시 JSON만 출력하세요. 다른 텍스트/머리말/코드펜스 금지.\n'
            '{\n'
            '  "main_idea": "영상이 전달하려는 핵심 메시지를 1문장",\n'
            '  "core_materials": ["핵심 소재를 3~7개, 간결한 명사구"],\n'
            '  "lang_patterns": ["반복되는 언어/표현 3~6개"],\n'
            '  "emotion_points": ["감정 몰입 포인트 3~6개"],\n'
            '  "info_delivery": ["정보 전달 방식 특징 3~6개"]\n'
            '}'
        )

    def _build_hooking_prompt() -> str:
        return (
            _persona() + '\n\n'
            '2. 후킹 프롬프트 — "영상에 쓰인 후킹 패턴은?"\n'
            '대본의 시작부(가능하면 첫 문장 기준)에서 시청자의 궁금증을 유발한 핵심을 1줄로 "요약"하고, 사용된 후킹 패턴을 분류해 표로 작성하세요.\n'
            '출력은 마크다운 표 한 개만(열 머리 포함), 다른 텍스트 금지.\n'
            '| 🤔 후킹 요약 | 패턴(분류) |\n| :--- | :--- |\n| (시작부 요약 1줄) | (예: 의문제시/과장/반전/위기제시/금기발화/강한명령/모순 제시 등) |'
        )

    def _build_structure_prompt() -> str:
        return (
            _persona() + '\n\n'
            '1. 기승전결 프롬프트 — "영상에 나타나는 기승전결 구조는?"\n'
            '대본에서 기·승·전·결의 핵심을 각 1문장으로 "요약"해 표로 작성하세요(원문 복사 금지).\n'
            '출력은 마크다운 표 한 개만, 다른 텍스트 금지.\n'
            '| 구분 | 요약 |\n| :--- | :--- |\n| 기 (상황 도입) | ... |\n| 승 (사건 전개) | ... |\n| 전 (위기/전환) | ... |\n| 결 (결말) | ... |'
        )

    def _parse_material_sections(text: str):
        # Prefer strict JSON parse; fallback to regex capture
        import re, json as _json
        main_idea = ''
        core_materials = []
        lang_patterns = []
        emotion_points = []
        info_delivery = []
        t = (text or '').strip()
        if not t:
            return {
                'main_idea': main_idea,
                'core_materials': core_materials,
                'lang_patterns': lang_patterns,
                'emotion_points': emotion_points,
                'info_delivery': info_delivery
            }
        # Try JSON
        try:
            payload = _json.loads(t)
            if isinstance(payload, dict):
                main_idea = str(payload.get('main_idea') or '').strip()
                core_materials = [str(x).strip() for x in (payload.get('core_materials') or []) if str(x).strip()][:12]
                lang_patterns = [str(x).strip() for x in (payload.get('lang_patterns') or []) if str(x).strip()][:12]
                emotion_points = [str(x).strip() for x in (payload.get('emotion_points') or []) if str(x).strip()][:12]
                info_delivery = [str(x).strip() for x in (payload.get('info_delivery') or []) if str(x).strip()][:12]
                return {
                    'main_idea': main_idea,
                    'core_materials': core_materials,
                    'lang_patterns': lang_patterns,
                    'emotion_points': emotion_points,
                    'info_delivery': info_delivery
                }
        except Exception:
            # try to extract JSON object inside code fences or surrounding text
            try:
                start = t.index('{')
                end = t.rindex('}') + 1
                payload = _json.loads(t[start:end])
                if isinstance(payload, dict):
                    main_idea = str(payload.get('main_idea') or '').strip()
                    core_materials = [str(x).strip() for x in (payload.get('core_materials') or []) if str(x).strip()][:12]
                    lang_patterns = [str(x).strip() for x in (payload.get('lang_patterns') or []) if str(x).strip()][:12]
                    emotion_points = [str(x).strip() for x in (payload.get('emotion_points') or []) if str(x).strip()][:12]
                    info_delivery = [str(x).strip() for x in (payload.get('info_delivery') or []) if str(x).strip()][:12]
                    return {
                        'main_idea': main_idea,
                        'core_materials': core_materials,
                        'lang_patterns': lang_patterns,
                        'emotion_points': emotion_points,
                        'info_delivery': info_delivery
                    }
            except Exception:
                pass
        # Fallback: header-based capture
        t = t.replace('\r', '')
        m = re.search(r"메인\s*아이디어\s*\(Main\s*Idea\)\s*[:：]\s*(.+)", t)
        if m:
            main_idea = m.group(1).strip()
        def capture_list_after(header_patterns, stop_patterns):
            pat = re.compile(header_patterns, re.I)
            stop = re.compile(stop_patterns, re.I) if stop_patterns else None
            lines = t.split('\n')
            capturing = False
            acc = []
            for line in lines:
                if not capturing and pat.search(line):
                    capturing = True
                    continue
                if capturing:
                    if stop and stop.search(line):
                        break
                    s = line.strip()
                    if not s:
                        continue
                    if re.match(r"^\s*(메인\s*아이디어|핵심\s*소재|3-1|3-2|3-3)\b", s):
                        break
                    s = re.sub(r"^[-*•·]\s*", '', s)
                    acc.append(s)
            return [x for x in acc if x and len(x) > 1][:12]
        core_materials = capture_list_after(r"핵심\s*소재\s*\(Core\s*Materials\)\s*:?", r"^(3-1|3-2|3-3)\b")
        lang_patterns = capture_list_after(r"^(3-1\s*반복되는\s*언어\s*패턴)\b|반복되는\s*언어\s*패턴\s*[:：]", r"^(3-2|3-3)\b")
        emotion_points = capture_list_after(r"^(3-2\s*감정\s*몰입\s*포인트)\b|감정\s*몰입.*[:：]", r"^(3-3)\b")
        info_delivery = capture_list_after(r"^(3-3\s*정보\s*전달\s*방식\s*특징)\b|정보\s*전달\s*방식.*[:：]", None)
        return {
            'main_idea': main_idea,
            'core_materials': core_materials,
            'lang_patterns': lang_patterns,
            'emotion_points': emotion_points,
            'info_delivery': info_delivery
        }

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
        # Richer heuristic for varied scores when LLM parsing fails
        s = (sentence or '').strip()
        s_lower = s.lower()
        score = 5
        # curiosity words
        if any(k in s_lower for k in ['왜', '어떻게', '정말', '충격', '반전', '경악', '대박', '소름', '비밀', '최초', '금지', '경고']):
            score += 2
        # punctuation intensity
        excl = s.count('!')
        quest = s.count('?')
        score += min(2, excl) + min(2, quest)
        # numbers and superlatives
        if any(ch.isdigit() for ch in s):
            score += 1
        if any(k in s_lower for k in ['가장', '최고', '최악', '첫', '완전']):
            score += 1
        # length normalization
        ln = len(s)
        if ln < 20:
            score -= 1
        elif ln > 120:
            score -= 1
        return max(1, min(10, score))

    def _safe_json_arr(text: str):
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, list) else []
        except Exception:
            return []

    def _build_dopamine_prompt(sentences):
        header = '다음 "문장 배열"에 대해, 각 문장별로 궁금증/도파민 유발 정도를 1~10 정수로 평가하고, 그 이유를 간단히 설명하세요. 반드시 JSON 배열로만, 요소는 {"sentence":"문장","level":정수,"reason":"이유"} 형태로 출력하세요. 여는 대괄호부터 닫는 대괄호까지 외 텍스트는 출력하지 마세요.'
        return header + '\n\n문장 배열:\n' + json.dumps(sentences, ensure_ascii=False)

    # --- Validators to strictly require LLM-formatted outputs ---
    def _is_md_table(text: str) -> bool:
        t = (text or '').strip()
        return '|' in t and '\n' in t and t.count('|') >= 6

    def _looks_like_structure_table(text: str) -> bool:
        t = (text or '').lower()
        return ('| 기' in t) and ('| 승' in t) and ('| 전' in t) and ('| 결' in t)

    def _material_json_ok(text: str) -> bool:
        try:
            obj = json.loads(text)
            return (
                isinstance(obj, dict) and
                isinstance(obj.get('main_idea', ''), str) and
                isinstance(obj.get('core_materials', []), list) and
                isinstance(obj.get('lang_patterns', []), list) and
                isinstance(obj.get('emotion_points', []), list) and
                isinstance(obj.get('info_delivery', []), list)
            )
        except Exception:
            return False

    def _call_strict(kind: str, prompt: str, content: str, validator, tries: int = 3) -> str:
        last = ''
        for _ in range(max(1, tries)):
            last = (_call_gemini(prompt, content) or '').strip()
            if validator(last):
                break
            try:
                time.sleep(0.3)
            except Exception:
                pass
        return last

    def _call_array_only(kind: str, label: str, text: str, min_len: int = 3, max_len: int = 8) -> list:
        prompt = (
            _persona() + '\n\n'
            f'아래 대본을 참고해 "{label}" 항목을 {min_len}~{max_len}개 추출하세요.\n'
            '반드시 JSON 배열로만 출력하세요. 예: ["항목1","항목2"]\n'
            '불릿/설명/코드펜스/기타 텍스트 금지.'
        )
        out = []
        try:
            raw = _call_gemini(prompt, text)
            arr = json.loads(raw)
            if isinstance(arr, list):
                out = [str(x).strip() for x in arr if str(x).strip()][:max_len]
        except Exception:
            out = []
        return out

    def _analyze_video_fast(doc):  # type: ignore
        transcript = (doc or {}).get('transcript_text') or ''
        if not transcript:
            raise RuntimeError('no transcript_text in DB')
        # Trim overly long transcripts to improve latency
        max_chars = int(os.getenv('GEMINI_MAX_CHARS') or '12000')
        tshort = transcript if len(transcript) <= max_chars else transcript[:max_chars]
        # 후킹 입력은 시작부 요약 정확도를 위해 처음 2~3문장만 전달
        first_sents = _clean_sentences_ko(tshort)[:3]
        hook_input = ' '.join(first_sents)[:800]
        # Strict LLM calls without transcript-derived fallbacks
        results = { 'material': '', 'hooking': '', 'structure': '' }
        with ThreadPoolExecutor(max_workers=3) as ex:
            futs = {
                'material': ex.submit(_call_strict, 'material', _build_material_prompt(), tshort, _material_json_ok, 3),
                # 후킹/구조는 형식을 강제하지 않고 비어있지만 않으면 저장
                'hooking': ex.submit(_call_strict, 'hooking', _build_hooking_prompt(), (hook_input or tshort[:1200]), lambda s: bool((s or '').strip()), 2),
                'structure': ex.submit(_call_strict, 'structure', _build_structure_prompt(), tshort, lambda s: bool((s or '').strip()), 2)
            }
            for k, f in futs.items():
                try:
                    results[k] = (f.result() or '').strip()
                except Exception:
                    results[k] = ''
        # dopamine graph (LLM, chunked)
        sentences = _clean_sentences_ko(tshort)
        dopamine_graph = []
        batch_size = 50
        for i in range(0, len(sentences), batch_size):
            sub = sentences[i:i+batch_size]
            try:
                resp = _call_gemini(_build_dopamine_prompt(sub), '')
                arr = _safe_json_arr(resp)
                for item in arr:
                    s = str(item.get('sentence') or item.get('text') or '')
                    try:
                        level = int(round(float(item.get('level') or item.get('score') or 0)))
                    except Exception:
                        level = 1
                    level = max(1, min(10, level))
                    dopamine_graph.append({ 'sentence': s, 'level': level, 'reason': str(item.get('reason') or '') })
            except Exception:
                # fallback to simple heuristic for this chunk
                for s in sub:
                    dopamine_graph.append({ 'sentence': s, 'level': _estimate_dopamine(s), 'reason': 'heuristic' })
        # No local transcript fallbacks — keep empty if model failed
        # parse material into sections for new detail boxes
        material_sections = _parse_material_sections(results['material'])
        # second-pass fill to guarantee non-empty sections (user prefers completeness over speed)
        if not material_sections.get('main_idea'):
            try:
                mi = _call_strict('main_idea', _persona() + '\n\n메인 아이디어만 1문장으로 출력. 다른 텍스트 금지.', tshort, lambda s: bool(s.strip()) and '\n' not in s and len(s) <= 200, 2)
                material_sections['main_idea'] = mi.strip() if mi else ''
            except Exception:
                pass
        if not material_sections.get('core_materials'):
            material_sections['core_materials'] = _call_array_only('core', '핵심 소재', tshort, 3, 7)
        if not material_sections.get('lang_patterns'):
            material_sections['lang_patterns'] = _call_array_only('lang', '반복되는 언어 패턴', tshort, 3, 6)
        if not material_sections.get('emotion_points'):
            material_sections['emotion_points'] = _call_array_only('emo', '감정 몰입 포인트', tshort, 3, 6)
        if not material_sections.get('info_delivery'):
            material_sections['info_delivery'] = _call_array_only('info', '정보 전달 방식 특징', tshort, 3, 6)
        return {
            'material': results['material'][:2000] if results['material'] else None,
            'material_main_idea': material_sections.get('main_idea')[:1000] if material_sections.get('main_idea') else None,
            'material_core_materials': material_sections.get('core_materials') or None,
            'material_lang_patterns': material_sections.get('lang_patterns') or None,
            'material_emotion_points': material_sections.get('emotion_points') or None,
            'material_info_delivery': material_sections.get('info_delivery') or None,
            'hooking': results['hooking'][:2000] if results['hooking'] else None,
            'narrative_structure': results['structure'][:4000] if results['structure'] else None,
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
        # 디버깅: 실제 저장된 값 샘플 확인
        sample_fields = {}
        if updated:
            for k in ['material', 'hooking', 'narrative_structure']:
                v = updated.get(k)
                if v:
                    sample_fields[k] = str(v)[:100] + '...' if len(str(v)) > 100 else str(v)
        return jsonify({ 'ok': True, 'updated': bool(updated), 'saved_keys': saved, 'skipped_keys': skipped, 'sample': sample_fields })
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


