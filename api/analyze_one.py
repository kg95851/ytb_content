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
    
    # Global key rotation state
    _gemini_key_index = 0
    _gemini_keys_cache = None

    def _load_sb():  # type: ignore
        if create_client is None:
            raise RuntimeError('supabase client not available')
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')
        if not url or not key:
            raise RuntimeError('Missing SUPABASE_URL or SUPABASE_*_KEY env')
        return create_client(url, key)
    
    def _get_next_gemini_key():
        global _gemini_key_index, _gemini_keys_cache
        
        # Load keys only once and cache them
        if _gemini_keys_cache is None:
            keys = []
            
            # Try comma-separated first (GEMINI_API_KEYS="key1,key2,key3")
            multi_keys = os.getenv('GEMINI_API_KEYS')
            if multi_keys:
                keys = [k.strip() for k in multi_keys.split(',') if k.strip()]
                print(f"Found {len(keys)} keys from GEMINI_API_KEYS")
            
            # Also try numbered keys (GEMINI_API_KEY1, GEMINI_API_KEY2, etc.)
            if not keys:
                for i in range(1, 101):
                    key = os.getenv(f'GEMINI_API_KEY{i}')
                    if key:
                        # Remove quotes if present (common mistake)
                        key = key.strip().strip('"').strip("'")
                        if key and key.startswith('AIza'):  # Valid Gemini key format
                            keys.append(key)
                if keys:
                    print(f"Total {len(keys)} numbered keys found (GEMINI_API_KEY1..{len(keys)})")
            
            # Fall back to single key
            if not keys:
                single_key = os.getenv('GEMINI_API_KEY')
                if single_key:
                    single_key = single_key.strip().strip('"').strip("'")
                    keys = [single_key]
                    print("Using single GEMINI_API_KEY")
            
            if not keys:
                # Debug: show what environment variables exist
                env_vars = [k for k in os.environ.keys() if 'GEMINI' in k]
                print(f"Available GEMINI env vars: {env_vars}")
                raise RuntimeError('No valid GEMINI_API_KEY found')
            
            _gemini_keys_cache = keys
            print(f"=== Initialized with {len(keys)} Gemini API keys ===")
        
        # Use cached keys
        keys = _gemini_keys_cache
        
        # Get the next key in rotation
        key = keys[_gemini_key_index % len(keys)]
        current_index = (_gemini_key_index % len(keys)) + 1
        _gemini_key_index += 1
        
        # Always log in development to verify all keys are being used
        print(f"[KEY ROTATION] Using key #{current_index}/{len(keys)} | Total calls: {_gemini_key_index} | Key: ...{key[-8:]}")
        
        # Log summary every full rotation
        if current_index == len(keys):
            print(f"[KEY ROTATION] ✓ Complete rotation - All {len(keys)} keys have been used")
        
        return key, len(keys)

    def _call_gemini(system_prompt: str, user_content: str) -> str:
        import time
        import random
        
        # Only use Gemini 2.5 Flash as requested
        model = 'models/gemini-2.5-flash'
        api_ver = 'v1'
        
        payload = {
            'contents': [
                { 'role': 'user', 'parts': [{ 'text': f"{system_prompt}\n\n{user_content}" }] }
            ],
            'generationConfig': { 'temperature': 0.3 }
        }
        base = 'https://generativelanguage.googleapis.com'
        
        all_errors = []
        
        # Get total number of keys available
        try:
            _, total_keys = _get_next_gemini_key()
        except Exception as e:
            raise RuntimeError(f"No API keys available: {e}")
        
        # Try ALL keys with fast rotation (2 rounds max)
        max_rounds = 2
        attempts = 0
        used_keys = set()  # Track which keys have been used
        
        for round_num in range(max_rounds):
            for key_index in range(total_keys):
                attempts += 1
                
                try:
                    api_key, _ = _get_next_gemini_key()
                    current_key = (key_index % total_keys) + 1
                    used_keys.add(current_key)  # Track this key usage
                    
                    # Delay strategy
                    if attempts == 1:
                        # First attempt - minimal delay
                        time.sleep(0.2)
                    elif round_num == 0:
                        # First round - quick rotation
                        delay = 0.3 + random.uniform(0, 0.2)
                        time.sleep(delay)
                    else:
                        # Second round - slightly longer delay
                        delay = 1.0 + random.uniform(0, 0.5)
                        time.sleep(delay)
                        if key_index == 0:
                            print(f"Starting round {round_num + 1} after {delay:.1f}s delay")
                    
                    url = f"{base}/{api_ver}/{model}:generateContent?key={api_key}"
                    
                    try:
                        # Reduced timeout for Vercel Pro: 55 seconds max
                        res = requests.post(url, json=payload, timeout=55)
                        
                        if res.status_code == 429:
                            # Rate limited - DO NOT SAVE ERROR RESPONSE
                            all_errors.append(f"429-key{current_key}/{total_keys}")
                            print(f"[429 ERROR] Key {current_key}/{total_keys} rate limited")
                            print(f"[429 ERROR] Rotating to next key (will not save error to DB)")
                            # Small delay before trying next key
                            time.sleep(0.8 + random.uniform(0, 0.4))
                            continue  # Try next key immediately without saving
                        
                        if res.status_code == 404:
                            # Model not found - this is a configuration error
                            raise RuntimeError(f"Model {model} not found - check model name")
                        
                        res.raise_for_status()
                        data = res.json()
                        
                        # Extract text from response
                        candidates = data.get('candidates', [])
                        if candidates:
                            content = candidates[0].get('content', {})
                            parts = content.get('parts', [])
                            if parts:
                                text = parts[0].get('text', '')
                                if text:
                                    # Success!
                                    if attempts > 1:
                                        print(f"Success with key {current_key}/{total_keys} after {attempts} attempts")
                                    return text
                        
                        all_errors.append(f"empty-response-key{current_key}")
                        
                    except requests.exceptions.Timeout:
                        all_errors.append(f"timeout-key{current_key}")
                        print(f"Timeout with key {current_key}/{total_keys}, rotating...")
                        continue
                        
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            all_errors.append(f"429-key{current_key}")
                            print(f"HTTP 429 with key {current_key}/{total_keys}, rotating...")
                        else:
                            all_errors.append(f"http-error-key{current_key}:{e.response.status_code}")
                        continue
                        
                    except requests.exceptions.RequestException as e:
                        all_errors.append(f"request-error-key{current_key}:{str(e)[:30]}")
                        continue
                        
                except Exception as e:
                    all_errors.append(f"unexpected:{str(e)[:30]}")
                    continue
            
            # After first round, wait a bit before second round
            if round_num == 0 and total_keys > 0:
                wait_time = 3.0 + random.uniform(0, 2)
                print(f"Completed first round of {total_keys} keys, waiting {wait_time:.1f}s before round 2...")
                time.sleep(wait_time)
        
        # All attempts failed
        error_summary = '; '.join(all_errors[-5:])  # Show last 5 errors
        print(f"[KEY USAGE SUMMARY] Used {len(used_keys)}/{total_keys} unique keys in {attempts} attempts")
        if len(used_keys) < total_keys:
            print(f"[WARNING] Not all keys were used! Used keys: {sorted(used_keys)}")
        raise RuntimeError(f'Gemini request failed after {attempts} attempts with {total_keys} keys: {error_summary}')

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

    def _analyze_patterns_only(doc):  # type: ignore
        """Phase 2: Analyze only the 4 detailed patterns"""
        transcript = (doc or {}).get('transcript_text') or ''
        if not transcript:
            return {}
        
        # Already have basic analysis, now get detailed patterns
        max_chars = 8000  # Less text for phase 2
        tshort = transcript if len(transcript) <= max_chars else transcript[:max_chars]
        
        results = {}
        
        # Detailed patterns prompt (simpler, focused)
        patterns_prompt = """대본을 분석하여 다음 4가지 패턴을 구체적으로 추출하세요. 
각 항목마다 실제 대본에서 인용하여 예시를 포함하세요.

JSON 형식으로만 출력:
{
  "core_materials": ["핵심 소재 3-5개 (예: '펜타킬', '3대5 상황' 등)"],
  "lang_patterns": ["반복 언어 패턴 3-5개와 실제 인용 (예: '미친' - '미친 3대5 펜타킬')"],
  "emotion_points": ["감정 몰입 포인트 3-5개와 해당 대사 (예: [긴장감] - '이게 되나?')"],
  "info_delivery": ["정보 전달 특징 3-5개와 예시 (예: [게임 용어 사용] - '펜타킬', '탱킹')"]
}"""
        
        try:
            import time
            import random
            time.sleep(1.0 + random.uniform(0, 0.5))  # Small delay
            
            resp = _call_gemini(patterns_prompt, tshort)
            if resp:
                # Parse response
                resp_clean = resp.strip()
                if resp_clean.startswith('```json'):
                    resp_clean = resp_clean[7:]
                if resp_clean.startswith('```'):
                    resp_clean = resp_clean[3:]
                if resp_clean.endswith('```'):
                    resp_clean = resp_clean[:-3]
                resp_clean = resp_clean.strip()
                
                try:
                    parsed = json.loads(resp_clean)
                    results['material_core_materials'] = parsed.get('core_materials', [])[:10]
                    results['material_lang_patterns'] = parsed.get('lang_patterns', [])[:10]
                    results['material_emotion_points'] = parsed.get('emotion_points', [])[:10]
                    results['material_info_delivery'] = parsed.get('info_delivery', [])[:10]
                except:
                    # Fallback
                    results['material_core_materials'] = ['패턴 분석 실패']
                    results['material_lang_patterns'] = ['패턴 분석 실패']
                    results['material_emotion_points'] = ['감정 분석 실패']
                    results['material_info_delivery'] = ['전달 방식 분석 실패']
        except Exception as e:
            print(f"Phase 2 analysis error: {e}")
            # Return empty to not overwrite existing data
            return {}
        
        # Add timestamp
        results['last_modified'] = int(time.time() * 1000)
        return results

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
        # Direct LLM calls without strict validation - just get the response
        results = { 'material': '', 'hooking': '', 'structure': '' }
        
        # Combine all analyses into ONE LLM call to reduce API calls
        import time
        
        # Simplified prompt for phase 1 (faster)
        combined_prompt = """대본을 빠르게 분석하여 JSON 출력:
{
  "material": "핵심 주제 1-2문장 요약",
  "hooking": "첫 문장 후킹 기법",
  "structure": "기: (도입), 승: (전개), 전: (클라이맥스), 결: (마무리)"
}
JSON만 출력, 간단명료하게:

대본 분석:"""
        
        try:
            # Single API call for all three analyses
            # Minimal delay for phase 1
            import random
            time.sleep(0.5 + random.uniform(0, 0.5))  # 0.5-1 second only
            combined_resp = _call_gemini(combined_prompt, tshort[:4000])  # Less text for faster response
            
            # Debug: Log the raw response length
            if combined_resp:
                print(f"LLM response length: {len(combined_resp)} chars")
                if len(combined_resp) < 500:
                    print(f"Warning: Short response - {combined_resp[:200]}")
            
            # Parse combined response
            if combined_resp:
                # Clean response to extract JSON
                resp_clean = combined_resp.strip()
                if resp_clean.startswith('```json'):
                    resp_clean = resp_clean[7:]
                if resp_clean.startswith('```'):
                    resp_clean = resp_clean[3:]
                if resp_clean.endswith('```'):
                    resp_clean = resp_clean[:-3]
                resp_clean = resp_clean.strip()
                
                try:
                    # Try to parse as JSON
                    parsed = json.loads(resp_clean)
                    results['material'] = parsed.get('material', '')[:2000] or '소재 분석 실패'
                    results['hooking'] = parsed.get('hooking', '')[:1000] or '후킹 분석 실패'
                    # Get full structure without truncation
                    structure = parsed.get('structure', '')
                    # Ensure we have the complete structure with all 4 parts
                    if structure:
                        # Check if it's already complete
                        if all(part in structure for part in ['기:', '승:', '전:', '결:']):
                            results['structure'] = structure[:4000]  # Much larger limit
                        else:
                            # Structure might be truncated, try to reconstruct
                            print(f"Incomplete structure detected: {structure[:100]}...")
                            results['structure'] = structure[:4000] if structure else '구조 분석 실패'
                    else:
                        results['structure'] = '구조 분석 실패'
                except Exception as e:
                    # If JSON parsing fails, try to extract manually
                    print(f"JSON parsing failed: {e}, trying manual extraction")
                    
                    # Try to extract each field manually from the response
                    if '"material"' in combined_resp and '"hooking"' in combined_resp and '"structure"' in combined_resp:
                        try:
                            # Extract material
                            mat_start = combined_resp.find('"material"') + len('"material"')
                            mat_start = combined_resp.find('"', mat_start) + 1
                            mat_end = combined_resp.find('"', mat_start)
                            while mat_end > 0 and combined_resp[mat_end-1] == '\\':
                                mat_end = combined_resp.find('"', mat_end + 1)
                            results['material'] = combined_resp[mat_start:mat_end] if mat_end > mat_start else '소재 추출 실패'
                            
                            # Extract hooking
                            hook_start = combined_resp.find('"hooking"') + len('"hooking"')
                            hook_start = combined_resp.find('"', hook_start) + 1
                            hook_end = combined_resp.find('"', hook_start)
                            while hook_end > 0 and combined_resp[hook_end-1] == '\\':
                                hook_end = combined_resp.find('"', hook_end + 1)
                            results['hooking'] = combined_resp[hook_start:hook_end] if hook_end > hook_start else '후킹 추출 실패'
                            
                            # Extract structure - need to handle the entire string with colons
                            struct_start = combined_resp.find('"structure"') + len('"structure"')
                            struct_start = combined_resp.find('"', struct_start) + 1
                            # Find the closing quote, but handle escaped quotes and multi-line content
                            struct_end = struct_start
                            while struct_end < len(combined_resp):
                                struct_end = combined_resp.find('"', struct_end)
                                if struct_end == -1:
                                    struct_end = len(combined_resp)
                                    break
                                # Check if this quote is escaped
                                if combined_resp[struct_end-1] != '\\':
                                    # Also check if we have all 4 parts
                                    temp_str = combined_resp[struct_start:struct_end]
                                    if all(part in temp_str for part in ['기:', '승:', '전:', '결:']):
                                        break
                                struct_end += 1
                            results['structure'] = combined_resp[struct_start:struct_end] if struct_end > struct_start else '구조 추출 실패'
                        except:
                            # Last resort: use the full response
                            results['material'] = combined_resp[:600] if combined_resp else '분석 실패'
                            results['hooking'] = '후킹 분석 실패'
                            results['structure'] = '구조 분석 실패'
                    else:
                        # Response doesn't look like JSON at all
                        results['material'] = combined_resp[:600] if combined_resp else '분석 실패'
                        results['hooking'] = '후킹 분석 실패'
                        results['structure'] = '구조 분석 실패'
            else:
                results['material'] = '응답 없음'
                results['hooking'] = '응답 없음'
                results['structure'] = '응답 없음'
                
        except Exception as e:
            print(f"Combined analysis error: {e}")
            results['material'] = f'분석 오류: {str(e)[:100]}'
            results['hooking'] = '분석 오류'
            results['structure'] = '분석 오류'
        # dopamine graph - simplified to reduce API calls
        sentences = _clean_sentences_ko(tshort)[:30]  # Limit to 30 sentences
        dopamine_graph = []
        # Skip LLM for dopamine to save API calls - use heuristic only
        for s in sentences:
            dopamine_graph.append({ 
                'sentence': s[:200], 
                'level': _estimate_dopamine(s), 
                'reason': 'auto' 
            })
        # parse material into sections for new detail boxes
        material_sections = _parse_material_sections(results['material'])
        
        # Always fill sections if empty - don't require strict format
        if not material_sections.get('main_idea') and results['material']:
            # Extract first meaningful line as main idea
            lines = results['material'].split('\n')
            for line in lines:
                if line.strip() and len(line.strip()) > 10:
                    material_sections['main_idea'] = line.strip()[:200]
                    break
                    
        # If still no sections, make direct calls
        if not material_sections.get('core_materials'):
            try:
                time.sleep(1.0)  # Delay for stability
                core_prompt = """영상의 핵심 소재와 주제를 구체적으로 나열하세요.
예시: '정치 스캔들', '고위직 의혹', '직접 추궁', '침묵/회피', '국민 주권 강조'
실제 영상의 핵심 소재 3-7개를 구체적으로 쉼표로 구분:"""
                resp = _call_gemini(core_prompt, tshort[:4000])
                if resp:
                    items = []
                    for item in resp.replace('\n', ',').split(','):
                        cleaned = item.strip().strip('*').strip('-').strip()
                        if cleaned and len(cleaned) > 2 and len(cleaned) < 30:
                            items.append(cleaned)
                    items = items[:7]
                    if items and len(items) >= 3:
                        material_sections['core_materials'] = items
                    else:
                        material_sections['core_materials'] = ['주요 사건', '핵심 인물', '중심 갈등']
            except Exception as e:
                print(f"Core materials analysis failed: {e}")
                material_sections['core_materials'] = ['핵심 주제', '주요 소재']
                
        # Ensure all arrays have actual content
        if not material_sections.get('lang_patterns') or material_sections['lang_patterns'] == ['반복 표현 분석 중', '패턴 추출 중']:
            try:
                time.sleep(1.0)  # Delay before secondary analysis
                lang_prompt = """대본에서 실제로 반복되는 구체적인 언어 패턴과 표현을 찾아 나열하세요.
예시: '~습니까?', '조희대 대법원장', '~하시면', '~잖아요', '그런데 ~'
실제 대본에서 2번 이상 나오는 구체적 표현 3-5개를 쉼표로 구분:"""
                resp = _call_gemini(lang_prompt, tshort[:4000])
                if resp and resp.strip():
                    items = [x.strip() for x in resp.replace('\n', ',').split(',') if x.strip() and len(x.strip()) > 2][:6]
                    if items and len(items) >= 2:
                        material_sections['lang_patterns'] = items
                    else:
                        # Fallback: extract common patterns manually
                        patterns = []
                        if '습니까' in tshort: patterns.append('~습니까?')
                        if '그런데' in tshort: patterns.append('그런데 ~')
                        if '이게' in tshort: patterns.append('이게 ~')
                        if not patterns: patterns = ['질문 형식', '강조 표현']
                        material_sections['lang_patterns'] = patterns
            except Exception as e:
                print(f"Lang patterns analysis failed: {e}")
                material_sections['lang_patterns'] = ['반복 질문', '직접 호칭']
                
        if not material_sections.get('emotion_points') or material_sections['emotion_points'] == ['감정 포인트 분석 중', '몰입 요소 추출 중']:
            try:
                time.sleep(1.0)  # Delay before secondary analysis
                emotion_prompt = f"""다음 대본을 분석하여 감정 몰입 포인트를 찾아주세요.

대본:
{tshort[:3000]}

위 대본에서 감정 몰입 포인트를 아래 형식으로 3-5개 작성하세요:
[감정 설명] - [실제 대본 인용]

예시:
아버지가 딸을 걱정하는 모습 - "우리 딸 어디 가는 거야?"
충격적 폭로 순간 - "사실 그때 그 사건의 진범은..."

각 항목은 쉼표로 구분:"""
                resp = _call_gemini(emotion_prompt, "")  # Already included transcript in prompt
                if resp and resp.strip():
                    items = []
                    # Parse response looking for [description] - [quote] format
                    for line in resp.replace('\n', ',').split(','):
                        cleaned = line.strip().strip('*').strip('-').strip()
                        if cleaned and '-' in cleaned:
                            parts = cleaned.split('-', 1)
                            if len(parts) == 2:
                                desc = parts[0].strip()
                                quote = parts[1].strip().strip('"').strip("'")
                                # Verify quote is from transcript or make sense
                                if desc and quote:
                                    formatted = f"{desc[:50]} - {quote[:80]}"
                                    items.append(formatted)
                    items = items[:5]
                    
                    if items and len(items) >= 2:
                        material_sections['emotion_points'] = items
                    else:
                        # Fallback: Find emotional moments with context
                        emotional_parts = []
                        sentences = [s.strip() for s in tshort.replace('?', '.').replace('!', '.').split('.') if s.strip()]
                        for i, sentence in enumerate(sentences[:50]):  # Check first 50 sentences
                            if any(word in sentence for word in ['충격', '놀라', '대박', '미친', '경악', '소름', '감동', '눈물', '분노', '화']):
                                context = "감정 고조 순간"
                                if '충격' in sentence or '놀라' in sentence: context = "충격적 순간"
                                elif '분노' in sentence or '화' in sentence: context = "분노 표출"
                                elif '눈물' in sentence or '감동' in sentence: context = "감동적 순간"
                                quote = sentence[:60] + ('...' if len(sentence) > 60 else '')
                                emotional_parts.append(f"{context} - \"{quote}\"")
                        material_sections['emotion_points'] = emotional_parts[:4] if emotional_parts else ['감정 분석 - 대본 확인 필요']
            except Exception as e:
                print(f"Emotion points analysis failed: {e}")
                material_sections['emotion_points'] = ['감정 포인트 - 재분석 필요']
                
        if not material_sections.get('info_delivery') or material_sections['info_delivery'] == ['전달 방식 분석 중', '구성 특징 추출 중']:
            try:
                time.sleep(1.0)  # Delay before secondary analysis
                delivery_prompt = f"""다음 대본을 분석하여 정보 전달 방식의 특징을 찾아주세요.

대본:
{tshort[:3000]}

위 대본에서 정보 전달 방식을 아래 형식으로 3-5개 작성하세요:
[전달 방식 특징] - [해당하는 대본 예시]

예시:
직접적인 질문 사용 - "언제 열람하셨습니까? 어떻게 보셨습니까?"
약품 효능 강조 - "이 약을 사면 암이 낫습니다"
반복적 추궁 - "대답해 주십시오. 왜 침묵하십니까?"

각 항목은 쉼표로 구분:"""
                resp = _call_gemini(delivery_prompt, "")  # Already included transcript in prompt
                if resp and resp.strip():
                    items = []
                    # Parse response looking for [style] - [example] format
                    for line in resp.replace('\n', ',').split(','):
                        cleaned = line.strip().strip('*').strip('-').strip()
                        if cleaned and '-' in cleaned:
                            parts = cleaned.split('-', 1)
                            if len(parts) == 2:
                                style = parts[0].strip()
                                example = parts[1].strip().strip('"').strip("'")
                                if style and example:
                                    formatted = f"{style[:40]} - {example[:60]}"
                                    items.append(formatted)
                    items = items[:5]
                    
                    if items and len(items) >= 2:
                        material_sections['info_delivery'] = items
                    else:
                        # Fallback: Analyze transcript patterns with examples
                        styles = []
                        sentences = tshort[:2000].split('.')
                        
                        # Find questions
                        questions = [s.strip() for s in tshort[:2000].split('?')[:3] if s.strip()]
                        if questions:
                            q_example = questions[0][-50:] if questions[0] else ""
                            styles.append(f"질문 형식 - \"{q_example}?\"")
                        
                        # Find commands/exclamations
                        exclaims = [s.strip() for s in tshort[:2000].split('!')[:3] if s.strip()]
                        if exclaims:
                            e_example = exclaims[0][-50:] if exclaims[0] else ""
                            styles.append(f"강조/명령 - \"{e_example}!\"")
                        
                        # Check for transitions
                        if '그런데' in tshort or '하지만' in tshort:
                            for s in sentences:
                                if '그런데' in s or '하지만' in s:
                                    styles.append(f"대조/전환 - \"{s[:60]}...\"")
                                    break
                        
                        material_sections['info_delivery'] = styles[:4] if styles else ['정보 전달 - 대본 분석 필요']
            except Exception as e:
                print(f"Info delivery analysis failed: {e}")
                material_sections['info_delivery'] = ['전달 방식 - 재분석 필요']
        return {
            'material': results['material'][:2000] if results['material'] else None,
            'material_main_idea': material_sections.get('main_idea')[:1000] if material_sections.get('main_idea') else None,
            'material_core_materials': material_sections.get('core_materials') or None,
            'material_lang_patterns': material_sections.get('lang_patterns') or None,
            'material_emotion_points': material_sections.get('emotion_points') or None,
            'material_info_delivery': material_sections.get('info_delivery') or None,
            'hooking': results['hooking'][:2000] if results['hooking'] else None,
            'narrative_structure': results['structure'][:4000] if results['structure'] else None,  # Enough space for full 기승전결
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
        phase = body.get('phase', 1)  # Default to phase 1
        if not vid:
            return jsonify({ 'ok': False, 'error': 'missing id' }), 400
        stage = 'fetch_video'
        row = sb.table('videos').select('*').eq('id', vid).limit(1).execute()
        rows = getattr(row, 'data', []) or []
        if not rows:
            return jsonify({ 'ok': False, 'error': 'not_found' }), 404
        video = rows[0]
        stage = 'analyze'
        
        # Phase-based analysis for Vercel Pro (60-second limit)
        if phase == 2:
            # Phase 2: Only analyze detailed patterns
            updated = _analyze_patterns_only(video) or {}
        else:
            # Phase 1: Basic analysis (material, hooking, structure, dopamine)
            updated = _analyze_video_fast(video) or {}
            # Mark that phase 2 is needed if phase 1 succeeded
            if updated and any(k in updated for k in ['material', 'hooking', 'narrative_structure']):
                updated['skip_phase2'] = False
        if updated:
            # 스키마에 없는 컬럼은 제거 + None 값 제외
            allowed = set(video.keys())
            payload = { k: v for k, v in updated.items() if k in allowed and v is not None }
            # 빈 배열도 제외 (DB가 null을 기대하는 경우)
            filtered_payload = {}
            for k, v in payload.items():
                if isinstance(v, list) and len(v) == 0:
                    continue  # skip empty arrays
                if isinstance(v, str) and v.strip() == '':
                    continue  # skip empty strings
                filtered_payload[k] = v
            if filtered_payload:
                stage = 'update'
                sb.table('videos').update(filtered_payload).eq('id', vid).execute()
            payload = filtered_payload  # use filtered for response
        wanted = list(updated.keys()) if updated else []
        saved = list(payload.keys()) if updated else []
        skipped = [k for k in wanted if k not in (saved or [])]
        # 디버깅: 실제 저장된 값 샘플 확인
        sample_fields = {}
        debug_info = {}
        if updated:
            for k in ['material', 'hooking', 'narrative_structure', 'material_core_materials', 'material_lang_patterns']:
                v = updated.get(k)
                if v is None:
                    debug_info[k] = 'None'
                elif isinstance(v, list):
                    debug_info[k] = f'list({len(v)} items)'
                    if len(v) > 0:
                        sample_fields[k] = str(v[0])[:50] if v else '[]'
                elif isinstance(v, str):
                    debug_info[k] = f'str({len(v)} chars)'
                    sample_fields[k] = v[:100] + '...' if len(v) > 100 else v
                else:
                    debug_info[k] = f'{type(v).__name__}'
        response = { 'ok': True, 'updated': bool(updated), 'saved_keys': saved, 'skipped_keys': skipped, 'sample': sample_fields, 'debug': debug_info }
        # Add skip_phase2 flag for client
        if phase == 1 and saved:
            response['skip_phase2'] = False  # Needs phase 2
        return jsonify(response)
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
        # Count actual API keys
        key_count = 0
        
        # Check comma-separated keys
        multi_keys = os.getenv('GEMINI_API_KEYS')
        if multi_keys:
            key_count = len([k.strip() for k in multi_keys.split(',') if k.strip()])
        
        # Check numbered keys if no comma-separated found
        if key_count == 0:
            for i in range(1, 101):
                if os.getenv(f'GEMINI_API_KEY{i}'):
                    key_count += 1
        
        # Check single key as fallback
        if key_count == 0 and os.getenv('GEMINI_API_KEY'):
            key_count = 1
            
        info = {
            'has_SUPABASE_URL': bool(os.getenv('SUPABASE_URL')),
            'has_SUPABASE_SERVICE_ROLE_KEY': bool(os.getenv('SUPABASE_SERVICE_ROLE_KEY')),
            'has_SUPABASE_ANON_KEY': bool(os.getenv('SUPABASE_ANON_KEY')),
            'has_GEMINI_API_KEY': bool(os.getenv('GEMINI_API_KEY')),
            'gemini_key_count': key_count,
            'routes': ['/api/analyze_one', '/api/analyze_one/debug', '/api/health']
        }
        return jsonify({ 'ok': True, 'env': info })
    except Exception as e:
        return jsonify({ 'ok': False, 'error': str(e) }), 500


