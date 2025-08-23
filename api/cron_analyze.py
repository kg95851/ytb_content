import os
import json
import time
from typing import List, Dict, Any, Tuple

from flask import Flask, jsonify, request
import requests

try:
    from google.cloud import firestore
    from google.oauth2 import service_account
except Exception:
    firestore = None
    service_account = None

try:
    from youtube_transcript_api import (
        YouTubeTranscriptApi,
        TranscriptsDisabled,
        NoTranscriptFound,
        VideoUnavailable,
    )
except Exception:
    YouTubeTranscriptApi = None


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


def _load_db():
    if firestore is None or service_account is None:
        raise RuntimeError('Firestore libraries not available')
    creds_json = os.getenv('GOOGLE_CLOUD_CREDENTIALS') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if not creds_json:
        raise RuntimeError('Missing GOOGLE_CLOUD_CREDENTIALS env with service account JSON')
    info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    project_id = info.get('project_id')
    return firestore.Client(project=project_id, credentials=credentials)


def _call_gemini(system_prompt: str, user_content: str) -> str:
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise RuntimeError('GEMINI_API_KEY not set')
    model = 'models/gemini-1.5-pro-latest'
    url = f'https://generativelanguage.googleapis.com/v1beta/{model}:generateContent?key={api_key}'
    payload = {
        'contents': [
            { 'role': 'user', 'parts': [{ 'text': f"{system_prompt}\n\n{user_content}" }] }
        ],
        'generationConfig': { 'temperature': 0.3 }
    }
    res = requests.post(url, json=payload, timeout=90)
    res.raise_for_status()
    data = res.json()
    return data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')


def _build_category_prompt() -> str:
    return (
        '다음 대본을 기반으로 카테고리를 아래 형식으로만 한 줄씩 정확히 출력하세요. 다른 텍스트/머리말/설명 금지.\n'
        '한국 대 카테고리: \n한국 중 카테고리: \n한국 소 카테고리: \n'
        'EN Main Category: \nEN Sub Category: \nEN Micro Topic: \n'
        '중국 대 카테고리: \n중국 중 카테고리: \n중국 소 카테고리: '
    )


def _build_keywords_prompt() -> str:
    return (
        '아래 제공된 "제목"과 "대본"을 모두 참고하여, 원본 영상을 검색해 찾기 쉬운 핵심 검색 키워드를 한국어/영어/중국어로 각각 8~15개씩 추출하세요.\n'
        '출력 형식은 JSON 객체만, 다른 설명/머리말/코드펜스 금지.\n'
        '요구 형식: {"ko":["키워드1","키워드2",...],"en":["keyword1",...],"zh":["关键词1",...]}\n'
        '규칙:\n- 각 키워드는 1~4단어의 짧은 구로 작성\n- 해시태그/특수문자/따옴표 제거, 불용어 제외\n- 동일 의미/중복 표현은 하나만 유지\n- 인명/채널명/브랜드/핵심 주제 포함\n'
    )


def _build_material_prompt() -> str:
    return '다음 대본의 핵심 소재를 한 문장으로 요약하세요. 반드시 한 줄로만, "소재: "로 시작하여 출력하세요. 다른 설명이나 불필요한 문자는 금지합니다.'


def _build_dopamine_prompt(sentences: List[str]) -> str:
    header = '다음 "문장 배열"에 대해, 각 문장별로 궁금증/도파민 유발 정도를 1~10 정수로 평가하고, 그 이유를 간단히 설명하세요. 반드시 JSON 배열로만, 요소는 {"sentence":"문장","level":정수,"reason":"이유"} 형태로 출력하세요. 여는 대괄호부터 닫는 대괄호까지 외 텍스트는 출력하지 마세요.'
    return header + '\n\n문장 배열:\n' + json.dumps(sentences, ensure_ascii=False)


def _build_analysis_prompt() -> str:
    # 축약 없이 동일 템플릿 유지
    return (
        "[GPTs Instructions 최종안]\n\n페르소나 (Persona)\n\n당신은 \"대본분석_룰루랄라릴리\"입니다. 유튜브 대본을 분석하여 콘텐츠 전략 수립과 프롬프트 최적화를 돕는 최고의 전문가입니다. 당신의 답변은 항상 체계적이고, 깔끔하며, 사용자가 바로 활용할 수 있도록 완벽하게 구성되어야 합니다.\n\n핵심 임무 (Core Mission)\n\n사용자가 유튜브 대본(영어 또는 한국어)을 입력하면, 아래 4번 항목의 **[출력 템플릿]**을 단 하나의 글자나 기호도 틀리지 않고 그대로 사용하여 분석 결과를 제공해야 합니다.\n\n절대 규칙 (Golden Rules)\n\n규칙 1: 템플릿 복제 - 출력물의 구조, 디자인, 순서, 항목 번호, 이모지(✨, 📌, 🎬, 🧐, 💡, ✅, 🤔), 강조(), 구분선(*) 등 모든 시각적 요소를 아래 **[출력 템플릿]**과 완벽하게 동일하게 재현해야 합니다.\n\n규칙 2: 순서 및 항목 준수 - 항상 0번, 1번, 2번, 3번, 4번, 5번, 6번, 7번, 8번,9번 항목을 빠짐없이, 순서대로 포함해야 합니다.\n\n규칙 3: 표 형식 유지 - 분석 내용의 대부분은 마크다운 표(Table)로 명확하게 정리해야 합니다.\n\n규칙 4: 내용의 구체성 - 각 항목에 필요한 분석 내용을 충실히 채워야 합니다. 특히 프롬프트 비교 시, 단순히 '유사함'에서 그치지 말고 이유를 명확히 설명해야 합니다.\n\n출력 템플릿 (Output Template) - 이 틀을 그대로 사용하여 답변할 것\n\n✨ 룰루 GPTs 분석 템플릿 적용 결과\n\n0. 대본 번역 (영어 → 한국어)\n(여기에 자연스러운 구어체 한국어 번역문을 작성한다.)\n\n1. 대본 기승전결 분석\n| 구분 | 내용 |\n| :--- | :--- |\n| 기 (상황 도입) | (여기에 '기'에 해당하는 내용을 요약한다.) |\n| 승 (사건 전개) | (여기에 '승'에 해당하는 내용을 요약한다.) |\n| 전 (위기/전환) | (여기에 '전'에 해당하는 내용을 요약한다.) |\n| 결 (결말) | (여기에 '결'에 해당하는 내용을 요약한다.) |\n\n2. 기존 프롬프트와의 미스매치 비교표\n| 프롬프트 번호 | 기 (문제 제기) | 승 (예상 밖 전개) | 전 (몰입·긴장 유도) | 결 (결론/인사이트) | 특징 | 미스매치 여부 |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n| 001 | 욕망 자극 | 수상한 전개 | 반전 | 허무/반전 결말 | 욕망+반전+유머 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 002 | 일상 시작 | 실용적 해결 | 낯선 기술 | 꿀팁 or 정리 | 실용+공감 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 003 | 위기 상황 | 극한 도전 | 생존 위기 | 실패 or 생존법 | 생존+경고 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 004 | 문화 충돌 | 오해 과정 | 이해 확장 | 감동 | 문화+인식 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 005 | 이상 행동 | 분석 진행 | 시각 변화 | 진실 발견 | 반전+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 006 | 멀쩡해 보임 | 내부 파헤침 | 충격 실체 | 소비자 경고 | 사기+정보 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 007 | 실패할 도전 | 이상한 방식 | 몰입 상황 | 교훈 전달 | 도전+극복 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 008 | 자연 속 상황 | 생존 시도 | 변수 등장 | 생존 기술 | 자연+실용 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 009 | 흔한 장소 | 이상한 디테일 | 공포 증가 | 붕괴 경고 | 위기+공포 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 010 | '진짜일까?' | 실험/분석 | 반전 | 허세 or 실속 | 비교+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n\n3. 대본 vs 비슷하거나 똑같은 기존 프롬프트 비교\n→ 유사 프롬프트: (여기에 2번에서 '✅ 유사'로 표시한 프롬프트 번호와 제목을 기재한다.)\n| 구분 | 🎬 대본 내용 | 📌 기존 프롬프트 (00X번) |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (유사 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (유사 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (유사 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (유사 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (유사 프롬프트의 전반적인 특징) |\n차이점 요약\n→ (여기에 대본과 유사 프롬프트의 핵심적인 차이점을 명확하게 요약하여 작성한다.)\n\n4. 대본 vs 새롭게 제안한 프롬프트 비교\n제안 프롬프트 제목: “(여기에 대본에 가장 잘 맞는 새로운 프롬프트 제목을 창의적으로 작성한다.)” 스토리 구조\n| 구분 | 🎬 대본 내용 | 💡 제안 프롬프트 |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (새 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (새 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (새 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (새 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (새 프롬프트의 전반적인 특징) |\n이 프롬프트의 강점\n→ (여기에 제안한 프롬프트가 왜 대본에 더 적합한지, 어떤 강점이 있는지 2~3가지 포인트로 설명한다.)\n\n5. 결론 요약\n| 항목 | 내용 |\n| :--- | :--- |\n| 기존 프롬프트 매칭 | (여기에 가장 유사한 프롬프트 번호와 함께, '정확히 일치하는 구조 없음' 등의 요약평을 작성한다.) |\n| 추가 프롬프트 필요성 | 필요함 — (여기에 왜 새로운 프롬프트가 필요한지 이유를 구체적으로 작성한다.) |\n| 새 프롬프트 제안 | (여기에 4번에서 제안한 프롬프트 제목과 핵심 특징을 요약하여 작성한다.) |\n| 활용 추천 분야 | (여기에 새 프롬프트가 어떤 종류의 콘텐츠에 활용될 수 있는지 구체적인 예시를 3~4가지 제시한다.) |\n\n6. 궁금증 유발 및 해소 과정 분석\n| 구분 | 내용 분석 (대본에서 어떻게 표현되었나?) | 핵심 장치 및 기법 |\n| :--- | :--- | :--- |\n| 🤔 궁금증 유발 (Hook) | (시작 부분에서 시청자가 \"왜?\", \"어떻게?\"라고 생각하게 만든 구체적인 장면이나 대사를 요약합니다.) | (예: 의문제시형 후킹, 어그로 끌기, 모순된 상황 제시, 충격적인 비주얼 등 사용된 기법을 명시합니다.) |\n| 🧐 궁금증 증폭 (Deepening) | (중간 부분에서 처음의 궁금증이 더 커지거나, 새로운 의문이 더해지는 과정을 요약합니다.) | (예: 예상 밖의 변수 등장, 상반된 정보 제공, 의도적인 단서 숨기기 등 사용된 기법을 명시합니다.) |\n| 💡 궁금증 해소 (Payoff) | (결말 부분에서 궁금증이 해결되는 순간, 즉 '아하!'하는 깨달음을 주는 장면이나 정보를 요약합니다.) | (예: 반전 공개, 실험/분석 결과 제시, 명쾌한 원리 설명 등 사용된 기법을 명시합니다.) |\n\n7. 대본에서 전달하려는 핵심 메시지가 뭐야?\n\n8. 이야기 창작에 활용할 수 있도록, 원본 대본의 **'핵심 설정값'**을 아래 템플릿에 맞춰 추출하고 정리해 줘.\n[이야기 설정값 추출 템플릿]\n바꿀 수 있는 요소 (살)\n주인공 (누가):\n공간적 배경 (어디서):\n문제 발생 원인 (왜):\n갈등 대상 (누구와):\n유지할 핵심 요소 (뼈대)\n문제 상황:\n해결책:\n\n9. 이미지랑 같은 표 형식으로 만들어줘\n\n10. 여러 대본 동시 분석 요청\n..."
    )


def _split_sentences(text: str) -> List[str]:
    if not text:
        return []
    normalized = text.replace('\r', '\n')
    while '\n\n' in normalized:
        normalized = normalized.replace('\n\n', '\n')
    normalized = normalized.replace('>>', ' ')
    lines = [l.strip() for l in normalized.split('\n') if l.strip() and not l.strip().isdigit()]
    joined = '\n'.join(lines)
    joined = joined.replace('\n', ' ')
    # naive split by punctuation
    out = []
    buff = ''
    for ch in joined:
        buff += ch
        if ch in '.?!…':
            if buff.strip():
                out.append(buff.strip())
            buff = ''
    if buff.strip():
        out.append(buff.strip())
    return out


def _safe_json_arr(text: str) -> List[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        m = None
        try:
            import re
            m = re.search(r"\[([\s\S]*?)\]", text)
        except Exception:
            m = None
        if m:
            try:
                return json.loads('[' + m.group(1) + ']')
            except Exception:
                return []
        return []


def _parse_keywords(text: str) -> Tuple[List[str], List[str], List[str]]:
    def sanitize(payload: str) -> str:
        payload = payload.strip()
        if payload.startswith('```'):
            payload = payload.strip('`').strip()
        return payload
    def norm(arr) -> List[str]:
        if isinstance(arr, list):
            items = arr
        elif isinstance(arr, str):
            items = [x.strip() for x in arr.split(',')]
        else:
            items = []
        seen = set()
        out = []
        for it in items:
            s = str(getattr(it, 'keyword', it)).strip().strip('#"\'')
            if not s:
                continue
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(s)
        return out[:20]
    try:
        payload = sanitize(text)
        data = json.loads(payload)
    except Exception:
        try:
            m = json.loads(text[text.index('{'): text.rindex('}') + 1])
            data = m
        except Exception:
            data = {}
    return (
        norm(data.get('ko')), norm(data.get('en')), norm(data.get('zh') or data.get('cn'))
    )


def _fetch_transcript(video_url: str, preferred_langs: List[str]) -> str:
    if YouTubeTranscriptApi is None:
        return ''
    vid = None
    try:
        if 'watch?v=' in video_url:
            vid = video_url.split('watch?v=')[1].split('&')[0]
        elif 'youtu.be/' in video_url:
            vid = video_url.split('youtu.be/')[1].split('?')[0]
        elif '/shorts/' in video_url:
            vid = video_url.split('/shorts/')[1].split('?')[0]
        elif 'youtube.com/embed/' in video_url:
            vid = video_url.split('/embed/')[1].split('?')[0]
    except Exception:
        vid = None
    if not vid:
        vid = video_url
    api = YouTubeTranscriptApi()
    # Try preferred languages first
    fetched = None
    for lang in preferred_langs:
        try:
            fetched = api.fetch(vid, languages=[lang])
            if fetched:
                break
        except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
            continue
        except Exception:
            continue
    if not fetched:
        try:
            fetched = api.fetch(vid)
        except Exception:
            fetched = []
    text = '\n'.join([snip.text for snip in (fetched or []) if getattr(snip, 'text', '')])
    return text


def _analyze_video(doc: Dict[str, Any]) -> Dict[str, Any]:
    youtube_url = doc.get('youtube_url')
    if not youtube_url:
        return {}
    transcript = _fetch_transcript(youtube_url, ['ko', 'en'])
    sentences = _split_sentences(transcript)
    # Material
    material_only = _call_gemini(_build_material_prompt(), transcript)
    # Analysis
    analysis_text = _call_gemini(_build_analysis_prompt(), transcript)
    # Categories
    categories_text = _call_gemini(_build_category_prompt(), transcript)
    # Keywords
    keywords_text = _call_gemini(_build_keywords_prompt(), f"제목:\n{doc.get('title','')}\n\n대본:\n{transcript}")
    # Dopamine (batch into chunks of 30)
    dopamine_graph: List[Dict[str, Any]] = []
    batch = 30
    for i in range(0, len(sentences), batch):
        sub = sentences[i:i+batch]
        text = _call_gemini(_build_dopamine_prompt(sub), '')
        arr = _safe_json_arr(text)
        for item in arr:
            s = str(item.get('sentence') or item.get('text') or '')
            try:
                level = int(round(float(item.get('level') or item.get('score') or 0)))
            except Exception:
                level = 1
            level = max(1, min(10, level))
            dopamine_graph.append({ 'sentence': s, 'level': level, 'reason': str(item.get('reason') or '') })
        # Soft rate limit
        time.sleep(0.2)

    # Post processing
    def _extract_line(regex: str, text: str) -> str:
        import re
        m = re.search(regex, text, re.I)
        if not m:
            return ''
        return (m.group(1) if m.groups() else m.group(0)).strip()

    updated = {}
    updated['analysis_full'] = analysis_text
    updated['dopamine_graph'] = dopamine_graph
    updated['analysis_transcript_len'] = len(transcript)
    updated['transcript_text'] = transcript

    # categories
    updated['kr_category_large'] = _extract_line(r"한국\s*대\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('kr_category_large')
    updated['kr_category_medium'] = _extract_line(r"한국\s*중\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('kr_category_medium')
    updated['kr_category_small'] = _extract_line(r"한국\s*소\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('kr_category_small')
    updated['en_category_main'] = _extract_line(r"EN\s*Main\s*Category\s*[:：]\s*(.+)", categories_text) or doc.get('en_category_main')
    updated['en_category_sub'] = _extract_line(r"EN\s*Sub\s*Category\s*[:：]\s*(.+)", categories_text) or doc.get('en_category_sub')
    updated['en_micro_topic'] = _extract_line(r"EN\s*Micro\s*Topic\s*[:：]\s*(.+)", categories_text) or doc.get('en_micro_topic')
    updated['cn_category_large'] = _extract_line(r"중국\s*대\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('cn_category_large')
    updated['cn_category_medium'] = _extract_line(r"중국\s*중\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('cn_category_medium')
    updated['cn_category_small'] = _extract_line(r"중국\s*소\s*카테고리\s*[:：]\s*(.+)", categories_text) or doc.get('cn_category_small')

    # material
    material_candidate = _extract_line(r"소재\s*[:：]\s*(.+)", material_only) or material_only.strip()
    if not material_candidate:
        # fallback with categories or first sentence
        material_candidate = (
            updated.get('kr_category_small') or
            updated.get('kr_category_medium') or
            updated.get('kr_category_large') or
            updated.get('en_micro_topic') or
            updated.get('en_category_main') or
            (sentences[0] if sentences else transcript[:60])
        )
    updated['material'] = material_candidate

    # keywords
    ko, en, zh = _parse_keywords(keywords_text)
    updated['keywords_ko'] = ko
    updated['keywords_en'] = en
    updated['keywords_zh'] = zh

    return updated


def _get_youtube_keys(db) -> List[str]:
    keys_raw = os.getenv('YOUTUBE_API_KEYS', '')
    keys = [k.strip() for k in keys_raw.split(',') if k.strip()]
    if keys:
        return keys
    # Firestore fallback: system/settings.youtube_api_keys (newline or comma separated)
    try:
        snap = db.collection('system').document('settings').get()
        if snap.exists:
            data = snap.to_dict() or {}
            raw = str(data.get('youtube_api_keys') or '')
            if raw:
                parts = [p.strip() for p in raw.replace('\r', '\n').replace(',', '\n').split('\n') if p.strip()]
                return parts
    except Exception:
        pass
    return []


def _update_views_for_videos(db, ids: List[str]) -> int:
    keys = _get_youtube_keys(db)
    if not keys:
        return 0
    import random
    key = random.choice(keys)
    base = 'https://www.googleapis.com/youtube/v3/videos'
    id_map = {}
    vids = []
    for vid in ids:
        snap = db.collection('videos').document(vid).get()
        if not snap.exists:
            continue
        data = snap.to_dict() or {}
        url = data.get('youtube_url') or ''
        video_id = None
        try:
            if 'watch?v=' in url:
                video_id = url.split('watch?v=')[1].split('&')[0]
            elif 'youtu.be/' in url:
                video_id = url.split('youtu.be/')[1].split('?')[0]
            elif '/shorts/' in url:
                video_id = url.split('/shorts/')[1].split('?')[0]
            elif 'youtube.com/embed/' in url:
                video_id = url.split('/embed/')[1].split('?')[0]
        except Exception:
            video_id = None
        if video_id:
            id_map[video_id] = vid
            vids.append(video_id)
    if not vids:
        return 0
    now_ms = int(time.time()*1000)
    updated = 0
    for i in range(0, len(vids), 50):
        chunk = vids[i:i+50]
        # rotate key per chunk
        key = random.choice(keys)
        params = { 'part': 'statistics', 'id': ','.join(chunk), 'key': key }
        res = requests.get(base, params=params, timeout=20)
        if res.status_code != 200:
            continue
        data = res.json()
        for item in data.get('items', []):
            video_id = item.get('id')
            mapped = id_map.get(video_id)
            stats = item.get('statistics', {})
            views = int(stats.get('viewCount') or 0)
            if mapped:
                ref = db.collection('videos').document(mapped)
                snap = ref.get()
                prev = 0
                basev = 0
                if snap.exists:
                    old = snap.to_dict() or {}
                    prev = int(old.get('views_numeric') or 0)
                    basev = int(old.get('views_baseline_numeric') or 0)
                patch = {
                    'views_prev_numeric': prev or basev or views,
                    'views_numeric': views,
                    'views_last_checked_at': now_ms
                }
                if not basev:
                    patch['views_baseline_numeric'] = prev or views
                ref.set(patch, merge=True)
                updated += 1
    return updated


def _process_job_batch(db, job: Dict[str, Any], batch_size: int = 3) -> Dict[str, Any]:
    scope = job.get('scope')
    remaining = list(job.get('remainingIds') or job.get('ids') or [])
    if scope == 'all' and not remaining:
        # snapshot all video ids
        vids = db.collection('videos').select(['title']).stream()
        remaining = [doc.id for doc in vids]
    ids_to_run = remaining[:batch_size]
    left = remaining[batch_size:]
    if job.get('type') == 'ranking':
        cnt = _update_views_for_videos(db, ids_to_run)
        if cnt == 0:
            db.collection('schedules').document(job['id']).set({ 'lastError': 'no_updates_or_missing_keys' }, merge=True)
    else:
        for vid in ids_to_run:
            try:
                snap = db.collection('videos').document(vid).get()
                if not snap.exists:
                    continue
                video = { 'id': snap.id, **snap.to_dict() }
                updated = _analyze_video(video)
                if updated:
                    db.collection('videos').document(vid).update(updated)
            except Exception as e:
                # mark error on job for visibility
                db.collection('schedules').document(job['id']).set({ 'lastError': str(e) }, merge=True)
    # update job progress
    patch = { 'updatedAt': int(time.time()*1000) }
    if left:
        patch.update({ 'status': 'running', 'remainingIds': left })
    else:
        patch.update({ 'status': 'done', 'remainingIds': [] })
    db.collection('schedules').document(job['id']).set(patch, merge=True)
    return patch


@app.route('/', methods=['GET'])
@app.route('/cron_analyze', methods=['GET'])
@app.route('/api/cron_analyze', methods=['GET'])
def cron_analyze():
    try:
        db = _load_db()
        now = int(time.time() * 1000)
        # Simple scan (Firestore free plan lacks complex queries across fields here)
        rows = db.collection('schedules').stream()
        due = []
        for d in rows:
            data = d.to_dict() or {}
            status = data.get('status')
            runAt = int(data.get('runAt') or 0)
            if status in ('pending', 'running') and runAt <= now:
                data['id'] = d.id
                due.append(data)
        if not due:
            return jsonify({ 'ok': True, 'processed': 0 })

        processed = 0
        for job in due:
            # take lease: set running
            db.collection('schedules').document(job['id']).set({ 'status': 'running', 'updatedAt': now }, merge=True)
            _process_job_batch(db, job, batch_size=3)
            processed += 1
        return jsonify({ 'ok': True, 'processed': processed })
    except Exception as e:
        return jsonify({ 'ok': False, 'error': str(e) }), 500


@app.route('/health', methods=['GET'])
def health():
    return ('ok', 200)


