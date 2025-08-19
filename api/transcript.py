import json
import re
from typing import Dict, Any, List
from flask import Flask, request, jsonify

try:
    from yt_dlp import YoutubeDL
except Exception as e:
    YoutubeDL = None  # vercel 빌드 실패 시 런타임 에러 메시지 제공

try:
    import requests
except Exception:
    requests = None

try:
    # use public exports; avoid importing private module `_errors`
    from youtube_transcript_api import (
        YouTubeTranscriptApi,
        TranscriptsDisabled,
        NoTranscriptFound,
        VideoUnavailable,
    )
except Exception:
    YouTubeTranscriptApi = None
    class _TranscriptApiImportFallback(Exception):
        pass
    TranscriptsDisabled = NoTranscriptFound = VideoUnavailable = _TranscriptApiImportFallback

app = Flask(__name__)

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'ko,en;q=0.9',
    'Connection': 'keep-alive',
}

def _pick_audio_url(info: Dict[str, Any]) -> str:
    try:
        formats = info.get('formats') or []
        # best audio-only format
        audio_only = [f for f in formats if f.get('vcodec') in (None, 'none') and f.get('acodec') not in (None, 'none') and f.get('url')]
        # prefer m4a/webm opus by abr
        audio_only.sort(key=lambda f: (f.get('abr') or 0), reverse=True)
        return audio_only[0]['url'] if audio_only else ''
    except Exception:
        return ''

def _stt_with_deepgram(audio_url: str, preferred_langs: List[str]) -> Dict[str, Any]:
    import os
    api_key = os.getenv('DEEPGRAM_API_KEY')
    if not api_key or not audio_url:
        return {}
    # choose language
    lang = 'en'
    for p in preferred_langs:
        if p.startswith('ko'):
            lang = 'ko'
            break
        if p.startswith('en'):
            lang = 'en'
            break
    payload = { 'url': audio_url }
    resp = requests.post(
        f'https://api.deepgram.com/v1/listen?language={lang}&smart_format=true',
        headers={'Authorization': f'Token {api_key}', 'Content-Type': 'application/json'},
        data=json.dumps(payload), timeout=60
    )
    if resp.status_code != 200:
        return {}
    data = resp.json()
    # extract transcript text
    try:
        channels = data.get('results', {}).get('channels', [])
        alts = channels[0].get('alternatives', []) if channels else []
        transcript = alts[0].get('transcript', '') if alts else ''
        return { 'text': transcript, 'lang': lang, 'ext': 'stt' }
    except Exception:
        return {}

def _best_caption_track(info: Dict[str, Any]) -> Dict[str, Any]:
    subtitles = info.get('subtitles') or {}
    auto = info.get('automatic_captions') or {}
    # 병합
    merged: Dict[str, List[Dict[str, Any]]] = {}
    for source in (subtitles, auto):
        for lang, items in source.items():
            merged.setdefault(lang, []).extend(items or [])

    # 선호 언어/확장자 우선순위
    preferred_langs = ['ko', 'ko-KR', 'ko-kr', 'en', 'en-US', 'en-us']
    preferred_exts = ['vtt', 'srt']

    # 1) 선호 언어 + 선호 확장자
    for lang in preferred_langs:
        for ext in preferred_exts:
            for cand in merged.get(lang, []):
                if cand.get('ext') == ext and cand.get('url'):
                    return cand

    # 2) 아무 언어라도 선호 확장자
    for lang, items in merged.items():
        for ext in preferred_exts:
            for cand in items:
                if cand.get('ext') == ext and cand.get('url'):
                    return cand

    # 3) 아무거나 첫 번째
    for items in merged.values():
        for cand in items:
            if cand.get('url'):
                return cand
    return {}


def _strip_vtt(vtt: str) -> str:
    text = re.sub(r'^WEBVTT[\s\S]*?\n\n', '', vtt, flags=re.MULTILINE)
    text = re.sub(r"\d{2}:\d{2}:\d{2}\.\d{3} --> [^\n]+\n", '', text)
    text = re.sub(r"<[^>]+>", '', text)
    text = re.sub(r"\n{2,}", '\n', text)
    return text.strip()


def _strip_srt(srt: str) -> str:
    # 인덱스 줄 제거
    text = re.sub(r"^\d+\s*$", '', srt, flags=re.MULTILINE)
    # 타임코드 제거
    text = re.sub(r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}\s*\n", '', text)
    text = re.sub(r"<[^>]+>", '', text)
    text = re.sub(r"\n{2,}", '\n', text)
    return text.strip()


def _to_plain_text(body: str, ext: str) -> str:
    ext = (ext or '').lower()
    if ext == 'vtt':
        return _strip_vtt(body)
    if ext == 'srt':
        return _strip_srt(body)
    # fallback: 그냥 본문 반환
    return body


@app.get('/')
def transcript_root():
    try:
        # requests는 반드시 필요, yt-dlp는 폴백이므로 없어도 됨
        if requests is None:
            return jsonify({ 'error': 'dependencies not available' }), 500

        url = request.args.get('url')
        lang_pref_raw = (request.args.get('lang') or '').strip().lower()
        preferred_langs = [s.strip() for s in (lang_pref_raw or 'ko,en').split(',') if s.strip()]
        if not url:
            return jsonify({ 'error': 'url query required' }), 400

        # 1) youtube-transcript-api 우선 시도
        vid = None
        try:
            if 'watch?v=' in url:
                vid = url.split('watch?v=')[1].split('&')[0]
            elif 'youtu.be/' in url:
                vid = url.split('youtu.be/')[1].split('?')[0]
            elif '/shorts/' in url:
                vid = url.split('/shorts/')[1].split('?')[0]
        except Exception:
            vid = None

        if YouTubeTranscriptApi and vid:
            try:
                ytt = YouTubeTranscriptApi()
                fetched = None
                for lang in preferred_langs:
                    try:
                        fetched = ytt.fetch(vid, languages=[lang])
                        if fetched:
                            break
                    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
                        continue
                if not fetched:
                    fetched = ytt.fetch(vid)
                text = '\n'.join([snip.text for snip in fetched if getattr(snip, 'text', '')])
                if text.strip():
                    return jsonify({ 'text': text, 'lang': getattr(fetched, 'language_code', None), 'ext': 'json' }), 200
            except Exception:
                pass

        # 2) yt-dlp 폴백
        if YoutubeDL is None:
            return jsonify({ 'error': 'caption not found' }), 404
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'nocheckcertificate': True,
            'noprogress': True,
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        subtitles = info.get('subtitles') or {}
        auto = info.get('automatic_captions') or {}
        collected = []
        for source in (subtitles, auto):
            for lang, items in source.items():
                for it in items or []:
                    collected.append({ 'lang': str(lang).lower(), 'ext': it.get('ext'), 'url': it.get('url') })

        preferred_exts = ['vtt', 'srt']
        cand = None
        for p in preferred_langs:
            cand = next((t for t in collected if (t['ext'] in preferred_exts) and (t['lang'] == p or t['lang'].startswith(p + '-'))), None)
            if cand:
                break
        if cand is None:
            cand = next((t for t in collected if t['ext'] in preferred_exts), None)
        if cand is None and collected:
            cand = collected[0]
        if not cand or not cand.get('url'):
            # 3) STT 폴백 (Deepgram) — 자막이 전혀 없을 때
            audio_url = _pick_audio_url(info)
            stt = _stt_with_deepgram(audio_url, preferred_langs)
            if stt.get('text'):
                return jsonify(stt), 200
            return jsonify({ 'error': 'caption not found' }), 404

        r = requests.get(cand['url'], headers=DEFAULT_HEADERS, timeout=15)
        if r.status_code != 200:
            # 3) STT 폴백 (Deepgram) — 자막 파일 접근 실패
            audio_url = _pick_audio_url(info)
            stt = _stt_with_deepgram(audio_url, preferred_langs)
            if stt.get('text'):
                return jsonify(stt), 200
            return jsonify({ 'error': 'caption fetch failed', 'status': r.status_code }), 502
        # Google 차단 페이지 대응
        ctype = (r.headers.get('content-type') or '').lower()
        if 'text/html' in ctype and ('sorry' in r.text.lower() or '<html' in r.text.lower()):
            # 3) STT 폴백 (Deepgram) — 차단 시 음성 직접 STT 시도
            audio_url = _pick_audio_url(info)
            stt = _stt_with_deepgram(audio_url, preferred_langs)
            if stt.get('text'):
                return jsonify(stt), 200
            return jsonify({ 'error': 'blocked_by_provider' }), 429

        text = _to_plain_text(r.text, cand.get('ext'))
        return jsonify({ 'text': text, 'lang': cand.get('lang'), 'ext': cand.get('ext') }), 200

    except Exception as e:
        return jsonify({ 'error': str(e) }), 500


