function getUserGeminiKey() {
    try { return localStorage.getItem('my_gemini_key') || ''; } catch { return ''; }
}
function setUserGeminiKey(v) {
    try { localStorage.setItem('my_gemini_key', v || ''); } catch {}
}

function buildPromptWithChecklist(basePrompt, checklist) {
    const lines = (checklist || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    if (!lines.length) return basePrompt;
    const addon = '\n\n[사용자 체크리스트]\n' + lines.map((l, i) => `${i+1}. ${l}`).join('\n') + '\n\n위 체크리스트 항목을 반드시 분석에 반영하세요.';
    return basePrompt + addon;
}

async function callUserGemini(systemPrompt, userContent) {
    const key = (document.getElementById('my-gemini-key')?.value || getUserGeminiKey()).trim();
    if (!key) throw new Error('개인 Gemini 키를 입력하세요.');
    setUserGeminiKey(key);
    const model = 'models/gemini-1.5-pro-latest';
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/${model}:generateContent?key=${encodeURIComponent(key)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents: [{ role: 'user', parts: [{ text: systemPrompt + "\n\n" + userContent }] }], generationConfig: { temperature: 0.3 } })
    });
    if (!res.ok) throw new Error('Gemini 호출 실패: ' + res.status);
    const data = await res.json();
    return data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
}

function parseKeywordsText(text) {
    function sanitizeJson(input) { return String(input).replace(/^```json\s*/i,'').replace(/^```\s*/i,'').replace(/```\s*$/i,'').trim(); }
    function normalizeArray(value) {
        const arr = Array.isArray(value) ? value : (typeof value === 'string' ? value.split(/[\n,，]/) : []);
        const seen = new Set(); const out = [];
        for (const it of arr) {
            const v = (typeof it === 'string' ? it : (it && (it.keyword || it.text || it.name))) || '';
            const s = String(v).replace(/["'#]/g, '').trim();
            if (!s) continue; const key = s.toLowerCase(); if (seen.has(key)) continue; seen.add(key); out.push(s);
        }
        return out.slice(0, 20);
    }
    try {
        let payload = sanitizeJson(text); let obj = null;
        try { obj = JSON.parse(payload); } catch { const m = payload.match(/\{[\s\S]*\}/); if (m) { try { obj = JSON.parse(m[0]); } catch {} } }
        return { ko: normalizeArray(obj?.ko), en: normalizeArray(obj?.en), zh: normalizeArray(obj?.zh || obj?.cn) };
    } catch { return { ko: [], en: [], zh: [] }; }
}

function splitTranscriptIntoSentences(text) {
    if (!text) return [];
    let normalized = String(text).replace(/\r/g, '\n').replace(/\n{2,}/g, '\n').trim();
    normalized = normalized.replace(/>{2,}/g, ' ').replace(/\s{2,}/g, ' ').trim();
    const lines = normalized.split('\n').map(l => l.trim()).filter(l => l && !/^\d+(\.\d+)?$/.test(l));
    const joined = lines.join('\n').replace(/([\.\?\!…])\s*\n+/g,'$1__SENT__').replace(/\n+/g,' ').replace(/__SENT__/g,' ').replace(/\s{2,}/g,' ').trim();
    return joined.split(/(?<=[\.\?\!…])\s+/).map(s => s.trim()).filter(Boolean);
}

function escapeHtml(str) { return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function renderKeywordCard(label, keywords) {
    const safe = Array.isArray(keywords) ? keywords : [];
    const chips = safe.map(k => `<span class="keyword-chip" data-keyword="${escapeHtml(String(k))}">${escapeHtml(String(k))}</span>`).join('');
    return `<div class="keyword-card"><div class="keyword-card-header">${escapeHtml(label)}</div><div class="keyword-card-body">${chips || '<span class="keyword-chip empty">키워드 없음</span>'}</div></div>`;
}

function prettyFormatMarkdownTables(text) {
    const lines = String(text).split('\n'); const out = []; let i=0;
    while (i<lines.length) { const line = lines[i]; if (/^\s*\|/.test(line)) { const tbl = []; while (i<lines.length && /^\s*\|/.test(lines[i])) { tbl.push(lines[i]); i++; } const rows = tbl.map(l=>l.replace(/^\s*\|/,'').replace(/\|\s*$/,'')).map(l=>l.split('|').map(c=>c.trim())); const cleaned = rows.filter(r=>!r.every(c=>/^:?-{3,}:?$/.test(c))); if (rows.length>=2 && rows[1].every(c=>/^:?-{3,}:?$/.test(c))) cleaned.shift(); cleaned.forEach(r=>{ if (r.length===2) out.push(`- ${r[0]}: ${r[1]}`); else out.push(`- ${r.filter(Boolean).join(' · ')}`); }); } else { out.push(line); i++; } }
    return out.join('\n').trim();
}

function renderAnalysisCards(analysisText) {
    const secs = splitAnalysisSections(analysisText); if (!secs.length) return '';
    return secs.filter(sec => sec.idx !== 0 && sec.idx !== 2).map(sec => {
        const title = `${sec.idx}. ${sec.title || ''}`.trim();
        const body = prettyFormatMarkdownTables(sec.lines.join('\n').trim());
        return `<div class="analysis-card"><div class="analysis-card-header">${escapeHtml(title)}</div><div class="analysis-card-body"><pre class="analysis-pre">${escapeHtml(body)}</pre></div></div>`;
    }).join('');
}

function splitAnalysisSections(text) {
    const sections = []; const lines = String(text).split('\n'); let current = null;
    for (const raw of lines) { const m = raw.match(/^\s*([0-8])\.(.*)$/); if (m) { if (current) sections.push(current); current = { idx: Number(m[1]), title: m[2].trim(), lines: [] }; } else if (current) { current.lines.push(raw); } }
    if (current) sections.push(current); return sections;
}

function drawDopamineChart(canvas, data) {
    if (!canvas || !canvas.getContext || !Array.isArray(data) || data.length === 0) return;
    const parent = canvas.parentElement; const width = Math.max(320, parent?.clientWidth || 640); canvas.width = width;
    const ctx = canvas.getContext('2d'); ctx.clearRect(0,0,canvas.width,canvas.height);
    const paddingLeft=40, paddingRight=16, paddingTop=16, paddingBottom=28; const chartW = canvas.width - paddingLeft - paddingRight; const chartH = canvas.height - paddingTop - paddingBottom;
    const yMin=0, yMax=10; const n=data.length; const xPos=i=> (n===1? paddingLeft+chartW/2 : paddingLeft + (i/(n-1))*chartW); const yPos=v=>{ const vv = Math.max(yMin, Math.min(yMax, Number(v)||0)); const ratio=(vv-yMin)/(yMax-yMin); return paddingTop + (1-ratio)*chartH; };
    ctx.strokeStyle = '#e5e7eb'; ctx.lineWidth=1; ctx.beginPath(); [0,3,6,9,10].forEach(val=>{ const y=yPos(val); ctx.moveTo(paddingLeft,y); ctx.lineTo(canvas.width - paddingRight, y); }); ctx.stroke();
    ctx.fillStyle = '#6b7280'; ctx.font='12px system-ui, -apple-system, Segoe UI, Roboto, Noto Sans KR, Arial'; [0,3,6,9,10].forEach(val=>{ const y=yPos(val); ctx.fillText(String(val), 8, y+4); });
    ctx.strokeStyle='#2563eb'; ctx.lineWidth=2; ctx.beginPath(); data.forEach((it,i)=>{ const x=xPos(i); const y=yPos(it.level ?? it.score ?? 0); if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke();
    data.forEach((it,i)=>{ const level=Number(it.level ?? it.score ?? 0); const x=xPos(i); const y=yPos(level); let color='#94a3b8'; if (level>=4&&level<=6) color='#f59e0b'; if (level>=7&&level<=9) color='#ef4444'; if (level>=10) color='#b91c1c'; ctx.fillStyle=color; ctx.beginPath(); ctx.arc(x,y,3,0,Math.PI*2); ctx.fill(); });
}

async function run() {
    const statusEl = document.getElementById('my-status');
    const title = (document.getElementById('my-title')?.value || '').trim();
    const transcript = (document.getElementById('my-transcript')?.value || '').trim();
    const checklist = (document.getElementById('my-checklist')?.value || '').trim();
    if (!transcript) { statusEl.textContent = '대본을 입력하세요.'; return; }
    statusEl.textContent = '분석 시작...';

    // prompts
    const baseCategory = '다음 대본을 기반으로 카테고리를 아래 형식으로만 한 줄씩 정확히 출력하세요. 다른 텍스트/머리말/설명 금지.\n한국 대 카테고리: \n한국 중 카테고리: \n한국 소 카테고리: \nEN Main Category: \nEN Sub Category: \nEN Micro Topic: \n중국 대 카테고리: \n중국 중 카테고리: \n중국 소 카테고리: ';
    const baseKeywords = '아래 제공된 "제목"과 "대본"을 모두 참고하여, 원본 영상을 검색해 찾기 쉬운 핵심 검색 키워드를 한국어/영어/중국어로 각각 8~15개씩 추출하세요.\n출력 형식은 JSON 객체만, 다른 설명/머리말/코드펜스 금지.\n요구 형식: {"ko":["키워드1","키워드2",...],"en":["keyword1",...],"zh":["关键词1",...]}\n규칙:\n- 각 키워드는 1~4단어의 짧은 구로 작성\n- 해시태그/특수문자/따옴표 제거, 불용어 제외\n- 동일 의미/중복 표현은 하나만 유지\n- 인명/채널명/브랜드/핵심 주제 포함\n';
    const baseMaterial = '다음 대본의 핵심 소재를 한 문장으로 요약하세요. 반드시 한 줄로만, "소재: "로 시작하여 출력하세요. 다른 설명이나 불필요한 문자는 금지합니다.';
    const baseDopa = '다음 "문장 배열"에 대해, 각 문장별로 궁금증/도파민 유발 정도를 1~10 정수로 평가하고, 그 이유를 간단히 설명하세요. 반드시 JSON 배열로만, 요소는 {"sentence":"문장","level":정수,"reason":"이유"} 형태로 출력하세요. 여는 대괄호부터 닫는 대괄호까지 외 텍스트는 출력하지 마세요.';
    const baseAnalysis = `...`;

    try {
        // material
        const materialOnly = await callUserGemini(buildPromptWithChecklist(baseMaterial, checklist), transcript);
        statusEl.textContent = '카테고리 분석 중...';
        const categoriesText = await callUserGemini(buildPromptWithChecklist(baseCategory, checklist), transcript);
        statusEl.textContent = '키워드 분석 중...';
        const keywordsText = await callUserGemini(buildPromptWithChecklist(baseKeywords, checklist), `제목:\n${title}\n\n대본:\n${transcript}`);
        statusEl.textContent = '도파민 분석 중...';
        const sentences = splitTranscriptIntoSentences(transcript);
        const dopaText = await callUserGemini(buildPromptWithChecklist(baseDopa, checklist), '문장 배열:\n' + JSON.stringify(sentences));
        let dopa = []; try { dopa = JSON.parse(dopaText); } catch { const m=dopaText.match(/\[([\s\S]*?)\]/); if (m) { try { dopa = JSON.parse('['+m[1]+']'); } catch {} } }
        statusEl.textContent = '템플릿 분석 중...';
        const analysisText = await callUserGemini(buildPromptWithChecklist(baseAnalysis, checklist), transcript);

        // extract
        const detailsGrid = document.getElementById('my-details-grid');
        const kws = parseKeywordsText(keywordsText);
        function extractLine(regex, text){ const m=String(text).match(regex); return m? (m[1]||m[0]).trim():''; }
        const kr = [extractLine(/한국\s*대\s*카테고리\s*[:：]\s*(.+)/i,categoriesText), extractLine(/한국\s*중\s*카테고리\s*[:：]\s*(.+)/i,categoriesText), extractLine(/한국\s*소\s*카테고리\s*[:：]\s*(.+)/i,categoriesText)].filter(Boolean).join(' > ');
        const en = [extractLine(/EN\s*Main\s*Category\s*[:：]\s*(.+)/i,categoriesText), extractLine(/EN\s*Sub\s*Category\s*[:：]\s*(.+)/i,categoriesText), extractLine(/EN\s*Micro\s*Topic\s*[:：]\s*(.+)/i,categoriesText)].filter(Boolean).join(' > ');
        const cn = [extractLine(/중국\s*대\s*카테고리\s*[:：]\s*(.+)/i,categoriesText), extractLine(/중국\s*중\s*카테고리\s*[:：]\s*(.+)/i,categoriesText), extractLine(/중국\s*소\s*카테고리\s*[:：]\s*(.+)/i,categoriesText)].filter(Boolean).join(' > ');
        detailsGrid.innerHTML = `
            <div class="detail-item"><span class="detail-label">소재</span><span class="detail-value">${escapeHtml((materialOnly.match(/소재\s*[:：]\s*(.+)/i)?.[1]||materialOnly||'').trim())}</span></div>
            <div class="detail-item"><span class="detail-label">한국 카테고리</span><span class="detail-value">${escapeHtml(kr)}</span></div>
            <div class="detail-item"><span class="detail-label">영문 카테고리</span><span class="detail-value">${escapeHtml(en)}</span></div>
            <div class="detail-item"><span class="detail-label">중국 카테고리</span><span class="detail-value">${escapeHtml(cn)}</span></div>
        `;
        document.getElementById('my-keywords').innerHTML = renderKeywordCard('한국어', kws.ko) + renderKeywordCard('English', kws.en) + renderKeywordCard('中文', kws.zh);
        document.getElementById('my-analysis-cards').innerHTML = renderAnalysisCards(analysisText);
        const graph = document.getElementById('my-dopa-graph'); graph.innerHTML='';
        const canvas = document.createElement('canvas'); canvas.height=220; graph.appendChild(canvas); drawDopamineChart(canvas, Array.isArray(dopa)? dopa.map(it=>({ sentence: it.sentence||it.text||'', level: Number(it.level||it.score||0) })) : []);
        statusEl.textContent = '완료';
    } catch (e) {
        document.getElementById('my-status').textContent = '오류: ' + (e.message || e);
    }
}

document.getElementById('my-run-btn')?.addEventListener('click', run);
document.getElementById('my-gemini-key')?.addEventListener('change', (e)=> setUserGeminiKey(e.target.value));


