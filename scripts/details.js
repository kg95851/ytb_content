import { supabase } from '../supabase-client.js';

const detailsContent = document.getElementById('details-content');
const dopamineGraphContainer = document.getElementById('dopamine-graph');
const topCommentsContainer = document.getElementById('top-comments');

// URL에서 비디오 ID 가져오기
const getVideoIdFromUrl = () => {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('id');
};

// 비디오 상세 정보 가져오기 및 표시
const fetchAndDisplayDetails = async () => {
    const videoId = getVideoIdFromUrl();

    if (!videoId) {
        detailsContent.innerHTML = '<p class="error-message">잘못된 접근입니다. 비디오 ID가 필요합니다.</p>';
        return;
    }

    try {
        const { data, error } = await supabase
          .from('videos')
          .select('*')
          .eq('id', videoId)
          .single();
        if (error) throw error;
        if (!data) {
            detailsContent.innerHTML = '<p class="error-message">해당 비디오를 찾을 수 없습니다.</p>';
            return;
        }
        renderDetails(data);
    } catch (error) {
        console.error("Error fetching video details: ", error);
        detailsContent.innerHTML = '<p class="error-message">데이터를 불러오는 데 실패했습니다.</p>';
    }
};

// 상세 정보 렌더링 함수
const renderDetails = (video) => {
    // 페이지 제목 설정
    document.title = `${video.title} - 콘텐츠 상세 정보`;

    // 카테고리 조합
    const kr_categories = [video.kr_category_large, video.kr_category_medium, video.kr_category_small].filter(Boolean).join(' > ');
    const en_categories = [video.en_category_main, video.en_category_sub, video.en_micro_topic].filter(Boolean).join(' > ');
    const cn_categories = [video.cn_category_large, video.cn_category_medium, video.cn_category_small].filter(Boolean).join(' > ');

    // YouTube 임베드 플레이어 생성 시도
    let videoPlayerHTML = '';
    try {
        const url = new URL(video.youtube_url);
        let ytVideoId = url.searchParams.get('v');
        // 짧은 URL (youtu.be) 또는 Shorts URL 처리
        if (!ytVideoId && (url.hostname.includes('youtu.be') || url.hostname.includes('youtube.com/shorts/'))) {
            ytVideoId = url.pathname.split('/').pop();
        }
        
        if (ytVideoId) {
            videoPlayerHTML = `<iframe src="https://www.youtube.com/embed/${ytVideoId}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>`;
        }
    } catch (e) {
        console.error("Could not parse YouTube URL for embedding:", e);
    }

    // 임베드 실패 시 썸네일 표시 및 링크 제공
    if (!videoPlayerHTML) {
        const thumbnail = video.thumbnail
            ? `<img src="${video.thumbnail}" alt="${video.title}">`
            : `<div class="no-thumbnail-placeholder" style="height: 100%;">영상 또는 이미지 없음</div>`;
        videoPlayerHTML = `<a href="${video.youtube_url}" target="_blank">${thumbnail}</a>`;
    }

    const kwKO = Array.isArray(video.keywords_ko) ? video.keywords_ko : [];
    const kwEN = Array.isArray(video.keywords_en) ? video.keywords_en : [];
    const kwZH = Array.isArray(video.keywords_zh) ? video.keywords_zh : [];

    detailsContent.innerHTML = `
        <div class="details-container">
            <div class="video-player-container">
                ${videoPlayerHTML}
            </div>
            <div class="details-info">
                <h1>${video.title || '제목 없음'}</h1>
                <div class="details-meta-bar">
                    <div class="meta-item"><strong>채널</strong> <span>${video.channel || '없음'}</span></div>
                    <div class="meta-item"><strong>게시일</strong> <span>${video.date || '없음'}</span></div>
                    <div class="meta-item"><strong>조회수</strong> <span>${(video.views_numeric || 0).toLocaleString()}회</span></div>
                    <div class="meta-item"><strong>구독자</strong> <span>${(video.subscribers_numeric || 0).toLocaleString()}명</span></div>
                    <div class="meta-item"><strong>폼 유형</strong> <span class="group-tag">${video.group_name || '없음'}</span></div>
                </div>
                
                <h2>상세 분석 정보</h2>
                <div class="details-grid">
                    ${renderDetailItem('소재', video.material)}
                    ${renderDetailItem('템플릿 유형', video.template_type)}
                    ${renderDetailItem('후킹 요소', video.hooking)}
                    ${renderDetailItem('기승전결 구조', video.narrative_structure)}
                    ${renderDetailItem('한국 카테고리', kr_categories)}
                    ${renderDetailItem('영문 카테고리', en_categories)}
                    ${renderDetailItem('중국 카테고리', cn_categories)}
                </div>
                ${(kwKO.length || kwEN.length || kwZH.length) ? `
                <h2 style="margin-top:1.5rem;">검색 키워드</h2>
                <div class="keyword-cards-grid">
                    ${renderKeywordCard('한국어', kwKO)}
                    ${renderKeywordCard('English', kwEN)}
                    ${renderKeywordCard('中文', kwZH)}
                </div>` : ''}
                ${video.analysis_full ? `<h2 style="margin-top:1.5rem;">분석 카드</h2><div class="analysis-cards-grid">${renderAnalysisCards(filterAnalysisText(video.analysis_full))}</div>` : ''}
            </div>
        </div>
    `;

    // 도파민 그래프 표시
    if (Array.isArray(video.dopamine_graph) && video.dopamine_graph.length && dopamineGraphContainer) {
        dopamineGraphContainer.innerHTML = '';
        const legend = document.createElement('div');
        legend.className = 'dopamine-legend';
        legend.innerHTML = `
            <span><i class="dopamine-dot dot-low"></i> 1-3 낮음</span>
            <span><i class="dopamine-dot dot-mid"></i> 4-6 중간</span>
            <span><i class="dopamine-dot dot-high"></i> 7-9 높음</span>
        `;
        dopamineGraphContainer.appendChild(legend);
        const header = document.createElement('div');
        header.innerHTML = `<div style="font-weight:600;">문장</div><div style="font-weight:600;">레벨</div><div style="font-weight:600;">시각화</div>`;
        header.style.display = 'grid';
        header.style.gridTemplateColumns = '1fr auto auto';
        header.style.marginBottom = '8px';
        dopamineGraphContainer.appendChild(header);
        video.dopamine_graph.forEach(item => {
            const sentence = document.createElement('div');
            sentence.textContent = item.sentence || item.text || '';
            const level = Number(item.level ?? item.score ?? 0);
            const levelDiv = document.createElement('div');
            levelDiv.textContent = String(level);
            const bar = document.createElement('div');
            bar.style.height = '10px';
            bar.style.width = Math.max(5, Math.min(100, Math.round(level * 10))) + 'px';
            // 구간 색상: 1-3 낮음(회색), 4-6 중간(주황), 7-9 높음(빨강), 10은 더 진한 빨강
            let color = '#94a3b8';
            if (level >= 4 && level <= 6) color = '#f59e0b';
            if (level >= 7 && level <= 9) color = '#ef4444';
            if (level >= 10) color = '#b91c1c';
            bar.style.background = color;
            bar.style.borderRadius = '4px';
            const row = document.createElement('div');
            row.style.display = 'grid';
            row.style.gridTemplateColumns = '1fr auto auto';
            row.style.alignItems = 'center';
            row.style.gap = '12px';
            row.appendChild(sentence);
            row.appendChild(levelDiv);
            row.appendChild(bar);
            dopamineGraphContainer.appendChild(row);
        });

        // 차트 컨테이너 및 캔버스 추가
        const chartWrap = document.createElement('div');
        chartWrap.id = 'dopamine-chart-container';
        chartWrap.style.marginTop = '16px';
        const canvas = document.createElement('canvas');
        canvas.id = 'dopamine-chart';
        canvas.height = 220;
        chartWrap.appendChild(canvas);
        dopamineGraphContainer.appendChild(chartWrap);
        drawDopamineChart(canvas, video.dopamine_graph);
        // 리사이즈 대응(간단)
        window.addEventListener('resize', () => drawDopamineChart(canvas, video.dopamine_graph));
    }

    // 상위 인기 댓글 표시
    if (topCommentsContainer) {
        const list = Array.isArray(video.comments_top) ? video.comments_top : [];
        if (!list.length) {
            topCommentsContainer.innerHTML = '<p class="info-message">수집된 인기 댓글이 없습니다. 관리자에서 댓글분석을 실행하세요.</p>';
        } else {
            const html = list.map((c) => {
                const author = escapeHtml(c.author || '');
                const text = escapeHtml((c.text || '').replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, ''));
                const likes = Number(c.likeCount || 0).toLocaleString();
                const when = c.publishedAt ? new Date(c.publishedAt).toLocaleString() : '';
                const authorImg = c.authorProfileImageUrl ? `<img src="${c.authorProfileImageUrl}" alt="${author}" style="width:24px;height:24px;border-radius:50%;object-fit:cover;">` : '';
                const authorLinkOpen = c.authorChannelUrl ? `<a href="${c.authorChannelUrl}" target="_blank" rel="noopener noreferrer">` : '';
                const authorLinkClose = c.authorChannelUrl ? `</a>` : '';
                return `
                <div class="detail-item">
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">${authorImg}${authorLinkOpen}<strong>${author}</strong>${authorLinkClose}<span style="color:#6b7280; font-size:12px;">${when}</span></div>
                    <div style="white-space:pre-wrap;">${text}</div>
                    <div style="margin-top:6px;color:#6b7280; font-size:12px;">좋아요 ${likes}</div>
                </div>`;
            }).join('');
            topCommentsContainer.innerHTML = `<div class="details-grid">${html}</div>`;
        }
    }
};

// 상세 항목 렌더링 헬퍼
const renderDetailItem = (label, value, fullWidth = false) => {
    const wrapperClass = fullWidth ? 'detail-item analysis-full' : 'detail-item';
    return `
        <div class="${wrapperClass}">
            <span class="detail-label">${label}</span>
            <span class="detail-value">${value || '없음'}</span>
        </div>
    `;
};

function escapeHtml(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function filterAnalysisText(text) {
    try {
        const lines = String(text).split('\n');
        const out = [];
        for (const raw of lines) {
            const line = raw.trimEnd();
            // 9, 10 섹션 이후는 제외
            if (/^\s*9\./.test(line) || /^\s*10\./.test(line)) break;
            // 프롬프트/지시문 성격의 라인은 제거
            if (/^\s*\[gpts/i.test(line)) continue;
            if (/페르소나|핵심 임무|절대 규칙|출력 템플릿/.test(line)) continue;
            out.push(line);
        }
        return out.join('\n').trim();
    } catch {
        return text || '';
    }
}

function splitAnalysisSections(text) {
    const sections = [];
    const lines = String(text).split('\n');
    let current = null;
    for (const raw of lines) {
        const m = raw.match(/^\s*([0-8])\.(.*)$/);
        if (m) {
            if (current) sections.push(current);
            current = { idx: Number(m[1]), title: m[2].trim(), lines: [] };
        } else if (current) {
            current.lines.push(raw);
        }
    }
    if (current) sections.push(current);
    return sections;
}

function renderAnalysisCards(analysisText) {
    const secs = splitAnalysisSections(analysisText);
    if (!secs.length) return '';
    return secs
        .filter(sec => sec.idx !== 0 && sec.idx !== 2) // 0, 2 섹션 숨김
        .map(sec => {
            const title = `${sec.idx}. ${sec.title || ''}`.trim();
            const body = prettyFormatMarkdownTables(sec.lines.join('\n').trim());
            return `
        <div class="analysis-card">
            <div class="analysis-card-header">${escapeHtml(title)}</div>
            <div class="analysis-card-body">
                <pre class="analysis-pre">${escapeHtml(body)}</pre>
            </div>
        </div>`;
        }).join('');
}

function prettyFormatMarkdownTables(text) {
    const lines = String(text).split('\n');
    const out = [];
    let i = 0;
    while (i < lines.length) {
        const line = lines[i];
        if (/^\s*\|/.test(line)) {
            // collect table block
            const tbl = [];
            while (i < lines.length && /^\s*\|/.test(lines[i])) {
                tbl.push(lines[i]);
                i++;
            }
            // parse rows
            const rows = tbl
                .map(l => l.replace(/^\s*\|/, '').replace(/\|\s*$/, ''))
                .map(l => l.split('|').map(c => c.trim()));
            // drop header separator rows (---)
            const cleaned = rows.filter(r => !r.every(c => /^:?-{3,}:?$/.test(c)));
            // if header exists, drop first row if next was separator
            if (rows.length >= 2 && rows[1].every(c => /^:?-{3,}:?$/.test(c))) cleaned.shift();
            // format
            cleaned.forEach(r => {
                if (r.length === 2) out.push(`- ${r[0]}: ${r[1]}`);
                else out.push(`- ${r.filter(Boolean).join(' · ')}`);
            });
        } else {
            out.push(line);
            i++;
        }
    }
    return out.join('\n').trim();
}

function renderKeywordCard(label, keywords) {
    const safe = Array.isArray(keywords) ? keywords : [];
    const chips = safe.map(k => `<span class="keyword-chip" data-keyword="${escapeHtml(String(k))}">${escapeHtml(String(k))}</span>`).join('');
    return `
    <div class="keyword-card">
        <div class="keyword-card-header">${escapeHtml(label)}</div>
        <div class="keyword-card-body">${chips || '<span class="keyword-chip empty">키워드 없음</span>'}</div>
    </div>`;
}

// --- 키워드 칩 클릭 시 플랫폼 검색 메뉴 표시 ---
let __keywordMenuEl = null;
let __keywordMenuAnchor = null;

function closeKeywordMenu() {
    if (__keywordMenuEl && __keywordMenuEl.parentNode) {
        __keywordMenuEl.parentNode.removeChild(__keywordMenuEl);
    }
    __keywordMenuEl = null;
    __keywordMenuAnchor = null;
    window.removeEventListener('scroll', closeKeywordMenu, true);
    window.removeEventListener('resize', closeKeywordMenu, true);
}

function openKeywordMenu(anchorEl, keyword) {
    if (__keywordMenuAnchor === anchorEl && __keywordMenuEl) {
        closeKeywordMenu();
        return;
    }
    closeKeywordMenu();
    const q = encodeURIComponent(String(keyword || ''));
    const menu = document.createElement('div');
    menu.className = 'keyword-menu';
    menu.innerHTML = `
        <div class="keyword-menu-title">검색: ${escapeHtml(keyword)}</div>
        <a class="platform-link" href="https://www.youtube.com/results?search_query=${q}" target="_blank" rel="noopener noreferrer">YouTube</a>
        <a class="platform-link" href="https://www.instagram.com/explore/search/keyword/?q=${q}" target="_blank" rel="noopener noreferrer">Instagram</a>
        <a class="platform-link" href="https://www.tiktok.com/search?q=${q}" target="_blank" rel="noopener noreferrer">TikTok</a>
        <a class="platform-link" href="https://www.douyin.com/search/${q}" target="_blank" rel="noopener noreferrer">抖音 Douyin</a>
        <a class="platform-link" href="https://www.xiaohongshu.com/search_result?keyword=${q}" target="_blank" rel="noopener noreferrer">小红书 RED</a>
    `;
    document.body.appendChild(menu);
    __keywordMenuEl = menu;
    __keywordMenuAnchor = anchorEl;
    const rect = anchorEl.getBoundingClientRect();
    const top = window.scrollY + rect.bottom + 6;
    const left = window.scrollX + rect.left;
    menu.style.top = top + 'px';
    menu.style.left = left + 'px';
    menu.addEventListener('click', (e) => { e.stopPropagation(); });
    window.addEventListener('scroll', closeKeywordMenu, true);
    window.addEventListener('resize', closeKeywordMenu, true);
}

document.addEventListener('click', (e) => {
    const chip = e.target.closest('.keyword-chip');
    if (chip && !chip.classList.contains('empty')) {
        e.preventDefault();
        e.stopPropagation();
        const kw = chip.getAttribute('data-keyword') || chip.textContent || '';
        openKeywordMenu(chip, kw.trim());
        return;
    }
    if (__keywordMenuEl && !e.target.closest('.keyword-menu')) {
        closeKeywordMenu();
    }
});

function drawDopamineChart(canvas, data) {
    if (!canvas || !canvas.getContext || !Array.isArray(data) || data.length === 0) return;
    const parent = canvas.parentElement;
    const width = Math.max(320, parent?.clientWidth || 640);
    canvas.width = width;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // 차트 영역 설정
    const paddingLeft = 40;
    const paddingRight = 16;
    const paddingTop = 16;
    const paddingBottom = 28;
    const chartW = canvas.width - paddingLeft - paddingRight;
    const chartH = canvas.height - paddingTop - paddingBottom;

    // y축: 0~10 스케일
    const yMin = 0;
    const yMax = 10;
    const n = data.length;

    function xPos(i) {
        if (n === 1) return paddingLeft + chartW / 2;
        return paddingLeft + (i / (n - 1)) * chartW;
    }
    function yPos(value) {
        const v = Math.max(yMin, Math.min(yMax, Number(value) || 0));
        const ratio = (v - yMin) / (yMax - yMin);
        return paddingTop + (1 - ratio) * chartH;
    }

    // 격자 및 축
    ctx.strokeStyle = '#e5e7eb';
    ctx.lineWidth = 1;
    ctx.beginPath();
    // 가로 그리드: 0, 3, 6, 9, 10
    [0, 3, 6, 9, 10].forEach(val => {
        const y = yPos(val);
        ctx.moveTo(paddingLeft, y);
        ctx.lineTo(canvas.width - paddingRight, y);
    });
    ctx.stroke();

    // y축 눈금
    ctx.fillStyle = '#6b7280';
    ctx.font = '12px system-ui, -apple-system, Segoe UI, Roboto, Noto Sans KR, Arial';
    [0, 3, 6, 9, 10].forEach(val => {
        const y = yPos(val);
        ctx.fillText(String(val), 8, y + 4);
    });

    // 선 그래프 그리기
    ctx.strokeStyle = '#2563eb';
    ctx.lineWidth = 2;
    ctx.beginPath();
    data.forEach((item, i) => {
        const x = xPos(i);
        const y = yPos(item.level ?? item.score ?? 0);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // 포인트 및 색상(구간 색)
    data.forEach((item, i) => {
        const level = Number(item.level ?? item.score ?? 0);
        const x = xPos(i);
        const y = yPos(level);
        let color = '#94a3b8';
        if (level >= 4 && level <= 6) color = '#f59e0b';
        if (level >= 7 && level <= 9) color = '#ef4444';
        if (level >= 10) color = '#b91c1c';
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fill();
    });
}

// 페이지 로드 시 실행
fetchAndDisplayDetails();
