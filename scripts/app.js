import { supabase } from '../supabase-client.js';

// 컨테이너 및 입력 요소
const videoTableContainer = document.getElementById('video-table-container');
let videoTableBody = document.getElementById('video-table-body');
const paginationContainer = document.getElementById('pagination-container');
const searchInput = document.getElementById('searchInput');
const formTypeFilter = document.getElementById('form-type-filter');
const startDateFilter = document.getElementById('start-date-filter');
const endDateFilter = document.getElementById('end-date-filter');
const sortFilter = document.getElementById('sort-filter');

// 칩 & 통계 요소
const viewChips = document.querySelectorAll('.view-chip-group .chip');
const sortChips = document.querySelectorAll('.sort-chip-group .chip');
const statChannels = document.getElementById('stat-channels');
const statVideos = document.getElementById('stat-videos');
const statAvgRise = document.getElementById('stat-avg-rise');
const statUpdated = document.getElementById('stat-updated');
const statChannelsSub = document.getElementById('stat-channels-sub');
const statVideosSub = document.getElementById('stat-videos-sub');
const toggleStatsChip = document.getElementById('toggle-stats-chip');
const statsGrid = document.getElementById('stats-grid');
const pageSizeSelect = document.getElementById('page-size-select');

// 상태
let allVideos = [];
let filteredVideos = [];
let currentPage = 1;
let itemsPerPage = 100;
let viewMode = 'video'; // 'channel'
let sortMode = 'pct_desc'; // 'pct_desc' | 'abs_desc' | 'date_desc'

// 페이지네이션 쿼리 상태
let lastVisible = null;
let hasMore = true;
let dateCursor = null; // static JSON 기반 커서

// 로컬 캐시
const CACHE_TTL = 60 * 60 * 1000; // 1시간
const IDB_DB = 'videosCacheDB';
const IDB_STORE = 'kv';
const IDB_KEY = 'videosCompressed';

async function idbOpen() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(IDB_DB, 1);
        req.onupgradeneeded = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains(IDB_STORE)) db.createObjectStore(IDB_STORE);
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

async function idbGet(key) {
    try {
        const db = await idbOpen();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(IDB_STORE, 'readonly');
            const store = tx.objectStore(IDB_STORE);
            const r = store.get(key);
            r.onsuccess = () => resolve(r.result || null);
            r.onerror = () => reject(r.error);
        });
    } catch { return null; }
}

async function idbSet(key, value) {
    try {
        const db = await idbOpen();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(IDB_STORE, 'readwrite');
            const store = tx.objectStore(IDB_STORE);
            const r = store.put(value, key);
            r.onsuccess = () => resolve(true);
            r.onerror = () => reject(r.error);
        });
    } catch { return false; }
}

async function compressJSON(data) {
    const text = JSON.stringify(data);
    if ('CompressionStream' in window) {
        const cs = new CompressionStream('gzip');
        const blob = new Blob([text]);
        const stream = blob.stream().pipeThrough(cs);
        const buffer = await new Response(stream).arrayBuffer();
        return { ts: Date.now(), algo: 'gzip', buffer };
    }
    return { ts: Date.now(), algo: 'none', text };
}

async function decompressJSON(record) {
    if (!record) return null;
    if (record.algo === 'gzip' && 'DecompressionStream' in window) {
        const ds = new DecompressionStream('gzip');
        const stream = new Blob([record.buffer]).stream().pipeThrough(ds);
        const text = await new Response(stream).text();
        return { ts: record.ts, data: JSON.parse(text) };
    }
    if (record.text) return { ts: record.ts, data: JSON.parse(record.text) };
    return null;
}

// --------- 유틸 ---------
function parseCount(v) {
    if (typeof v === 'number') return v;
    const s = String(v || '');
    const digits = s.replace(/[^0-9]/g, '');
    return digits ? Number(digits) : 0;
}

function computeChangePct(doc) {
    const curr = parseCount(doc.views_numeric || doc.views || 0);
    const prev = parseCount(doc.views_prev_numeric || doc.views_baseline_numeric || doc.views || 0) || curr;
    if (!prev) return 0;
    return ((curr - prev) / prev) * 100;
}

function getRiseAbs(doc) {
    const curr = parseCount(doc.views_numeric || doc.views || 0) || 0;
    const prev = parseCount(doc.views_prev_numeric || doc.views_baseline_numeric || doc.views || 0) || curr;
    return Math.max(0, curr - prev);
}

function fmt(n) {
    try { return Number(n || 0).toLocaleString(); } catch { return String(n); }
}

function setActiveChip(groupNodeList, target) {
    groupNodeList.forEach(btn => btn.classList.remove('chip-active'));
    if (target) target.classList.add('chip-active');
}

function ensureTableSkeleton(mode) {
    // mode: 'video' | 'channel'
    if (!videoTableContainer) return;
    if (mode === 'channel') {
        videoTableContainer.innerHTML = `
        <table class="data-table">
            <thead>
                <tr>
                    <th>#</th><th>대표 썸네일</th><th>채널</th><th>영상 수</th>
                    <th>총 상승 조회수</th><th>평균 증가율</th><th>대표 영상</th>
                </tr>
            </thead>
            <tbody id="video-table-body"></tbody>
        </table>`;
    } else {
        videoTableContainer.innerHTML = `
        <table class="data-table" aria-describedby="stat-updated">
            <thead>
                <tr>
                    <th>#</th>
                    <th>썸네일</th>
                    <th>제목</th>
                    <th>채널</th>
                    <th>현재 조회수</th>
                    <th>기준</th>
                    <th>증가수</th>
                    <th>증가율</th>
                    <th>업데이트</th>
                    <th>링크</th>
                </tr>
            </thead>
            <tbody id="video-table-body"></tbody>
        </table>`;
    }
    videoTableBody = document.getElementById('video-table-body');
}

function updateStats(rows) {
    try {
        const channels = new Set(rows.map(v => (v.channel || '').trim()).filter(Boolean));
        const avg = rows.length ? rows.map(computeChangePct).reduce((a,b)=>a+b,0) / rows.length : 0;
        const maxTs = Math.max(...rows.map(r => Number(r.views_last_checked_at || 0)).filter(Boolean), 0);
        if (statChannels) statChannels.textContent = fmt(channels.size);
        if (statVideos) statVideos.textContent = fmt(rows.length);
        if (statAvgRise) statAvgRise.textContent = `${avg>=0?'+':''}${avg.toFixed(2)}%`;
        if (statUpdated) statUpdated.textContent = maxTs ? new Date(maxTs).toLocaleString() : '-';
        if (statChannelsSub) statChannelsSub.textContent = '고유 채널 수';
        if (statVideosSub) statVideosSub.textContent = '필터 적용됨';
    } catch {}
}

// --------- 데이터 로드 ---------
async function getCached() {
    const rec = await idbGet(IDB_KEY);
    const out = await decompressJSON(rec);
    return out ? { timestamp: out.ts, data: out.data } : null;
}
async function setCached(data) {
    const rec = await compressJSON(data);
    await idbSet(IDB_KEY, rec);
}

async function fetchVideos() {
    try {
        if (videoTableBody) videoTableBody.innerHTML = '<tr><td colspan="9" class="info-message">데이터를 불러오는 중...</td></tr>';
        // 0) CDN 정적 JSON 우선 시도 (public/data/videos.json 표준 경로). 로컬 렌더 후에도 계속 page 로드 가능하게 hasMore 유지
        try {
            const res = await fetch('/data/videos.json', { cache: 'no-cache' });
            if (res.ok) {
                allVideos = await res.json();
                await setCached(allVideos);
                filterAndRender();
                hasMore = true;
                updateLoadMoreVisibility();
                return;
            }
        } catch {}

        // system/settings 폴백은 사용하지 않음

        const cached = await getCached();
        if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
            allVideos = cached.data || [];
            filterAndRender();
            // 백그라운드 업데이트 확인
            checkForUpdates(cached.timestamp).catch(()=>{});
            return;
        }

        const { data, error } = await supabase
            .from('videos')
            .select('*')
            .order('date', { ascending: false })
            .range(0, itemsPerPage - 1);
        if (error) throw error;
        allVideos = Array.isArray(data) ? data : [];
        hasMore = allVideos.length === itemsPerPage;
        await setCached(allVideos);
        filterAndRender();
        updateLoadMoreVisibility();
    } catch (error) {
        console.error('Error fetching videos: ', error);
        ensureTableSkeleton('video');
        if (videoTableBody) videoTableBody.innerHTML = '<tr><td colspan="9" class="error-message">데이터를 불러오는 데 실패했습니다. Firebase 설정을 확인해주세요.</td></tr>';
    }
}

async function loadNextPage() {
    if (!hasMore) return;
    const offset = allVideos.length;
    const { data, error } = await supabase
        .from('videos')
        .select('*')
        .order('date', { ascending: false })
        .range(offset, offset + itemsPerPage - 1);
    const newVideos = (!error && Array.isArray(data)) ? data : [];
    if (newVideos.length) {
        allVideos = [...allVideos, ...newVideos];
        // 더 가져온 경우, 계속 더 시도할 수 있도록 hasMore 유지
        hasMore = newVideos.length === itemsPerPage;
        await setCached(allVideos);
        currentPage += 1; // 다음 페이지 노출
        filterAndRender(true);
        updateLoadMoreVisibility();
    } else {
        // 원격에서 더 이상 없지만, 로컬(allVideos/filteredVideos)에 아직 미노출 데이터가 있으면 페이지 증가로 표시
        const totalAvailable = Array.isArray(filteredVideos) && filteredVideos.length ? filteredVideos.length : allVideos.length;
        if (totalAvailable > currentPage * itemsPerPage) {
            currentPage += 1;
            filterAndRender(true);
            hasMore = true; // 아직 표시할 로컬 데이터가 남아있음
        } else {
            hasMore = false;
        }
        updateLoadMoreVisibility();
    }
}

async function checkForUpdates(sinceTs) {
    try {
        const { data, error } = await supabase
            .from('videos')
            .select('*')
            .gt('last_modified', sinceTs || 0)
            .order('last_modified', { ascending: false })
            .limit(50);
        if (error || !Array.isArray(data) || data.length === 0) return;
        const updates = data;
        const map = new Map(allVideos.map(v => [v.id, v]));
        updates.forEach(u => map.set(u.id, u));
        allVideos = Array.from(map.values());
        await setCached(allVideos);
        filterAndRender();
    } catch {}
}

function updateLoadMoreVisibility() {
    if (!loadMoreBtn) return;
    loadMoreBtn.style.display = hasMore ? 'inline-block' : 'none';
}

// --------- 필터링 ---------
function filterAndRender(keepPage = false) {
    filteredVideos = [...allVideos];
    const searchTerm = (searchInput?.value || '').toLowerCase();
    if (searchTerm) {
        filteredVideos = filteredVideos.filter(video => {
            const fieldsToSearch = [
                video.title, video.channel, video.kr_category_large,
                video.kr_category_medium, video.kr_category_small,
                video.material, video.template_type, video.group_name,
                video.source_type, video.hooking, video.narrative_structure
            ];
            return fieldsToSearch.some(field => field && String(field).toLowerCase().includes(searchTerm));
        });
    }
    const formType = formTypeFilter?.value || 'all';
    if (formType !== 'all') filteredVideos = filteredVideos.filter(v => v.group_name === formType);
    const startDate = startDateFilter?.value || '';
    const endDate = endDateFilter?.value || '';
    if (startDate) filteredVideos = filteredVideos.filter(v => v.date && v.date >= startDate);
    if (endDate) filteredVideos = filteredVideos.filter(v => v.date && v.date <= endDate);

    if (!keepPage) currentPage = 1;
    updateStats(filteredVideos);
    renderCurrentView();
}

// --------- 렌더링 ---------
function renderCurrentView() {
    if (viewMode === 'channel') {
        renderChannelView();
    } else {
        renderVideoView();
    }
    renderPagination();
}

function renderVideoView() {
    ensureTableSkeleton('video');
    if (!videoTableBody) return;

    // 정렬
    let rows = filteredVideos.slice();
    rows.forEach(r => { r._pct = computeChangePct(r); r._riseAbs = getRiseAbs(r); });
    rows.sort((a,b) => {
        if (sortMode === 'pct_desc') return (b._pct - a._pct) || (b._riseAbs - a._riseAbs);
        if (sortMode === 'abs_desc') return (b._riseAbs - a._riseAbs) || (b._pct - a._pct);
        // date_desc 또는 기타
        const dateA = a.date ? new Date(a.date).getTime() : 0;
        const dateB = b.date ? new Date(b.date).getTime() : 0;
                return dateB - dateA;
    });

    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageRows = rows.slice(startIndex, endIndex);

    if (!pageRows.length) {
        videoTableBody.innerHTML = '<tr><td colspan="9" class="info-message">조건에 맞는 항목이 없습니다.</td></tr>';
        return;
    }

    const html = pageRows.map((r, idx) => {
        const curr = parseCount(r.views_numeric || r.views || 0);
        const prev = parseCount(r.views_prev_numeric || r.views_baseline_numeric || r.views || 0) || curr;
        const pct = prev ? ((curr - prev) / prev) * 100 : 0;
        const riseColor = pct >= 0 ? '#16a34a' : '#dc2626';
        const thumbnail = r.thumbnail ? `<img src="${r.thumbnail}" class="table-thumbnail" loading="lazy" onerror="this.outerHTML=\'<div class=\\'no-thumbnail-placeholder\\'>이미지 없음</div>\'">` : `<div class="no-thumbnail-placeholder">이미지 없음</div>`;
        const lastChecked = r.views_last_checked_at ? new Date(r.views_last_checked_at).toLocaleString() : '-';
        return `
        <tr>
            <td>${startIndex + idx + 1}</td>
            <td>${thumbnail}</td>
            <td class="table-title"><a href="details.html?id=${r.id}" target="_blank">${r.title || ''}</a></td>
            <td>${r.channel || ''}</td>
            <td>${fmt(curr)}</td>
            <td>${fmt(prev)}</td>
            <td>${fmt(curr - prev)}</td>
            <td style="color:${riseColor}">${(pct>=0?'+':'') + pct.toFixed(2)}%</td>
            <td>${lastChecked}</td>
            <td><a class="btn btn-details" href="${r.youtube_url || '#'}" target="_blank">YouTube</a></td>
        </tr>`;
    }).join('');

    videoTableBody.innerHTML = html;
}

function groupByChannel(rows) {
    const map = new Map();
    for (const r of rows) {
        const channel = (r.channel || '').trim() || 'Unknown';
        if (!map.has(channel)) {
            map.set(channel, { channel, videos: [], totalRiseAbs: 0, avgRisePct: 0, representative: null });
        }
        const bucket = map.get(channel);
        const curr = parseCount(r.views_numeric || r.views || 0) || 0;
        const prev = parseCount(r.views_prev_numeric || r.views_baseline_numeric || r.views || 0) || curr;
        const riseAbs = Math.max(0, curr - prev);
        bucket.videos.push(r);
        bucket.totalRiseAbs += riseAbs;
        if (!bucket.representative) bucket.representative = r; else {
            const d1 = r.date ? new Date(r.date).getTime() : 0;
            const d2 = bucket.representative.date ? new Date(bucket.representative.date).getTime() : 0;
            if (d1 > d2) bucket.representative = r;
        }
    }
    for (const bucket of map.values()) {
        const pcts = bucket.videos.map(v => computeChangePct(v));
        bucket.avgRisePct = pcts.length ? (pcts.reduce((a,b)=>a+b,0) / pcts.length) : 0;
    }
    return Array.from(map.values());
}

function renderChannelView() {
    ensureTableSkeleton('channel');
    if (!videoTableBody) return;

    let rows = groupByChannel(filteredVideos);
    rows.sort((a,b) => {
        if (sortMode === 'pct_desc') return (b.avgRisePct - a.avgRisePct) || (b.totalRiseAbs - a.totalRiseAbs);
        if (sortMode === 'abs_desc') return (b.totalRiseAbs - a.totalRiseAbs) || (b.avgRisePct - a.avgRisePct);
        // 기본은 abs_desc와 유사
        return (b.totalRiseAbs - a.totalRiseAbs) || (b.avgRisePct - a.avgRisePct);
    });

    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageRows = rows.slice(startIndex, endIndex);

    if (!pageRows.length) {
        videoTableBody.innerHTML = '<tr><td colspan="7" class="info-message">조건에 맞는 항목이 없습니다.</td></tr>';
        return;
    }

    const html = pageRows.map((r, idx) => {
        const thumb = r.representative?.thumbnail ? `<img src="${r.representative.thumbnail}" class="table-thumbnail" loading="lazy" onerror="this.outerHTML=\'<div class=\\'no-thumbnail-placeholder\\'>이미지 없음</div>\'">` : `<div class="no-thumbnail-placeholder">이미지 없음</div>`;
        // 대표 영상이 있다면 상세 페이지로 이동하도록 연결
        const repId = r.representative?.id || '';
        const link = repId ? `details.html?id=${encodeURIComponent(repId)}` : '#';
        return `
            <tr>
            <td>${startIndex + idx + 1}</td>
            <td>${thumb}</td>
            <td class="table-title">${r.channel}</td>
            <td>${r.videos.length}</td>
            <td>${fmt(r.totalRiseAbs)}</td>
            <td style="color:${r.avgRisePct>=0? '#16a34a':'#dc2626'}">${(r.avgRisePct>=0?'+':'') + r.avgRisePct.toFixed(2)}%</td>
            <td><a class="btn btn-details" href="${link}" target="_blank">자세히</a></td>
        </tr>`;
    }).join('');

    videoTableBody.innerHTML = html;
}

function getTotalPages() {
    if (viewMode === 'channel') {
        const rows = groupByChannel(filteredVideos);
        return Math.max(1, Math.ceil(rows.length / itemsPerPage));
    }
    return Math.max(1, Math.ceil(filteredVideos.length / itemsPerPage));
}

function renderPagination() {
    // 무한 스크롤로 대체: 페이지네이션 UI 제거
    if (!paginationContainer) return;
    paginationContainer.innerHTML = '';
}

// --------- 이벤트 ---------
[searchInput, formTypeFilter, startDateFilter, endDateFilter].forEach(el => {
    if (el) {
        el.addEventListener('input', filterAndRender);
        if (el.tagName === 'SELECT' || el.type === 'date') el.addEventListener('change', filterAndRender);
    }
});

// 칩: 보기 전환
viewChips.forEach(btn => {
    btn.addEventListener('click', () => {
        setActiveChip(viewChips, btn);
        viewMode = btn.getAttribute('data-view') || 'video';
        currentPage = 1;
        renderCurrentView();
        renderPagination();
    });
});

// 칩: 정렬 전환
sortChips.forEach(btn => {
    btn.addEventListener('click', () => {
        setActiveChip(sortChips, btn);
        sortMode = btn.getAttribute('data-sort') || 'pct_desc';
        currentPage = 1;
        renderCurrentView();
        renderPagination();
    });
});

// 세부 정렬 드롭다운은 부가 옵션으로 유지: 변경 시 날짜 기준으로만 반영
if (sortFilter) {
    sortFilter.addEventListener('change', () => {
        if (sortMode !== 'date_desc') return; // 날짜 정렬 모드일 때만 반영
        // date_desc에서 추가 옵션은 기존 filterAndRender가 아니라 간단히 재정렬만
        renderCurrentView();
    });
}

// 초기 스켈레톤 & 데이터 로드
ensureTableSkeleton('video');
fetchVideos();

// 통계 카드 토글
if (toggleStatsChip && statsGrid) {
    toggleStatsChip.addEventListener('click', () => {
        statsGrid.classList.toggle('hidden');
    });
}

// 페이지당 개수 변경
if (pageSizeSelect) {
    pageSizeSelect.addEventListener('change', () => {
        const v = Number(pageSizeSelect.value || 100);
        itemsPerPage = isFinite(v) && v > 0 ? v : 100;
        currentPage = 1;
        renderCurrentView();
        renderPagination();
    });
}

// "더 보기" 버튼 또는 무한 스크롤 핸들러
// 무한 스크롤: IntersectionObserver 기반(뷰포트 하단 센티넬)
let isLoadingNext = false;
const sentinelEl = document.getElementById('scroll-sentinel');
if ('IntersectionObserver' in window && sentinelEl) {
    const io = new IntersectionObserver(async (entries) => {
        for (const e of entries) {
            if (e.isIntersecting && hasMore && !isLoadingNext) {
                isLoadingNext = true;
                try { await loadNextPage(); } finally { isLoadingNext = false; }
            }
        }
    }, { root: null, rootMargin: '200px 0px 200px 0px', threshold: 0 });
    io.observe(sentinelEl);
} else {
    // 폴백: 스크롤 근접 감지
    window.addEventListener('scroll', async () => {
        if (isLoadingNext) return;
        const nearBottom = window.innerHeight + window.scrollY >= document.body.offsetHeight - 200;
        if (nearBottom && hasMore) {
            isLoadingNext = true;
            try { await loadNextPage(); } finally { isLoadingNext = false; }
        }
    });
}
