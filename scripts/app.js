import { supabase } from '../supabase-client.js';

// 컨테이너 및 입력 요소
const videoTableContainer = document.getElementById('video-table-container');
let videoTableBody = document.getElementById('video-table-body');
const paginationContainer = document.getElementById('pagination-container');
const loadMoreBtn = document.getElementById('load-more-btn');
const searchInput = document.getElementById('searchInput');
const startDateFilter = null;
const endDateFilter = null;
const updateDateFilter = document.getElementById('update-date-filter');
if (updateDateFilter) {
    // 클릭/포커스 시 캘린더 오픈
    const openPicker = () => { try { if (typeof updateDateFilter.showPicker === 'function') updateDateFilter.showPicker(); } catch {} };
    updateDateFilter.addEventListener('click', openPicker);
    updateDateFilter.addEventListener('focus', openPicker);
    // 텍스트 타이핑 방지(탭/엔터/ESC/방향키만 허용)
    updateDateFilter.addEventListener('keydown', (e) => {
        const allow = ['Tab','Enter','Escape','ArrowLeft','ArrowRight','ArrowUp','ArrowDown'];
        if (!allow.includes(e.key)) e.preventDefault();
    });
}
const sortFilter = document.getElementById('sort-filter');
// 구독자 필터 UI 요소
const subsChips = document.querySelectorAll('.chip-subs');
const subsMinInput = document.getElementById('subs-min');
const subsMaxInput = document.getElementById('subs-max');
const subsApplyBtn = document.getElementById('subs-apply');
const subsResetBtn = document.getElementById('subs-reset');

// 칩 & 통계 요소
// 주의: 구독자 바에도 'view-chip-group' 클래스가 사용되던 문제가 있어, 뷰 전환 칩은 필터 컨테이너 내로 한정
const viewChips = document.querySelectorAll('.filter-container .view-chip-group .chip');
const sortChips = document.querySelectorAll('.sort-chip-group .chip');
const statChannels = document.getElementById('stat-channels');
const statVideos = document.getElementById('stat-videos');
const statAvgRise = document.getElementById('stat-avg-rise');
const statUpdated = document.getElementById('stat-updated');
const statChannelsSub = document.getElementById('stat-channels-sub');
const statVideosSub = document.getElementById('stat-videos-sub');
const toggleStatsChip = document.getElementById('toggle-stats-chip');
const statsGrid = document.getElementById('stats-grid');
// 페이지 크기 UI 제거. 내부 배치 크기만 사용
const PAGE_BATCH = 200;              // 페이지 표시 200개
const DB_FETCH_BATCH = 1000;         // Supabase 요청당 최대 1000 권장
const DB_CONCURRENCY = 4;            // 병렬 요청 개수
const FETCH_ALL_FROM_DB = true;      // DB에서 전량 로드 모드(페이지네이션은 클라이언트 분할)
const LIVE_SYNC_INTERVAL_MS = 2000;  // 실시간 증분 동기화 주기 (2초)
const SKIP_CACHE_ON_FIRST_LOAD = true; // 첫 진입 시 캐시 무시하고 항상 최신 로드
// 스키마 변경/추가 컬럼 호환을 위해 우선 전체 컬럼을 선택
const VIDEO_COLUMNS = '*';

// 상태
let allVideos = [];
let filteredVideos = [];
// 페이지 개념 없이 연속 표시. 요청 배치는 PAGE_BATCH로 처리
let viewMode = 'video'; // 'channel'
let currentPage = 1;     // 1-based 페이지 인덱스
let sortMode = 'pct_desc'; // 'pct_desc' | 'abs_desc' | 'date_desc' | 'update_desc'
let subsFilter = { preset: 'all', min: null, max: null };
let expandedChannels = new Set();
let liveSyncTimer = null;

// 페이지네이션 쿼리 상태
let lastVisible = null;
let hasMore = true;
let dateCursor = null; // static JSON 기반 커서

// 로컬 캐시
const CACHE_TTL = 60 * 60 * 1000; // 1시간
const IDB_DB = 'videosCacheDB';
const IDB_STORE = 'kv';
const IDB_KEY = 'videosCompressed';
const IDB_VER_KEY = 'videosVersionTag';

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

// 사전 계산: 자주 쓰는 숫자/검색 인덱스 캐싱
function precomputeOne(v) {
    // 조회수 현재/이전값
    const curr = parseCount(v.views_numeric || v.views || 0) || 0;
    const prev = parseCount(v.views_prev_numeric || v.views_baseline_numeric || v.views || 0) || curr;
    v.__viewsCurr = curr;
    v.__viewsPrev = prev;
    // 증가량/증가율
    v.__riseAbs = Math.max(0, curr - prev);
    v.__pct = prev ? ((curr - prev) / prev) * 100 : 0;
    // 구독자 수, 날짜 타임스탬프
    v.__subs = parseCount(v.subscribers_numeric || v.subscribers || 0) || 0;
    v.__dateTs = v.date ? new Date(v.date).getTime() : 0;
    v.__updateTs = v.update_date ? new Date(v.update_date).getTime() : 0;
    // 검색 인덱스(소문자)
    v.__search = buildSearchIndex(v);
}

function buildSearchIndex(v) {
    try {
        const parts = [
            v.title, v.channel,
            v.kr_category_large, v.kr_category_medium, v.kr_category_small,
            v.material, v.template_type, v.group_name,
            v.source_type, v.hooking, v.narrative_structure
        ].filter(Boolean).map(s => String(s).toLowerCase());
        return parts.join(' | ');
    } catch {
        return '';
    }
}

function precomputeNumericFields(arr) {
    if (!Array.isArray(arr)) return;
    for (const v of arr) precomputeOne(v);
}

function debounce(fn, wait = 250) {
    let t = null;
    return (...args) => {
        if (t) clearTimeout(t);
        t = setTimeout(() => fn(...args), wait);
    };
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
                    <th>현재 조회수 합</th><th>평균 증가율</th><th>대표영상</th>
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
        const avg = rows.length ? rows.map(v => (v.__pct != null ? v.__pct : computeChangePct(v))).reduce((a,b)=>a+b,0) / rows.length : 0;
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

function getLocalLatestTs() {
    try { return Math.max(0, ...allVideos.map(v => Number(v.last_modified || 0)).filter(Boolean)); } catch { return 0; }
}

async function pollLatestAndSync() {
    try {
        // 1) 증분 변경 감지: last_modified 최대값
        const { data } = await supabase.from('videos').select('last_modified').order('last_modified', { ascending: false }).limit(1);
        const remoteTs = Number((data && data[0]?.last_modified) || 0) || 0;
        const localTs = getLocalLatestTs();
        if (remoteTs > localTs) await syncIncrementalUpdates(localTs);
        // 2) 최신 N개 푸시 업데이트(상위 목록 즉시 반영)
        const TOP_N = 300;
        const { data: top } = await supabase.from('videos').select('*').order('last_modified', { ascending: false }).limit(TOP_N);
        if (Array.isArray(top) && top.length) {
            const map = new Map(allVideos.map(v => [v.id, v]));
            for (const r of top) map.set(r.id, r);
            allVideos = Array.from(map.values());
            precomputeNumericFields(allVideos);
            setCached(allVideos);
            filterAndRender(true);
        }
    } catch {}
}

function startLiveSync() {
    if (liveSyncTimer) return;
    // 즉시 1회 실행 후 주기 반복
    pollLatestAndSync();
    liveSyncTimer = setInterval(pollLatestAndSync, LIVE_SYNC_INTERVAL_MS);
}

function stopLiveSync() { if (liveSyncTimer) { clearInterval(liveSyncTimer); liveSyncTimer = null; } }

// 서버 버전 태그 조회 (총 개수 + 최신 last_modified)
async function fetchDatasetVersion() {
    let total = 0; let newest = 0;
    try {
        const { count } = await supabase.from('videos').select('id', { count: 'exact', head: true });
        if (typeof count === 'number') total = count;
    } catch {}
    try {
        const { data } = await supabase.from('videos').select('last_modified').order('last_modified', { ascending: false }).limit(1);
        if (Array.isArray(data) && data[0]?.last_modified) newest = Number(data[0].last_modified) || 0;
    } catch {}
    return { total, newest, tag: `${total}:${newest}` };
}

async function fetchVideos() {
    const t0 = performance.now();
    let loadedOk = false;
    try {
        if (videoTableBody) videoTableBody.innerHTML = '<tr><td colspan="9" class="info-message">데이터를 불러오는 중...</td></tr>';
        // 0) 서버 버전 태그 확인 → 로컬 버전과 다르면 무조건 전량 재로딩
        let remoteVer = null;
        try { remoteVer = await fetchDatasetVersion(); } catch {}
        const localVer = await idbGet(IDB_VER_KEY);

        const firstLoadDone = sessionStorage.getItem('firstLoadDone') === '1';
        const allowCache = !SKIP_CACHE_ON_FIRST_LOAD || firstLoadDone;
        if (allowCache && remoteVer?.tag && localVer && remoteVer.tag === localVer) {
            // 버전 일치 시 캐시 사용(TTL 무시)
            const cached = await getCached();
            if (cached?.data?.length) {
                allVideos = cached.data || [];
                precomputeNumericFields(allVideos);
                filterAndRender();
                loadedOk = true;
                return;
            }
        }

        if (FETCH_ALL_FROM_DB) {
            allVideos = await fetchAllFromSupabase();
            hasMore = false;
        } else {
            const { data, error } = await supabase
                .from('videos')
                .select(VIDEO_COLUMNS, { count: 'exact' })
                .order('date', { ascending: false })
                .range(0, PAGE_BATCH - 1);
            if (error) throw error;
            allVideos = Array.isArray(data) ? data : [];
            hasMore = (data?.length || 0) === PAGE_BATCH;
        }
        await setCached(allVideos);
        if (remoteVer?.tag) await idbSet(IDB_VER_KEY, remoteVer.tag);
        // 사전 계산(숫자 필드 캐싱)
        precomputeNumericFields(allVideos);
        filterAndRender();
        updateLoadMoreVisibility();
        loadedOk = true;
        try { sessionStorage.setItem('firstLoadDone', '1'); } catch {}
        startLiveSync();
    } catch (error) {
        console.error('Error fetching videos: ', error);
        // 에러를 화면에 즉시 표시하지 않고, 스피너를 유지해 후속 로딩(캐시/재시도)이 완료되도록 둡니다.
    }
    finally {
        const t1 = performance.now();
        console.info(`[perf] initial fetchVideos total: ${(t1 - t0).toFixed(0)}ms, rows=${allVideos.length}`);
    }
}

// Supabase에서 전량(14k) 로드: 1000개 단위 병렬 페이징
async function fetchAllFromSupabase() {
    const t0 = performance.now();
    // 1) 총 개수 조회(헤더만)
    let total = 0;
    try {
        const { count, error: cntErr } = await supabase
            .from('videos')
            .select('id', { count: 'exact', head: true });
        if (!cntErr && typeof count === 'number') total = count;
    } catch {}

    const out = [];
    // 2) 총 개수가 없으면 페이지를 모를 때 until-exhaust 방식으로 0..,1000.. 반복
    if (!total || total <= 0) {
        let offset = 0;
        while (true) {
            const { data, error } = await supabase
                .from('videos')
                .select(VIDEO_COLUMNS)
                .order('date', { ascending: false })
                .range(offset, offset + DB_FETCH_BATCH - 1);
            if (error) { console.error('fetchAllFromSupabase page error', error); break; }
            const batch = Array.isArray(data) ? data : [];
            if (!batch.length) break;
            out.push(...batch);
            if (batch.length < DB_FETCH_BATCH) break;
            offset += DB_FETCH_BATCH;
        }
        return dedupeById(out);
    }

    // 3) 총 개수 기반으로 병렬 페치
    const ranges = [];
    for (let start = 0; start < total; start += DB_FETCH_BATCH) {
        ranges.push([start, Math.min(start + DB_FETCH_BATCH - 1, total - 1)]);
    }
    // 병렬 제한
    const results = [];
    for (let i = 0; i < ranges.length; i += DB_CONCURRENCY) {
        const slice = ranges.slice(i, i + DB_CONCURRENCY);
        const chunk = await Promise.all(slice.map(async ([from, to]) => {
            const { data, error } = await supabase
                .from('videos')
                .select(VIDEO_COLUMNS)
                .order('date', { ascending: false })
                .range(from, to);
            if (error) { console.error('fetchAllFromSupabase range error', error); return []; }
            return Array.isArray(data) ? data : [];
        }));
        chunk.forEach(arr => results.push(...arr));
    }
    const all = dedupeById(results);
    precomputeNumericFields(all);
    const t1 = performance.now();
    console.info(`[perf] fetchAllFromSupabase: ${(t1 - t0).toFixed(0)}ms, rows=${all.length}`);
    return all;
}

function dedupeById(rows) {
    const map = new Map();
    for (const r of rows) { if (r && r.id) map.set(r.id, r); }
    return Array.from(map.values());
}

async function loadNextPage() {
    if (!hasMore) return;
    const offset = allVideos.length;
    const { data, error } = await supabase
        .from('videos')
        .select(VIDEO_COLUMNS)
        .order('date', { ascending: false })
        .range(offset, offset + PAGE_BATCH - 1);
    const newVideos = (!error && Array.isArray(data)) ? data : [];
    if (newVideos.length) {
        // 중복 합치기 방지: id 기준으로 병합
        const map = new Map(allVideos.map(v => [v.id, v]));
        newVideos.forEach(v => map.set(v.id, v));
        allVideos = Array.from(map.values());
        hasMore = newVideos.length === PAGE_BATCH;
        await setCached(allVideos);
        filterAndRender(true);
        updateLoadMoreVisibility();
    } else {
        // 원격에서 더 이상 없지만, 로컬(allVideos/filteredVideos)에 아직 미노출 데이터가 있으면 페이지 증가로 표시
        hasMore = false;
        updateLoadMoreVisibility();
    }
}

async function syncIncrementalUpdates(sinceTs) {
    try {
        let lastTs = Number(sinceTs || 0);
        const LIMIT = 1000;
        while (true) {
            const { data, error } = await supabase
                .from('videos')
                .select(VIDEO_COLUMNS)
                .gt('last_modified', lastTs)
                .order('last_modified', { ascending: true })
                .limit(LIMIT);
            if (error || !Array.isArray(data) || data.length === 0) break;
            const map = new Map(allVideos.map(v => [v.id, v]));
            for (const u of data) map.set(u.id, u);
            allVideos = Array.from(map.values());
            precomputeNumericFields(allVideos);
            await setCached(allVideos);
            lastTs = Number(data[data.length - 1]?.last_modified || lastTs);
            if (data.length < LIMIT) break;
        }
        // 현재 페이지 유지하여 부드럽게 갱신
        filterAndRender(true);
    } catch {}
}

function updateLoadMoreVisibility() {
    if (!loadMoreBtn) return;
    loadMoreBtn.style.display = hasMore ? 'inline-block' : 'none';
}

// --------- 필터링 ---------
function filterAndRender(keepPage = false) {
    const t0 = performance.now();
    filteredVideos = [...allVideos];
    const searchTerm = (searchInput?.value || '').toLowerCase().trim();
    if (searchTerm) {
        filteredVideos = filteredVideos.filter(v => {
            const idx = v.__search || buildSearchIndex(v);
            return idx.includes(searchTerm);
        });
    }
    // 폼 유형 필터 제거됨
    const startDate = startDateFilter?.value || '';
    const endDate = endDateFilter?.value || '';
    // 게시일 범위 필터 제거됨
    const updateDate = updateDateFilter?.value || '';
    if (updateDate) filteredVideos = filteredVideos.filter(v => v.update_date && v.update_date.slice(0,10) === updateDate);

    // 구독자 필터 적용
    if (subsFilter) {
        let min = subsFilter.min != null ? subsFilter.min : null;
        let max = subsFilter.max != null ? subsFilter.max : null;
        switch (subsFilter.preset) {
            case 'lt1k': min = 0; max = 1000; break;
            case '1k-10k': min = 1000; max = 10000; break;
            case '10k-100k': min = 10000; max = 100000; break;
            case '100k-1m': min = 100000; max = 1000000; break;
            case '>=1m': min = 1000000; max = null; break;
        }
        if (min != null || max != null) {
            filteredVideos = filteredVideos.filter(v => {
                const subs = v.__subs != null ? v.__subs : parseCount(v.subscribers_numeric || v.subscribers || 0);
                if (min != null && subs < min) return false;
                if (max != null && subs > max) return false;
                return true;
            });
        }
    }

    if (!keepPage) currentPage = 1;
    updateStats(filteredVideos);
    const t1 = performance.now();
    renderCurrentView();
    const t2 = performance.now();
    console.info(`[perf] filter: ${(t1 - t0).toFixed(0)}ms, render: ${(t2 - t1).toFixed(0)}ms, rows=${filteredVideos.length}, mode=${viewMode}`);
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
    const t0 = performance.now();
    ensureTableSkeleton('video');
    if (!videoTableBody) return;

    // 정렬
    let rows = filteredVideos.slice();
    rows.forEach(r => { if (r.__pct == null || r.__riseAbs == null) precomputeOne(r); });
    rows.sort((a,b) => {
        if (sortMode === 'pct_desc') return (b.__pct - a.__pct) || (b.__riseAbs - a.__riseAbs);
        if (sortMode === 'abs_desc') return (b.__riseAbs - a.__riseAbs) || (b.__pct - a.__pct);
        if (sortMode === 'views_desc' || sortMode === 'views_asc') {
            const av = a.__viewsCurr != null ? a.__viewsCurr : parseCount(a.views_numeric || a.views || 0);
            const bv = b.__viewsCurr != null ? b.__viewsCurr : parseCount(b.views_numeric || b.views || 0);
            return sortMode === 'views_desc' ? (bv - av) : (av - bv);
        }
        if (sortMode === 'subs_desc' || sortMode === 'subs_asc') {
            const as = a.__subs != null ? a.__subs : parseCount(a.subscribers_numeric || a.subscribers || 0);
            const bs = b.__subs != null ? b.__subs : parseCount(b.subscribers_numeric || b.subscribers || 0);
            return sortMode === 'subs_desc' ? (bs - as) : (as - bs);
        }
        if (sortMode === 'update_desc') {
            const ua = a.__updateTs != null ? a.__updateTs : (a.update_date ? new Date(a.update_date).getTime() : 0);
            const ub = b.__updateTs != null ? b.__updateTs : (b.update_date ? new Date(b.update_date).getTime() : 0);
            return ub - ua;
        }
        // date_desc 또는 기타
        const dateA = a.__dateTs != null ? a.__dateTs : (a.date ? new Date(a.date).getTime() : 0);
        const dateB = b.__dateTs != null ? b.__dateTs : (b.date ? new Date(b.date).getTime() : 0);
        return dateB - dateA;
    });

    const total = rows.length;
    if (!total) {
        videoTableBody.innerHTML = '<tr><td colspan="9" class="info-message">조건에 맞는 항목이 없습니다.</td></tr>';
        return;
    }

    // 현재 페이지 슬라이스
    const startIndex = (currentPage - 1) * PAGE_BATCH;
    const endIndex = startIndex + PAGE_BATCH;
    const pageRows = rows.slice(startIndex, endIndex);

    const html = pageRows.map((r, idx) => {
        const curr = r.__viewsCurr != null ? r.__viewsCurr : parseCount(r.views_numeric || r.views || 0);
        const prev = r.__viewsPrev != null ? r.__viewsPrev : (parseCount(r.views_prev_numeric || r.views_baseline_numeric || r.views || 0) || curr);
        const pct = r.__pct != null ? r.__pct : (prev ? ((curr - prev) / prev) * 100 : 0);
        const riseColor = pct >= 0 ? '#16a34a' : '#dc2626';
        const thumbnail = r.thumbnail ? `<img src="${r.thumbnail}" class="table-thumbnail" loading="lazy" onerror="this.outerHTML=\'<div class=\\'no-thumbnail-placeholder\\'>이미지 없음</div>\'">` : `<div class="no-thumbnail-placeholder">이미지 없음</div>`;
        const lastChecked = r.update_date ? new Date(r.update_date).toLocaleDateString('ko-KR') : (r.views_last_checked_at ? new Date(r.views_last_checked_at).toLocaleString() : '-');
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
    const t1 = performance.now();
    console.info(`[perf] renderVideoView: ${(t1 - t0).toFixed(0)}ms, pageRows=${pageRows.length}`);
}

function groupByChannel(rows) {
    const t0 = performance.now();
    const map = new Map();
    for (const r of rows) {
        const channel = (r.channel || '').trim() || 'Unknown';
        if (!map.has(channel)) {
            map.set(channel, { channel, videos: [], totalRiseAbs: 0, totalViews: 0, avgRisePct: 0, representative: null });
        }
        const bucket = map.get(channel);
        const curr = r.__viewsCurr != null ? r.__viewsCurr : (parseCount(r.views_numeric || r.views || 0) || 0);
        const prev = r.__viewsPrev != null ? r.__viewsPrev : (parseCount(r.views_prev_numeric || r.views_baseline_numeric || r.views || 0) || curr);
        const riseAbs = Math.max(0, curr - prev);
        bucket.videos.push(r);
        bucket.totalRiseAbs += riseAbs;
        bucket.totalViews += curr;
        // 대표 영상: 현재 조회수 최대
        if (!bucket.representative) bucket.representative = r; else {
            const curRepViews = bucket.representative.__viewsCurr != null ? bucket.representative.__viewsCurr : (parseCount(bucket.representative.views_numeric || bucket.representative.views || 0) || 0);
            if (curr > curRepViews) bucket.representative = r;
        }
    }
    for (const bucket of map.values()) {
        const pcts = bucket.videos.map(v => (v.__pct != null ? v.__pct : computeChangePct(v)));
        bucket.avgRisePct = pcts.length ? (pcts.reduce((a,b)=>a+b,0) / pcts.length) : 0;
    }
    const out = Array.from(map.values());
    const t1 = performance.now();
    console.info(`[perf] groupByChannel: ${(t1 - t0).toFixed(0)}ms, channels=${out.length}, rows=${rows.length}`);
    return out;
}

function renderChannelView() {
    const t0 = performance.now();
    ensureTableSkeleton('channel');
    if (!videoTableBody) return;

    let rows = groupByChannel(filteredVideos);
    rows.sort((a,b) => {
        if (sortMode === 'pct_desc') return (b.avgRisePct - a.avgRisePct) || (b.totalViews - a.totalViews);
        if (sortMode === 'abs_desc') return (b.totalViews - a.totalViews) || (b.avgRisePct - a.avgRisePct);
        // 기본: totalViews 내림차순
        return (b.totalViews - a.totalViews) || (b.avgRisePct - a.avgRisePct);
    });

    const total = rows.length;
    if (!total) {
        videoTableBody.innerHTML = '<tr><td colspan="7" class="info-message">조건에 맞는 항목이 없습니다.</td></tr>';
        return;
    }
    const startIndex = (currentPage - 1) * PAGE_BATCH;
    const endIndex = startIndex + PAGE_BATCH;
    const pageRows = rows.slice(startIndex, endIndex);
    const html = pageRows.map((r, idx) => {
        const thumb = r.representative?.thumbnail ? `<img src="${r.representative.thumbnail}" class="table-thumbnail" loading="lazy" onerror="this.outerHTML=\'<div class=\\'no-thumbnail-placeholder\\'>이미지 없음</div>\'">` : `<div class="no-thumbnail-placeholder">이미지 없음</div>`;
        // 대표 영상이 있다면 상세 페이지로 이동하도록 연결
        const repId = r.representative?.id || '';
        const link = repId ? `details.html?id=${encodeURIComponent(repId)}` : '#';
        // 모든 영상 썸네일 목록 (최신순)
        const videosSorted = r.videos.slice().sort((a,b) => {
            const da = a.date ? new Date(a.date).getTime() : 0;
            const db = b.date ? new Date(b.date).getTime() : 0;
            return db - da;
        });
        const thumbs = videosSorted.map(v => {
            const t = v.thumbnail ? `<img src="${v.thumbnail}" class="thumb-mini" loading="lazy" onerror="this.outerHTML=\'<div class=\\'no-thumb-mini\\'>-</div>\'">` : `<div class="no-thumb-mini">-</div>`;
            const vid = v.id ? `details.html?id=${encodeURIComponent(v.id)}` : '#';
            const title = (v.title || '').replace(/"/g, '');
            return `<a class="thumb-link" href="${vid}" target="_blank" title="${title}">${t}</a>`;
        }).join('');
        const isOpen = expandedChannels.has(r.channel);
        return `
            <tr class="channel-row" data-channel="${r.channel}">
                <td>${startIndex + idx + 1}</td>
                <td>${thumb}</td>
                <td class="table-title toggle-channel" title="클릭하여 영상 목록 열기/닫기">${r.channel}</td>
                <td>${r.videos.length}</td>
            <td>${fmt(r.totalViews)}</td>
                <td style=\"color:${r.avgRisePct>=0? '#16a34a':'#dc2626'}\">${(r.avgRisePct>=0?'+':'') + r.avgRisePct.toFixed(2)}%</td>
                <td><button class="btn btn-details open-details" data-rep="${repId}">자세히</button></td>
            </tr>
            <tr class="channel-videos-row ${isOpen ? 'open' : ''}" data-channel="${r.channel}">
                <td colspan="7">
                    <div class="channel-videos ${isOpen ? 'expanded' : ''}">${thumbs}</div>
                </td>
            </tr>`;
    }).join('');

    videoTableBody.innerHTML = html;
    const t1 = performance.now();
    console.info(`[perf] renderChannelView: ${(t1 - t0).toFixed(0)}ms, pageRows=${pageRows.length}`);
}

function getTotalPages() {
    const rows = viewMode === 'channel' ? groupByChannel(filteredVideos) : filteredVideos;
    return Math.max(1, Math.ceil(rows.length / PAGE_BATCH));
}

function renderPagination() {
    if (!paginationContainer) return;
    const totalPages = getTotalPages();
    if (totalPages <= 1) { paginationContainer.innerHTML = ''; return; }
    const makeBtn = (p) => `<button class="page-btn ${p===currentPage?'active':''}" data-page="${p}">${p}</button>`;
    const maxShow = 9; // 1 2 3 4 5 6 7 8 9
    let start = Math.max(1, currentPage - Math.floor(maxShow/2));
    let end = Math.min(totalPages, start + maxShow - 1);
    if (end - start + 1 < maxShow) start = Math.max(1, end - maxShow + 1);
    const parts = [];
    if (currentPage > 1) parts.push(`<button class="page-btn" data-page="${currentPage-1}">이전</button>`);
    if (start > 1) parts.push(makeBtn(1));
    if (start > 2) parts.push('<span style="color:var(--text-secondary);padding:4px 6px;">...</span>');
    for (let p = start; p <= end; p++) parts.push(makeBtn(p));
    if (end < totalPages - 1) parts.push('<span style="color:var(--text-secondary);padding:4px 6px;">...</span>');
    if (end < totalPages) parts.push(makeBtn(totalPages));
    if (currentPage < totalPages) parts.push(`<button class="page-btn" data-page="${currentPage+1}">다음</button>`);
    paginationContainer.innerHTML = parts.join('');
}

// --------- 이벤트 ---------
[searchInput, updateDateFilter].forEach(el => {
    if (el) {
        // 검색 인풋은 디바운스 적용
        if (el === searchInput) {
            const debounced = debounce(() => filterAndRender(false), 250);
            el.addEventListener('input', debounced);
        } else {
            el.addEventListener('input', filterAndRender);
        }
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

// 세부 정렬 드롭다운: 조회수/구독자 오름·내림차순 반영
if (sortFilter) {
    sortFilter.addEventListener('change', () => {
        const v = sortFilter.value;
        if (v === 'views_desc') sortMode = 'views_desc';
        else if (v === 'views_asc') sortMode = 'views_asc';
        else if (v === 'subs_desc') sortMode = 'subs_desc';
        else if (v === 'subs_asc') sortMode = 'subs_asc';
        else sortMode = 'date_desc';
        currentPage = 1;
        renderCurrentView();
        renderPagination();
    });
}

// 초기 스켈레톤 & 데이터 로드
ensureTableSkeleton('video');
fetchVideos();

// 캐시 초기화 버튼
document.getElementById('clear-cache-btn')?.addEventListener('click', async () => {
    try {
        await idbSet(IDB_KEY, null);
        await idbSet(IDB_VER_KEY, null);
        allVideos = [];
        filteredVideos = [];
        currentPage = 1;
        if (videoTableBody) videoTableBody.innerHTML = '<tr><td colspan="9" class="info-message">캐시 초기화됨. 다시 불러오는 중...</td></tr>';
        await fetchVideos();
    } catch (e) {
        console.error('clear cache error', e);
    }
});

// 통계 카드 토글
if (toggleStatsChip && statsGrid) {
    toggleStatsChip.addEventListener('click', () => {
        statsGrid.classList.toggle('hidden');
    });
}

// 페이지당 개수 변경
// 페이지 크기 UI 제거됨

// 페이지네이션 클릭 핸들러
document.addEventListener('click', (e) => {
    // 페이지네이션
    const btn = e.target.closest('.page-btn');
    if (btn) {
        const p = Number(btn.getAttribute('data-page'));
        if (!isFinite(p) || p < 1) return;
        currentPage = p;
        renderCurrentView();
        renderPagination();
        window.scrollTo({ top: 0, behavior: 'smooth' });
        return;
    }
    // 채널 드롭다운 토글
    const toggleCell = e.target.closest('.toggle-channel');
    if (toggleCell) {
        const tr = toggleCell.closest('.channel-row');
        const channel = tr?.getAttribute('data-channel');
        if (!channel) return;
        if (expandedChannels.has(channel)) expandedChannels.delete(channel); else expandedChannels.add(channel);
        renderCurrentView();
        renderPagination();
        return;
    }
    // 대표 영상 버튼: 바로 상세 페이지 열기
    const openBtn = e.target.closest('.open-details');
    if (openBtn) {
        const repId = openBtn.getAttribute('data-rep');
        if (repId) {
            window.open(`details.html?id=${encodeURIComponent(repId)}`, '_blank');
        }
        return;
    }
});

// 구독자 필터 이벤트 바인딩
subsChips.forEach(btn => {
    btn.addEventListener('click', () => {
        setActiveChip(subsChips, btn);
        subsFilter.preset = btn.getAttribute('data-subs') || 'all';
        subsFilter.min = null; subsFilter.max = null;
        currentPage = 1;
        // 현재 뷰 모드 유지한 채 재렌더
        filterAndRender(true);
    });
});
if (subsApplyBtn) {
    subsApplyBtn.addEventListener('click', () => {
        subsFilter.preset = 'custom';
        const min = Number(subsMinInput?.value || '');
        const max = Number(subsMaxInput?.value || '');
        subsFilter.min = Number.isFinite(min) ? min : null;
        subsFilter.max = Number.isFinite(max) ? max : null;
        currentPage = 1;
        filterAndRender(true);
    });
}
if (subsResetBtn) {
    subsResetBtn.addEventListener('click', () => {
        subsFilter = { preset: 'all', min: null, max: null };
        if (subsMinInput) subsMinInput.value = '';
        if (subsMaxInput) subsMaxInput.value = '';
        subsChips.forEach(ch => ch.classList.remove('chip-active'));
        currentPage = 1;
        filterAndRender(true);
    });
}
