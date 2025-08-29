import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, collection, getDocs, query, orderBy } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

const tableContainer = document.getElementById('video-table-container');
const searchInput = document.getElementById('rank-search');
const sortUpBtn = document.getElementById('sort-up-btn');
const sortDownBtn = document.getElementById('sort-down-btn');
const lastUpdatedEl = document.getElementById('last-updated');

// Channel board elements
const channelTableContainer = document.getElementById('channel-table-container');
const channelSearchInput = document.getElementById('channel-search');
const channelSortAbsBtn = document.getElementById('channel-sort-abs-btn');
const channelSortPctBtn = document.getElementById('channel-sort-pct-btn');

let allRows = [];
let currentSort = 'up';
let channelSort = 'abs';

function computeChangePct(doc) {
    const parseNum = (v) => {
        if (typeof v === 'number') return v;
        const s = String(v || '');
        const digits = s.replace(/[^0-9]/g, '');
        return digits ? Number(digits) : 0;
    };
    const curr = parseNum(doc.views_numeric || doc.views || 0);
    const prev = parseNum(doc.views_prev_numeric || doc.views_baseline_numeric || doc.views || 0) || curr;
    if (!prev) return 0;
    return ((curr - prev) / prev) * 100;
}

// 파싱 일관성: 채널 집계에서도 동일한 숫자 파싱 규칙 사용
function parseCount(v) {
    if (typeof v === 'number') return v;
    const s = String(v || '');
    const digits = s.replace(/[^0-9]/g, '');
    return digits ? Number(digits) : 0;
}

function fmt(n) {
    try { return Number(n || 0).toLocaleString(); } catch { return String(n); }
}

function renderTable(rows) {
    if (!rows.length) { tableContainer.innerHTML = '<p class="info-message">표시할 데이터가 없습니다.</p>'; return; }
    const html = `
    <table class="data-table">
      <thead>
        <tr>
          <th>#</th><th>썸네일</th><th>제목</th><th>채널</th>
          <th>현재 조회수</th><th>이전</th><th>변화율</th><th>업데이트</th><th>링크</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((r, idx) => `
          <tr>
            <td>${idx + 1}</td>
            <td><img src="${r.thumbnail || ''}" class="table-thumbnail"/></td>
            <td class="table-title"><a href="details.html?id=${r.id}" target="_blank">${r.title || ''}</a></td>
            <td>${r.channel || ''}</td>
            <td>${fmt(r.views_numeric)}</td>
            <td>${fmt(r.views_prev_numeric || r.views_baseline_numeric)}</td>
            <td style="${r._pct >= 0 ? 'color:#16a34a;' : 'color:#dc2626;'}">${(r._pct >= 0 ? '+' : '') + r._pct.toFixed(2)}%</td>
            <td>${r.views_last_checked_at ? new Date(r.views_last_checked_at).toLocaleString() : '-'}</td>
            <td><a class="btn btn-details" href="${r.youtube_url || '#'}" target="_blank">YouTube</a></td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
    tableContainer.innerHTML = html;
}

async function loadData() {
    tableContainer.innerHTML = '<p class="info-message">랭킹을 불러오는 중...</p>';
    const q = query(collection(db, 'videos'), orderBy('date', 'desc'));
    const snap = await getDocs(q);
    allRows = snap.docs.map(d => {
        const data = { id: d.id, ...d.data() };
        data._pct = computeChangePct(data);
        return data;
    });
    applyFiltersAndSort();
    renderChannelBoard();
    const maxTs = Math.max(...allRows.map(r => Number(r.views_last_checked_at || 0)).filter(Boolean), 0);
    if (lastUpdatedEl) lastUpdatedEl.textContent = maxTs ? `마지막 업데이트: ${new Date(maxTs).toLocaleString()}` : '';
}

function applyFiltersAndSort() {
    const term = (searchInput?.value || '').toLowerCase();
    let rows = allRows;
    if (term) rows = rows.filter(r => (r.title || '').toLowerCase().includes(term) || (r.channel || '').toLowerCase().includes(term));
    rows = rows.sort((a,b) => currentSort === 'up' ? (b._pct - a._pct) : (a._pct - b._pct));
    renderTable(rows);
}

searchInput?.addEventListener('input', applyFiltersAndSort);
sortUpBtn?.addEventListener('click', () => { currentSort = 'up'; applyFiltersAndSort(); });
sortDownBtn?.addEventListener('click', () => { currentSort = 'down'; applyFiltersAndSort(); });

loadData();

// ---------------- Tabs handling (shared header styles already in CSS) ----------------
document.querySelector('.tabs')?.addEventListener('click', (e) => {
    if (!e.target.classList.contains('tab-link')) return;
    const tabId = e.target.getAttribute('data-tab');
    document.querySelectorAll('.tab-link').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    e.target.classList.add('active');
    document.getElementById(tabId)?.classList.add('active');
});

// ---------------- Channel-level ranking board ----------------
function groupByChannel(rows) {
    const map = new Map();
    for (const r of rows) {
        const channel = (r.channel || '').trim() || 'Unknown';
        if (!map.has(channel)) {
            map.set(channel, { 
                channel,
                videos: [],
                // 총 상승 조회수 (현재-기준) 합산
                totalRiseAbs: 0,
                // 총 기준 대비 평균 증가율(가중치: 영상수 기준 단순 평균)
                avgRisePct: 0,
                // 대표 썸네일/영상: 최근 영상 기준 1개
                representative: null
            });
        }
        const bucket = map.get(channel);
        const curr = parseCount(r.views_numeric || r.views || 0) || 0;
        const prev = parseCount(r.views_prev_numeric || r.views_baseline_numeric || r.views || 0) || curr;
        const riseAbs = Math.max(0, curr - prev);
        const risePct = prev > 0 ? (curr - prev) / prev : (r._pct || 0) / 100;
        bucket.videos.push(r);
        bucket.totalRiseAbs += riseAbs;
        // 평균 증가율은 나중에 계산
        // 대표 영상은 최신 날짜 우선
        if (!bucket.representative) bucket.representative = r;
        else {
            const d1 = r.date ? new Date(r.date).getTime() : 0;
            const d2 = bucket.representative.date ? new Date(bucket.representative.date).getTime() : 0;
            if (d1 > d2) bucket.representative = r;
        }
    }
    // 평균 증가율 계산
    for (const bucket of map.values()) {
        const pcts = bucket.videos.map(v => v._pct || computeChangePct(v));
        if (pcts.length) bucket.avgRisePct = pcts.reduce((a,b)=>a+b,0) / pcts.length;
    }
    return Array.from(map.values());
}

function renderChannelTable(rows) {
    if (!channelTableContainer) return;
    if (!rows.length) { channelTableContainer.innerHTML = '<p class="info-message">표시할 데이터가 없습니다.</p>'; return; }
    const html = `
    <table class="data-table">
      <thead>
        <tr>
          <th>#</th><th>대표 썸네일</th><th>채널</th><th>영상 수</th>
          <th>총 상승 조회수</th><th>평균 증가율</th><th>대표 영상</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((r, idx) => `
          <tr>
            <td>${idx + 1}</td>
            <td><img src="${r.representative?.thumbnail || ''}" class="table-thumbnail"/></td>
            <td class="table-title">${r.channel}</td>
            <td>${r.videos.length}</td>
            <td>${Number(r.totalRiseAbs || 0).toLocaleString()}</td>
            <td style="color:${r.avgRisePct>=0? '#16a34a':'#dc2626'}">${(r.avgRisePct>=0?'+':'') + r.avgRisePct.toFixed(2)}%</td>
            <td>${r.representative ? `<a class="btn btn-details" href="details.html?id=${r.representative.id}" target="_blank">자세히</a>` : '-'}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
    channelTableContainer.innerHTML = html;
}

function renderChannelBoard() {
    const term = (channelSearchInput?.value || '').toLowerCase();
    let rows = groupByChannel(allRows);
    if (term) rows = rows.filter(r => r.channel.toLowerCase().includes(term));
    rows.sort((a, b) => {
        if (channelSort === 'pct') return (b.avgRisePct - a.avgRisePct) || (b.totalRiseAbs - a.totalRiseAbs);
        return (b.totalRiseAbs - a.totalRiseAbs) || (b.avgRisePct - a.avgRisePct);
    });
    renderChannelTable(rows);
}

channelSearchInput?.addEventListener('input', renderChannelBoard);
channelSortAbsBtn?.addEventListener('click', () => { channelSort = 'abs'; renderChannelBoard(); });
channelSortPctBtn?.addEventListener('click', () => { channelSort = 'pct'; renderChannelBoard(); });


