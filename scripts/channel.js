import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, collection, getDocs, query, where, orderBy } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

const tableEl = document.getElementById('channel-video-table');
const chanTitle = document.getElementById('channel-title');
const searchInput = document.getElementById('chan-search');
const sortUpBtn = document.getElementById('chan-sort-up-btn');
const sortDownBtn = document.getElementById('chan-sort-down-btn');
const lastUpdatedEl = document.getElementById('channel-last-updated');

function getChannelName() {
    const usp = new URLSearchParams(location.search);
    return usp.get('name') || '';
}

function parseNum(v) {
    if (typeof v === 'number') return v;
    const s = String(v || '');
    const digits = s.replace(/[^0-9]/g, '');
    return digits ? Number(digits) : 0;
}

function computeChangePct(doc) {
    const curr = parseNum(doc.views_numeric || doc.views || 0);
    const prev = parseNum(doc.views_prev_numeric || doc.views_baseline_numeric || doc.views || 0) || curr;
    if (!prev) return 0;
    return ((curr - prev) / prev) * 100;
}

function fmt(n) {
    try { return Number(n || 0).toLocaleString(); } catch { return String(n); }
}

let allVideos = [];
let currentSort = 'up';

function renderTable(rows) {
    if (!rows.length) { tableEl.innerHTML = '<p class="info-message">표시할 데이터가 없습니다.</p>'; return; }
    const html = `
    <table class="data-table">
      <thead>
        <tr>
          <th>#</th><th>썸네일</th><th>제목</th>
          <th>현재 조회수</th><th>이전</th><th>변화율</th><th>업데이트</th><th>링크</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((r, idx) => `
          <tr>
            <td>${idx + 1}</td>
            <td><img src="${r.thumbnail || ''}" class="table-thumbnail"/></td>
            <td class="table-title"><a href="details.html?id=${r.id}" target="_blank">${r.title || ''}</a></td>
            <td>${fmt(r.views_numeric)}</td>
            <td>${fmt(r.views_prev_numeric || r.views_baseline_numeric)}</td>
            <td style="${r._pct >= 0 ? 'color:#16a34a;' : 'color:#dc2626;'}">${(r._pct >= 0 ? '+' : '') + r._pct.toFixed(2)}%</td>
            <td>${r.views_last_checked_at ? new Date(r.views_last_checked_at).toLocaleString() : '-'}</td>
            <td><a class="btn btn-details" href="${r.youtube_url || '#'}" target="_blank">YouTube</a></td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
    tableEl.innerHTML = html;
}

async function loadChannelVideos() {
    const channel = getChannelName();
    chanTitle.textContent = `${channel} — 채널 상세`;
    tableEl.innerHTML = '<p class="info-message">영상을 불러오는 중...</p>';
    // Firestore에서 where("channel","==",channel) + orderBy('date','desc')
    // 복합 인덱스 없을 수 있으므로 우선 channel만 where, 클라이언트 정렬
    const qCol = query(collection(db, 'videos'), where('channel', '==', channel));
    const snap = await getDocs(qCol);
    allVideos = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    allVideos.forEach(v => { v._pct = computeChangePct(v); });
    applyFiltersAndSort();
    const maxTs = Math.max(...allVideos.map(r => Number(r.views_last_checked_at || 0)).filter(Boolean), 0);
    if (lastUpdatedEl) lastUpdatedEl.textContent = maxTs ? `마지막 업데이트: ${new Date(maxTs).toLocaleString()}` : '';
}

function applyFiltersAndSort() {
    const term = (searchInput?.value || '').toLowerCase();
    let rows = allVideos;
    if (term) rows = rows.filter(r => (r.title || '').toLowerCase().includes(term));
    rows = rows.sort((a,b) => currentSort === 'up' ? (b._pct - a._pct) : (a._pct - b._pct));
    renderTable(rows);
}

searchInput?.addEventListener('input', applyFiltersAndSort);
sortUpBtn?.addEventListener('click', () => { currentSort = 'up'; applyFiltersAndSort(); });
sortDownBtn?.addEventListener('click', () => { currentSort = 'down'; applyFiltersAndSort(); });

loadChannelVideos();


