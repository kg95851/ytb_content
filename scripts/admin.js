import { supabase } from '../supabase-client.js';

// DOM refs
const loginView = document.getElementById('login-view');
const adminPanel = document.getElementById('admin-panel');
const logoutBtn = document.getElementById('logout-btn');
const tabs = document.querySelector('.tabs');
const tabLinks = document.querySelectorAll('.tab-link');
const tabContents = document.querySelectorAll('.tab-content');

// Data management
const dataTableContainer = document.getElementById('data-table-container');
const adminPaginationContainer = document.getElementById('admin-pagination-container');
const dataSearchInput = document.getElementById('data-search-input');
const adminUpdateDateFilter = document.getElementById('admin-update-date-filter');
const adminSortSelect = document.getElementById('admin-sort-select');
const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
const runAnalysisSelectedBtn = document.getElementById('run-analysis-selected-btn');
const runAnalysisAllBtn = document.getElementById('run-analysis-all-btn');
const analysisStatus = document.getElementById('analysis-status');
const youtubeStatus = document.getElementById('youtube-status');
const commentCountInput = document.getElementById('comment-count-input');
const runCommentsSelectedBtn = document.getElementById('run-comments-selected-btn');
const ytTranscriptSelectedBtn = document.getElementById('yt-transcript-selected-btn');
const ytViewsSelectedBtn = document.getElementById('yt-views-selected-btn');
const ytTranscriptAllBtn = document.getElementById('yt-transcript-all-btn');
const ytViewsAllBtn = document.getElementById('yt-views-all-btn');

// Upload
const fileDropArea = document.getElementById('file-drop-area');
const fileInput = document.getElementById('file-input');
const fileNameDisplay = document.getElementById('file-name-display');
const uploadBtn = document.getElementById('upload-btn');
const uploadStatus = document.getElementById('upload-status');

// Settings (Gemini/transcript)
const geminiKeyInput = document.getElementById('gemini-api-key');
const saveGeminiKeyBtn = document.getElementById('save-gemini-key-btn');
const testGeminiKeyBtn = document.getElementById('test-gemini-key-btn');
const geminiKeyStatus = document.getElementById('gemini-key-status');
const transcriptServerInput = document.getElementById('transcript-server-url');
const saveTranscriptServerBtn = document.getElementById('save-transcript-server-btn');
const transcriptServerStatus = document.getElementById('transcript-server-status');

// Settings (YouTube keys)
const ytKeysTextarea = document.getElementById('youtube-api-keys');
const ytKeysSaveBtn = document.getElementById('save-youtube-keys-btn');
const ytKeysTestBtn = document.getElementById('test-youtube-keys-btn');
const ytKeysStatus = document.getElementById('youtube-keys-status');

// Schedule
const scheduleCreateBtn = document.getElementById('schedule-create-btn');
const scheduleRankingBtn = document.getElementById('schedule-ranking-btn');
const rankingRefreshNowBtn = document.getElementById('ranking-refresh-now-btn');
const scheduleCreateStatus = document.getElementById('schedule-create-status');
const scheduleTimeInput = document.getElementById('schedule-time');
const schedulesTableContainer = document.getElementById('schedules-table-container');
const schedulesBulkDeleteBtn = document.getElementById('schedules-bulk-delete-btn');
const scheduleLogEl = document.getElementById('schedule-log');

// Analysis banner
const analysisBanner = document.getElementById('analysis-banner');
const analysisBannerText = document.getElementById('analysis-banner-text');
const analysisProgressBar = document.getElementById('analysis-progress-bar');
const analysisLogEl = document.getElementById('analysis-log');

// Export JSON
const exportJsonBtn = document.getElementById('export-json-btn');
const exportStatus = document.getElementById('export-status');
// Concurrency inputs
const ytTranscriptConcInput = document.getElementById('yt-transcript-conc');
const ytViewsConcInput = document.getElementById('yt-views-conc');
// Options
const ytTranscriptOnlyMissing = document.getElementById('yt-transcript-only-missing');
const ytViewsOnlyMissing = document.getElementById('yt-views-only-missing');
const ytViewsExcludeMin = document.getElementById('yt-views-exclude-min');
const youtubeLogEl = document.getElementById('youtube-log');

function ylog(line) {
  if (!youtubeLogEl) return;
  const t = new Date().toLocaleTimeString();
  youtubeLogEl.textContent += `[${t}] ${line}\n`;
  youtubeLogEl.scrollTop = youtubeLogEl.scrollHeight;
}

let currentData = [];
let adminCurrentPage = 1;
const ADMIN_PAGE_SIZE = 200;
let adminSortMode = 'update_desc';
let selectedFile = null;
let docIdToEdit = null;
let isBulkDelete = false;

// --------- Admin cache (ETag-like) ---------
const ADMIN_IDB_DB = 'adminVideosCacheDB';
const ADMIN_IDB_STORE = 'kv';
const ADMIN_CACHE_KEY = 'videosCompressed';

async function adminIdbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(ADMIN_IDB_DB, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(ADMIN_IDB_STORE)) db.createObjectStore(ADMIN_IDB_STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function adminIdbGet(key) {
  try {
    const db = await adminIdbOpen();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(ADMIN_IDB_STORE, 'readonly');
      const store = tx.objectStore(ADMIN_IDB_STORE);
      const r = store.get(key);
      r.onsuccess = () => resolve(r.result || null);
      r.onerror = () => reject(r.error);
    });
  } catch { return null; }
}

async function adminIdbSet(key, value) {
  try {
    const db = await adminIdbOpen();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(ADMIN_IDB_STORE, 'readwrite');
      const store = tx.objectStore(ADMIN_IDB_STORE);
      const r = store.put(value, key);
      r.onsuccess = () => resolve(true);
      r.onerror = () => reject(r.error);
    });
  } catch { return false; }
}

async function adminCompressJSON(data) {
  const text = JSON.stringify(data);
  if ('CompressionStream' in window) {
    const cs = new CompressionStream('gzip');
    const blob = new Blob([text]);
    const stream = blob.stream().pipeThrough(cs);
    const buffer = await new Response(stream).arrayBuffer();
    return { algo: 'gzip', buffer };
  }
  return { algo: 'none', text };
}

async function adminDecompressJSON(record) {
  if (!record) return null;
  if (record.algo === 'gzip' && 'DecompressionStream' in window) {
    const ds = new DecompressionStream('gzip');
    const stream = new Blob([record.buffer]).stream().pipeThrough(ds);
    const text = await new Response(stream).text();
    return JSON.parse(text);
  }
  if (record.text) return JSON.parse(record.text);
  return null;
}

async function getAdminCache() {
  const rec = await adminIdbGet(ADMIN_CACHE_KEY);
  if (!rec) return null;
  try {
    const payload = await adminDecompressJSON(rec.payload);
    return { version: rec.version, data: payload };
  } catch { return null; }
}

async function setAdminCache(version, data) {
  const payload = await adminCompressJSON(data);
  await adminIdbSet(ADMIN_CACHE_KEY, { version, payload, savedAt: Date.now() });
}

function computeVersionFromData(rows) {
  const total = Array.isArray(rows) ? rows.length : 0;
  const maxTs = Math.max(...(rows || []).map(r => Number(r.last_modified || 0)).filter(Boolean), 0);
  return `${total}:${maxTs}`;
}

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

// ---------- Auth (Supabase) ----------
const loginDebug = document.getElementById('login-debug');

async function refreshAuthUI() {
  const { data: { session } } = await supabase.auth.getSession();
  if (session) {
        loginView.classList.add('hidden');
        adminPanel.classList.remove('hidden');
        fetchAndDisplayData();
    refreshSchedulesUI();
    } else {
        loginView.classList.remove('hidden');
        adminPanel.classList.add('hidden');
    }
}

document.getElementById('login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
  geminiKeyStatus && (geminiKeyStatus.textContent = '');
  try {
    if (loginDebug) { loginDebug.style.display='block'; loginDebug.textContent = '';
      try {
        let hasUrl = false; let hasAnon = false;
        try { hasUrl = !!(import.meta && import.meta.env && import.meta.env.VITE_SUPABASE_URL); } catch {}
        try { hasAnon = !!(import.meta && import.meta.env && import.meta.env.VITE_SUPABASE_ANON_KEY); } catch {}
        loginDebug.textContent += `[client] URL: ${hasUrl ? 'OK' : 'MISSING'}\n`;
        loginDebug.textContent += `[client] ANON: ${hasAnon ? 'OK' : 'MISSING'}\n`;
      } catch {}
      try { const probe = await supabase.auth.getSession(); loginDebug.textContent += `[probe] session: ${probe?.data?.session ? 'present' : 'none'}\n`; } catch {}
    }
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) throw error;
    if (loginDebug) loginDebug.textContent += `[signin] user: ${data?.user?.id || 'none'}\n`;
    // 세션 전파 폴링(100~300ms 대기, 최대 3회)
    for (let i = 0; i < 3; i++) {
      await new Promise(r => setTimeout(r, 120));
      try {
        const probe2 = await supabase.auth.getSession();
        if (loginDebug) loginDebug.textContent += `[probe${i+1}] session: ${probe2?.data?.session ? 'present' : 'none'}\n`;
        if (probe2?.data?.session) break;
      } catch {}
    }
  } catch (err) {
    const msg = (err?.message || err || '').toString();
    document.getElementById('login-error').textContent = '로그인 실패: ' + msg;
    if (loginDebug) loginDebug.textContent += `[error] ${msg}\n`;
  }
  await refreshAuthUI();
});

logoutBtn.addEventListener('click', async () => {
  await supabase.auth.signOut();
  await refreshAuthUI();
});

supabase.auth.onAuthStateChange((e, s) => {
  const d = document.getElementById('login-debug');
  if (d) { d.style.display = 'block'; d.textContent += `[auth] ${e} session=${!!s?.session}\n`; }
  refreshAuthUI();
});
window.addEventListener('DOMContentLoaded', () => {
  // 기본 예약 시간: +30분
  if (scheduleTimeInput) {
    const now = new Date(); now.setMinutes(now.getMinutes() + 30);
    scheduleTimeInput.value = formatDateTimeLocal(now);
  }
  restoreLocalSettings();
  refreshAuthUI();
});

// ---------- Tabs ----------
tabs.addEventListener('click', (e) => {
  if (!e.target.classList.contains('tab-link')) return;
        const tabId = e.target.getAttribute('data-tab');
  tabLinks.forEach(l => l.classList.remove('active'));
  tabContents.forEach(c => c.classList.remove('active'));
        e.target.classList.add('active');
        document.getElementById(tabId).classList.add('active');
});

// ---------- CRUD: Read/Render ----------
async function fetchAndDisplayData() {
  dataTableContainer.innerHTML = '<p class="info-message">데이터 로딩...</p>';
  try {
    // 0) 캐시 버전 확인 및 조건부 로딩
    const remoteVer = await fetchDatasetVersion();
    const cached = await getAdminCache();
    if (cached && cached.version === remoteVer.tag) {
      currentData = cached.data || [];
      adminCurrentPage = 1;
        renderTable(currentData);
      renderAdminPagination();
      return;
    }

    // 전체 카운트
    let total = remoteVer.total || 0;

    const BATCH = 1000;
    const CONC = 4;
    const ranges = [];
    if (total > 0) {
      for (let start = 0; start < total; start += BATCH) {
        ranges.push([start, Math.min(start + BATCH - 1, total - 1)]);
      }
    } else {
      // 총 개수를 못 가져오면 until-exhaust 페치
      ranges.push([0, BATCH - 1]);
    }

    const results = [];
    for (let i = 0; i < ranges.length; i += CONC) {
      const slice = ranges.slice(i, i + CONC);
      const chunk = await Promise.all(slice.map(async ([from, to]) => {
        const { data, error } = await supabase
          .from('videos')
          .select('*')
          .order('date', { ascending: false })
          .range(from, to);
        if (error) return [];
        return Array.isArray(data) ? data : [];
      }));
      chunk.forEach(arr => results.push(...arr));
      // until-exhaust: 총 개수 미확정이면서 마지막 청크가 꽉 찼으면 다음 범위를 추가
      if (!total && slice.length && (chunk[chunk.length - 1]?.length === BATCH)) {
        const lastEnd = ranges[ranges.length - 1][1];
        ranges.push([lastEnd + 1, lastEnd + BATCH]);
      }
    }

    // 중복 제거
    const map = new Map();
    for (const r of results) { if (r && r.id) map.set(r.id, r); }
    currentData = Array.from(map.values());
    // 캐시 저장: 총개수/최신 last_modified 기반 버전
    const version = remoteVer.tag || computeVersionFromData(currentData);
    await setAdminCache(version, currentData);
    adminCurrentPage = 1;
    renderTable(currentData);
    renderAdminPagination();
  } catch (e) {
    console.error('fetch error', e);
    dataTableContainer.innerHTML = '<p class="error-message">불러오기 실패</p>';
  }
}

function renderTable(rows) {
  if (!rows?.length) {
        dataTableContainer.innerHTML = '<p class="info-message">표시할 데이터가 없습니다.</p>';
        return;
    }
  // 정렬 적용
  let sorted = rows.slice();
  const getUpdateTs = (v) => { try { return v.update_date ? new Date(v.update_date).getTime() : 0; } catch { return 0; } };
  if (adminSortMode === 'update_desc') sorted.sort((a,b) => getUpdateTs(b) - getUpdateTs(a));
  else if (adminSortMode === 'date_desc') sorted.sort((a,b) => {
    const da = a.date ? new Date(a.date).getTime() : 0; const db = b.date ? new Date(b.date).getTime() : 0; return db - da;
  });
  else if (adminSortMode === 'title_asc') sorted.sort((a,b) => String(a.title||'').localeCompare(String(b.title||'')));
  else if (adminSortMode === 'channel_asc') sorted.sort((a,b) => String(a.channel||'').localeCompare(String(b.channel||'')));
    const table = document.createElement('table');
    table.className = 'data-table';
  // 페이지 슬라이스
  const startIndex = (adminCurrentPage - 1) * ADMIN_PAGE_SIZE;
  const endIndex = startIndex + ADMIN_PAGE_SIZE;
  const pageRows = sorted.slice(startIndex, endIndex);

    table.innerHTML = `
        <thead>
            <tr>
        <th><input type="checkbox" id="select-all-checkbox" /></th>
        <th>썸네일</th><th>제목</th><th>채널</th><th>게시일</th><th>업데이트</th><th>상태</th><th>관리</th>
            </tr>
        </thead>
        <tbody>
      ${pageRows.map(v => `
        <tr data-id="${v.id}">
          <td><input type="checkbox" class="row-checkbox" data-id="${v.id}"></td>
          <td>${v.thumbnail ? `<img class="table-thumbnail" src="${v.thumbnail}">` : ''}</td>
          <td class="table-title">${escapeHtml(v.title || '')}</td>
          <td>${escapeHtml(v.channel || '')}</td>
          <td>${escapeHtml(v.date || '')}</td>
          <td>${escapeHtml(v.update_date || '')}</td>
          <td>${Array.isArray(v.dopamine_graph) && v.dopamine_graph.length ? '<span class="group-tag" style="background:#10b981;">Graph</span>' : ''}</td>
                    <td class="action-buttons">
            <button class="btn btn-edit" data-id="${v.id}">수정</button>
            <button class="btn btn-danger single-delete-btn" data-id="${v.id}">삭제</button>
                    </td>
                </tr>
            `).join('')}
    </tbody>`;
    dataTableContainer.innerHTML = '';
    dataTableContainer.appendChild(table);
  const selectAll = document.getElementById('select-all-checkbox');
  if (selectAll) selectAll.addEventListener('change', (e) => {
    document.querySelectorAll('.row-checkbox').forEach(cb => { cb.checked = e.target.checked; });
  });
}

function renderAdminPagination() {
  if (!adminPaginationContainer) return;
  const totalPages = Math.max(1, Math.ceil(currentData.length / ADMIN_PAGE_SIZE));
  if (totalPages <= 1) { adminPaginationContainer.innerHTML = ''; return; }
  const makeBtn = (p) => `<button class="page-btn ${p===adminCurrentPage?'active':''}" data-admin-page="${p}">${p}</button>`;
  const maxShow = 9;
  let start = Math.max(1, adminCurrentPage - Math.floor(maxShow/2));
  let end = Math.min(totalPages, start + maxShow - 1);
  if (end - start + 1 < maxShow) start = Math.max(1, end - maxShow + 1);
  const parts = [];
  if (adminCurrentPage > 1) parts.push(`<button class="page-btn" data-admin-page="${adminCurrentPage-1}">이전</button>`);
  if (start > 1) parts.push(makeBtn(1));
  if (start > 2) parts.push('<span style="color:var(--text-secondary);padding:4px 6px;">...</span>');
  for (let p = start; p <= end; p++) parts.push(makeBtn(p));
  if (end < totalPages - 1) parts.push('<span style="color:var(--text-secondary);padding:4px 6px;">...</span>');
  if (end < totalPages) parts.push(makeBtn(totalPages));
  if (adminCurrentPage < totalPages) parts.push(`<button class="page-btn" data-admin-page="${adminCurrentPage+1}">다음</button>`);
  adminPaginationContainer.innerHTML = parts.join('');
}

dataTableContainer.addEventListener('click', (e) => {
  const btnEdit = e.target.closest('.btn-edit');
  const btnDel = e.target.closest('.single-delete-btn');
  if (btnEdit) openEditModal(btnEdit.getAttribute('data-id'));
  if (btnDel) openConfirmModal(btnDel.getAttribute('data-id'), false);
});

// 페이지네이션 버튼 클릭
document.addEventListener('click', (e) => {
  const pageBtn = e.target.closest('.page-btn');
  if (!pageBtn) return;
  const p = Number(pageBtn.getAttribute('data-admin-page'));
  if (!isFinite(p)) return;
  adminCurrentPage = p;
  const query = String(dataSearchInput?.value || '').toLowerCase();
  const upd = String(adminUpdateDateFilter?.value || '');
  let rows = query ? currentData.filter(v => (v.title || '').toLowerCase().includes(query) || (v.channel || '').toLowerCase().includes(query)) : currentData;
  if (upd) rows = rows.filter(v => v.update_date && v.update_date.slice(0,10) === upd);
  renderTable(rows);
  renderAdminPagination();
  window.scrollTo({ top: 0, behavior: 'smooth' });
});

dataSearchInput?.addEventListener('input', (e) => {
  const t = String(e.target.value || '').toLowerCase();
  const upd = String(adminUpdateDateFilter?.value || '');
  let filtered = currentData.filter(v => (v.title || '').toLowerCase().includes(t) || (v.channel || '').toLowerCase().includes(t));
  if (upd) filtered = filtered.filter(v => v.update_date && v.update_date.slice(0,10) === upd);
  adminCurrentPage = 1;
  renderTable(filtered);
  renderAdminPagination();
});
adminUpdateDateFilter?.addEventListener('change', () => {
  const upd = String(adminUpdateDateFilter.value || '');
  const t = String(dataSearchInput?.value || '').toLowerCase();
  let rows = t ? currentData.filter(v => (v.title || '').toLowerCase().includes(t) || (v.channel || '').toLowerCase().includes(t)) : currentData;
  if (upd) rows = rows.filter(v => v.update_date && v.update_date.slice(0,10) === upd);
  adminCurrentPage = 1;
  renderTable(rows);
  renderAdminPagination();
});

adminSortSelect?.addEventListener('change', () => {
  adminSortMode = adminSortSelect.value || 'update_desc';
  adminCurrentPage = 1;
  const query = String(dataSearchInput?.value || '').toLowerCase();
  const rows = query ? currentData.filter(v => (v.title || '').toLowerCase().includes(query) || (v.channel || '').toLowerCase().includes(query)) : currentData;
  renderTable(rows);
  renderAdminPagination();
});

// ---------- CRUD: Edit ----------
const editModal = document.getElementById('edit-modal');
const editForm = document.getElementById('edit-form');
const saveEditBtn = document.getElementById('save-edit-btn');
const cancelEditBtn = document.getElementById('cancel-edit-btn');
const closeEditModalBtn = document.getElementById('close-edit-modal-btn');

async function openEditModal(id) {
    docIdToEdit = id;
  const { data, error } = await supabase.from('videos').select('*').eq('id', id).single();
  if (error || !data) return;
  const obj = data;
        editForm.innerHTML = '';
  Object.keys(obj).sort().forEach((key) => {
    const raw = obj[key];
            const isObject = raw && typeof raw === 'object';
            const value = isObject ? JSON.stringify(raw, null, 2) : (raw ?? '');
            const isLong = String(value).length > 100 || isObject;
            editForm.innerHTML += `
                <div class="form-group">
        <label for="edit-${key}">${escapeHtml(key)}</label>
                    ${isLong
          ? `<textarea id="edit-${key}" name="${escapeHtml(key)}" style="min-height:120px;">${escapeHtml(String(value))}</textarea>`
          : `<input type="text" id="edit-${key}" name="${escapeHtml(key)}" value="${escapeHtml(String(value))}">`}
      </div>`;
        });
        editModal.classList.remove('hidden');
    }

function closeEditModal() { editModal.classList.add('hidden'); }
cancelEditBtn.addEventListener('click', closeEditModal);
closeEditModalBtn.addEventListener('click', closeEditModal);

saveEditBtn.addEventListener('click', async () => {
  const updated = {};
    new FormData(editForm).forEach((value, key) => {
    try { updated[key] = (/^\s*\[|\{/.test(String(value))) ? JSON.parse(value) : value; }
    catch { updated[key] = value; }
  });
  await supabase.from('videos').update(updated).eq('id', docIdToEdit);
    closeEditModal();
    fetchAndDisplayData();
});

// ---------- CRUD: Delete ----------
const confirmModal = document.getElementById('confirm-modal');
const confirmModalTitle = document.getElementById('confirm-modal-title');
const confirmModalMessage = document.getElementById('confirm-modal-message');
const confirmDeleteBtn = document.getElementById('confirm-delete-btn');
const cancelDeleteBtn = document.getElementById('cancel-delete-btn');

function openConfirmModal(id, bulk) {
  isBulkDelete = !!bulk;
  if (isBulkDelete) {
        confirmModalTitle.textContent = '선택 삭제 확인';
    confirmModalMessage.textContent = '선택된 항목들을 삭제하시겠습니까?';
    } else {
    docIdToEdit = id;
        confirmModalTitle.textContent = '삭제 확인';
        confirmModalMessage.textContent = '정말로 삭제하시겠습니까?';
    }
    confirmModal.classList.remove('hidden');
}
function closeConfirmModal() { confirmModal.classList.add('hidden'); }
cancelDeleteBtn.addEventListener('click', closeConfirmModal);

confirmDeleteBtn.addEventListener('click', async () => {
    if (isBulkDelete) {
    const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
    if (ids.length) await supabase.from('videos').delete().in('id', ids);
    } else {
    await supabase.from('videos').delete().eq('id', docIdToEdit);
    }
    closeConfirmModal();
    fetchAndDisplayData();
});

bulkDeleteBtn.addEventListener('click', () => {
  const anyChecked = document.querySelector('.row-checkbox:checked');
  if (!anyChecked) { alert('삭제할 항목을 선택하세요.'); return; }
        openConfirmModal(null, true);
});

// ---------- Upload ----------
function handleFile(file) {
  if (!file) return;
  const ext = (file.name.split('.').pop() || '').toLowerCase();
  if (!['csv', 'xlsx'].includes(ext)) {
    alert('CSV 또는 XLSX 파일만 지원됩니다.');
    return;
  }
  selectedFile = file;
  fileNameDisplay.textContent = `선택된 파일: ${file.name}`;
  fileNameDisplay.classList.add('active');
}

fileInput.addEventListener('change', () => handleFile(fileInput.files[0]));
['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => fileDropArea.addEventListener(evt, (e) => { e.preventDefault(); e.stopPropagation(); }));
['dragenter', 'dragover'].forEach(evt => fileDropArea.addEventListener(evt, () => fileDropArea.classList.add('dragover')));
['dragleave', 'drop'].forEach(evt => fileDropArea.addEventListener(evt, () => fileDropArea.classList.remove('dragover')));
fileDropArea.addEventListener('drop', (e) => handleFile(e.dataTransfer.files[0]));

uploadBtn.addEventListener('click', () => {
  if (!selectedFile) { uploadStatus.textContent = '파일을 선택하세요.'; uploadStatus.style.color = 'red'; return; }
  uploadStatus.textContent = '파일 처리 중...'; uploadStatus.style.color = '';
  const ext = (selectedFile.name.split('.').pop() || '').toLowerCase();
  if (ext === 'csv') {
    Papa.parse(selectedFile, { header: true, skipEmptyLines: true, complete: (res) => processDataAndUpload(res.data), error: (err) => { uploadStatus.textContent = 'CSV 파싱 오류: ' + err.message; uploadStatus.style.color='red'; } });
    } else {
    const reader = new FileReader(); reader.onload = (e) => {
      try { const wb = XLSX.read(e.target.result, { type: 'array' }); const rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]); processDataAndUpload(rows); }
      catch (err) { uploadStatus.textContent = 'XLSX 파싱 오류: ' + (err?.message || err); uploadStatus.style.color='red'; }
    }; reader.readAsArrayBuffer(selectedFile);
  }
});

async function processDataAndUpload(data) {
  uploadStatus.textContent = '변경사항 분석 중...';
  // update_date 컬럼 존재 여부 확인(없으면 payload에서 제외)
  let canWriteUpdateDate = false;
  try {
    const probe = await supabase.from('videos').select('update_date').limit(0);
    canWriteUpdateDate = !probe.error;
  } catch { canWriteUpdateDate = false; }

  // 1) 입력 정규화
  const isEmptyStringValue = (v) => {
    if (v == null) return true;
    const s = String(v).trim().toLowerCase();
    return s === '' || s === '-' || s === 'n/a' || s === 'na' || s === 'null' || s === 'undefined' || s === '이미지 없음';
  };
  const isEmptyNumericValue = (v) => {
    if (v == null) return true;
    const s = String(v).trim().toLowerCase();
    if (s === '' || s === 'null' || s === 'undefined' || s === 'nan') return true;
    const digits = s.replace(/[^0-9]/g, '');
    return digits === '' || digits === '0';
  };

  const incoming = [];
  for (const row of data) {
    if (!row || typeof row !== 'object') continue;
    const normMap = new Map();
    Object.keys(row).forEach((k) => { normMap.set(String(k).trim().toLowerCase().replace(/[\s_-]+/g,'')); });
    const getCell = (variants, raw=false) => {
      for (const v of variants) {
        const keyNorm = String(v).trim().toLowerCase().replace(/[\s_-]+/g,'');
        // exact or normalized match
        for (const origKey of Object.keys(row)) {
          const ok = String(origKey).trim().toLowerCase().replace(/[\s_-]+/g,'');
          if (ok === keyNorm) {
            const val = row[origKey];
            return raw ? val : (val == null ? '' : String(val).trim());
          }
        }
      }
      return raw ? undefined : '';
    };

    const url = getCell(['YouTube URL','YouTube Url','youtube_url','youtube url','유튜브URL','url','링크']);
    const hashExplicit = getCell(['hash','Hash']);
    const title = getCell(['title','Title','제목']);
    const thumb = getCell(['thumbnail','Thumbnail','썸네일','썸네일주소','썸네일url','thumbnail url','thumb','image','image_url','이미지','이미지url']);
    const views = getCell(['views','Views','조회수','viewCount','view count']);
    const views_numeric_raw = getCell(['views_numeric','Views_numeric','조회수_numeric','조회수(숫자)','조회수수치'], true);
    const channel = getCell(['channel','Channel','채널']);
    const date = normalizeDate(getCell(['date','Date','게시일','publishedAt','publish_date']));
    const subs = getCell(['subscribers','Subscribers','구독자','subs']);
    const subs_numeric_raw = getCell(['subscribers_numeric','Subscribers_numeric','구독자_numeric','구독자(숫자)'], true);
    const update_date_raw = getCell(['update_date','Update_date','update date','Update Date','업데이트날짜','업데이트 일자','업데이트일','업데이트']);
    const group_name = getCell(['group_name','Group','그룹']);
    const template_type = getCell(['template_type','템플릿 유형','템플릿','template']);

    // 최소 식별 정보 없는 행 스킵
    const hasAny = !!(hashExplicit || url || title);
    if (!hasAny) continue;

    const computedHash = String(hashExplicit || stableHash(String(url || title || ''))).trim();
    if (!computedHash) continue;

    incoming.push({
      hash: computedHash,
      thumbnail: thumb,
      title,
      views: views ? String(views) : '',
      views_numeric: (views_numeric_raw !== undefined ? views_numeric_raw : undefined),
      channel,
      date,
      subscribers: subs,
      subscribers_numeric: (subs_numeric_raw !== undefined ? subs_numeric_raw : undefined),
      youtube_url: url,
      group_name,
      template_type,
      update_date: normalizeUpdateDate(update_date_raw)
    });
  }
  if (!incoming.length) { uploadStatus.textContent = '업로드할 유효한 행이 없습니다.'; uploadStatus.style.color = 'orange'; return; }

  // 2) 기존 데이터 조회 (hash 기준)
  const BATCH = 500; let processed = 0;
  const hashList = incoming.map(r => r.hash);
  const existingByHash = new Map();
  for (let i = 0; i < hashList.length; i += BATCH) {
    const slice = hashList.slice(i, i + BATCH);
    const selectFields = canWriteUpdateDate
      ? 'id,hash,thumbnail,title,views,views_numeric,channel,date,update_date,subscribers,subscribers_numeric,youtube_url,group_name,template_type'
      : 'id,hash,thumbnail,title,views,views_numeric,channel,date,subscribers,subscribers_numeric,youtube_url,group_name,template_type';
    const { data: rows } = await supabase
      .from('videos')
      .select(selectFields)
      .in('hash', slice);
    (rows || []).forEach(r => existingByHash.set(r.hash, r));
  }

  // 3) 삽입 대상과 "누락 채움" 업데이트 대상 분리
  const toInsert = [];
  const toUpdate = [];
  const now = Date.now();
  for (const item of incoming) {
    const exist = existingByHash.get(item.hash);
    if (!exist) {
      // 신규: 제공된 값만으로 삽입
      const payload = { hash: item.hash, last_modified: now };
      if (item.thumbnail) payload.thumbnail = item.thumbnail;
      if (item.title) payload.title = item.title;
      if (item.views) payload.views = item.views;
      if (item.views_numeric != null) payload.views_numeric = toBigIntSafe(item.views_numeric);
      if (item.channel) payload.channel = item.channel;
      if (item.date) payload.date = item.date;
      if (item.subscribers) payload.subscribers = item.subscribers;
      if (item.subscribers_numeric != null) payload.subscribers_numeric = toBigIntSafe(item.subscribers_numeric);
      if (item.youtube_url) payload.youtube_url = item.youtube_url;
      if (item.group_name) payload.group_name = item.group_name;
      if (item.template_type) payload.template_type = item.template_type;
      if (canWriteUpdateDate && item.update_date) payload.update_date = item.update_date;
      toInsert.push(payload);
    } else {
      // 기존: 누락된 필드만 채우기 (null/빈 문자열만 누락으로 간주)
      const upd = { id: exist.id };
      let has = false;
      if (item.thumbnail && isEmptyStringValue(exist.thumbnail)) { upd.thumbnail = item.thumbnail; has = true; }
      if (item.title && isEmptyStringValue(exist.title)) { upd.title = item.title; has = true; }
      if (item.views && isEmptyStringValue(exist.views)) { upd.views = item.views; has = true; }
      if (item.views_numeric != null && isEmptyNumericValue(exist.views_numeric)) { upd.views_numeric = toBigIntSafe(item.views_numeric); has = true; }
      if (item.channel && isEmptyStringValue(exist.channel)) { upd.channel = item.channel; has = true; }
      if (item.date && isEmptyStringValue(exist.date)) { upd.date = item.date; has = true; }
      if (item.subscribers && isEmptyStringValue(exist.subscribers)) { upd.subscribers = item.subscribers; has = true; }
      if (item.subscribers_numeric != null && isEmptyNumericValue(exist.subscribers_numeric)) { upd.subscribers_numeric = toBigIntSafe(item.subscribers_numeric); has = true; }
      if (item.youtube_url && isEmptyStringValue(exist.youtube_url)) { upd.youtube_url = item.youtube_url; has = true; }
      if (item.group_name && isEmptyStringValue(exist.group_name)) { upd.group_name = item.group_name; has = true; }
      if (item.template_type && isEmptyStringValue(exist.template_type)) { upd.template_type = item.template_type; has = true; }
      if (canWriteUpdateDate && item.update_date) { upd.update_date = item.update_date; has = true; }
      if (has) { upd.last_modified = now; toUpdate.push(upd); }
    }
  }

  // 4) 삽입 처리
  let inserted = 0, updated = 0;
  for (let i = 0; i < toInsert.length; i += BATCH) {
    const chunk = toInsert.slice(i, i + BATCH);
    const { error } = await supabase.from('videos').upsert(chunk, { onConflict: 'hash' });
    if (error) { uploadStatus.textContent = '삽입/업서트 실패: ' + error.message; uploadStatus.style.color='red'; return; }
    inserted += chunk.length; processed += chunk.length;
    uploadStatus.textContent = `처리 중... 삽입 ${inserted}, 업데이트 ${updated}`;
    await new Promise(r => setTimeout(r, 60));
  }

  // 5) 누락 채움 업데이트 처리 (id 충돌로 업데이트)
  for (let i = 0; i < toUpdate.length; i += BATCH) {
    const chunk = toUpdate.slice(i, i + BATCH);
    const { error } = await supabase.from('videos').upsert(chunk, { onConflict: 'id' });
    if (error) { uploadStatus.textContent = '누락 채움 실패: ' + error.message; uploadStatus.style.color='orange'; return; }
    updated += chunk.length; processed += chunk.length;
    uploadStatus.textContent = `처리 중... 삽입 ${inserted}, 업데이트 ${updated}`;
    await new Promise(r => setTimeout(r, 60));
  }

  uploadStatus.textContent = `완료: 삽입 ${inserted}, 누락 채움 업데이트 ${updated}`;
  uploadStatus.style.color = 'green';
  selectedFile = null; fileNameDisplay.textContent = ''; fileNameDisplay.classList.remove('active');
  fetchAndDisplayData();
}

// ---------- Export JSON (download + Supabase Storage) ----------
if (exportJsonBtn) {
  exportJsonBtn.addEventListener('click', async () => {
    try {
      exportStatus.style.display = 'block'; exportStatus.textContent = '데이터 내보내는 중...'; exportStatus.style.color = '';
      const { data, error } = await supabase.from('videos').select('*');
      if (error) throw error;
      const rows = data || [];
      const jsonText = JSON.stringify(rows, null, 2);
      // 1) 로컬 다운로드
      const url = URL.createObjectURL(new Blob([jsonText], { type: 'application/json' }));
      const a = document.createElement('a'); a.href = url; a.download = `videos_${new Date().toISOString().slice(0,10)}.json`; a.click(); URL.revokeObjectURL(url);
      // 2) Supabase Storage 업로드 (bucket: public, path: data/videos.json)
      try {
        const path = 'data/videos.json';
        const { error: upErr } = await supabase.storage.from('public').upload(path, new Blob([jsonText], { type:'application/json' }), { upsert: true, contentType: 'application/json' });
        if (upErr) throw upErr;
        const { data: pub } = supabase.storage.from('public').getPublicUrl(path);
        const publicUrl = pub?.publicUrl || '';
        await supabase.from('system').upsert({ id: 'settings', videos_json_url: publicUrl, last_build: new Date().toISOString() }, { onConflict: 'id' });
        exportStatus.textContent = `✅ ${rows.length}개 JSON 내보내기 및 업로드 완료`;
        exportStatus.style.color = 'green';
        } catch (e) {
        exportStatus.textContent = `다운로드 완료, 업로드 실패: ${e?.message || e}`;
        exportStatus.style.color = 'orange';
      }
        } catch (e) {
      exportStatus.style.display = 'block'; exportStatus.textContent = '❌ 내보내기 실패: ' + (e?.message || e); exportStatus.style.color = 'red';
        }
    });
}

// ---------- Schedules ----------
async function createSchedule(scope, ids, runAt, forceType) {
  const type = forceType || (document.querySelector('input[name="schedule-type"]:checked')?.value) || 'analysis';
  const now = new Date();
  const nowIso = new Date(now.getTime()).toISOString();
  // datetime-local 값은 로컬 타임존 기준의 벽시각. 이를 실제 순간(UTC) ISO로 변환
  const local = new Date(runAt);
  const runAtIso = new Date(local.getTime()).toISOString();
  const cfg = {
    type,
        scope,
    remaining_ids: scope === 'selected' ? ids : [],
        status: 'pending',
    run_at: runAtIso,
    created_at: nowIso,
    updated_at: nowIso
  };
  // 최소 스키마(id, date, content, created_at)에 맞춰 저장
  // date는 날짜만 보존되어 오동작을 유발하므로 빈 값으로 두거나(권장) content.run_at만 사용합니다.
  const payload = { content: JSON.stringify(cfg), created_at: nowIso };
  const { data, error } = await supabase.from('schedules').insert(payload).select('id').single();
  if (error) throw error; return data.id;
}

function parseScheduleContent(row) {
  let cfg = {};
  try {
    if (row && typeof row.content === 'string') cfg = JSON.parse(row.content);
    else if (row && typeof row.content === 'object' && row.content) cfg = row.content;
  } catch {}
  const type = cfg.type || row?.type || 'analysis';
  const scope = cfg.scope || row?.scope || 'all';
  const remainingIds = cfg.remaining_ids || cfg.ids || row?.remaining_ids || row?.ids || [];
  const status = cfg.status || row?.status || 'pending';
  // 실행/표시는 시간 손실이 없는 값만 사용
  // 표시: content.run_at(ISO) 또는 row.run_at(ISO)만 허용. date 컬럼은 무시(날짜만이라 KST 09:00로 보일 수 있음)
  const runAtIso = cfg.run_at || row?.run_at || null;
  return { type, scope, remainingIds, status, runAtIso };
}

async function listSchedules() {
  const { data, error } = await supabase.from('schedules').select('*');
  if (error) return [];
  const rows = data || [];
  return rows.sort((a,b) => {
    const A = new Date(parseScheduleContent(a).runAtIso || 0).getTime();
    const B = new Date(parseScheduleContent(b).runAtIso || 0).getTime();
    return A - B;
  });
}

async function cancelSchedule(id) {
  const { data } = await supabase.from('schedules').select('*').eq('id', id).single();
  let cfg = {};
  try { cfg = typeof data?.content === 'string' ? JSON.parse(data.content) : (data?.content || {}); } catch {}
  cfg.status = 'canceled'; cfg.updated_at = new Date().toISOString();
  if (data?.content !== undefined) {
    await supabase.from('schedules').update({ content: JSON.stringify(cfg) }).eq('id', id);
  } else {
    await supabase.from('schedules').update({ status: 'canceled', updated_at: new Date().toISOString() }).eq('id', id);
  }
}

function renderSchedulesTable(rows) {
    if (!rows.length) { schedulesTableContainer.innerHTML = '<p class="info-message">예약이 없습니다.</p>'; return; }
    const html = `
    <table class="data-table">
        <thead><tr><th><input type="checkbox" id="sched-select-all"></th><th>ID</th><th>작업</th><th>대상</th><th>실행 시각</th><th>상태</th><th>관리</th></tr></thead>
        <tbody>
            ${rows.map(r => `
            <tr data-id="${r.id}">
                <td><input type="checkbox" class="sched-row" data-id="${r.id}"></td>
                <td>${r.id}</td>
          <td>${(() => { const c = parseScheduleContent(r); return c.type === 'ranking' ? '랭킹' : '분석'; })()}</td>
          <td>${(() => { const c = parseScheduleContent(r); return c.scope === 'all' ? '전체' : `선택(${(c.remainingIds||[]).length})`; })()}</td>
          <td>${(() => { const c = parseScheduleContent(r); return c.runAtIso ? new Date(c.runAtIso).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }) : ''; })()}</td>
          <td>${(() => { const c = parseScheduleContent(r); return c.status; })()}</td>
          <td>${(() => { const c = parseScheduleContent(r); return c.status === 'pending' ? `<button class="btn btn-danger btn-cancel-schedule" data-id="${r.id}">취소</button>` : ''; })()}</td>
            </tr>`).join('')}
        </tbody>
    </table>`;
    schedulesTableContainer.innerHTML = html;
  document.getElementById('sched-select-all')?.addEventListener('change', (e) => {
        document.querySelectorAll('.sched-row').forEach(cb => { cb.checked = e.target.checked; });
    });
}

async function refreshSchedulesUI() {
    const rows = await listSchedules();
  renderSchedulesTable(rows);
}

scheduleCreateBtn?.addEventListener('click', async () => {
        const scope = (document.querySelector('input[name="schedule-scope"]:checked')?.value) || 'selected';
        const runAtStr = scheduleTimeInput?.value || '';
        if (!runAtStr) { scheduleCreateStatus.textContent = '실행 시각을 선택하세요.'; return; }
        const runAt = new Date(runAtStr).getTime();
  if (!isFinite(runAt) || runAt < Date.now() + 30000) { scheduleCreateStatus.textContent = '현재 시각 + 30초 이후로 설정.'; return; }
        let ids = [];
        if (scope === 'selected') {
    ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
            if (!ids.length) { scheduleCreateStatus.textContent = '선택 항목이 없습니다.'; return; }
        }
        scheduleCreateStatus.textContent = '예약 등록 중...';
  try { const id = await createSchedule(scope, ids, runAt); scheduleCreateStatus.textContent = `예약 등록 완료: ${id}`; await refreshSchedulesUI(); }
  catch (e) { scheduleCreateStatus.textContent = '예약 등록 실패: ' + (e?.message || e); }
});

scheduleRankingBtn?.addEventListener('click', async () => {
  const runAtStr = scheduleTimeInput?.value || '';
  if (!runAtStr) { scheduleCreateStatus.textContent = '실행 시각을 선택하세요.'; return; }
  const runAt = new Date(runAtStr).getTime();
  if (!isFinite(runAt) || runAt < Date.now() + 30000) { scheduleCreateStatus.textContent = '현재 시각 + 30초 이후로 설정.'; return; }
        scheduleCreateStatus.textContent = '랭킹 예약 등록 중...';
  try { const id = await createSchedule('all', [], runAt, 'ranking'); scheduleCreateStatus.textContent = `랭킹 예약 완료: ${id}`; await refreshSchedulesUI(); }
  catch (e) { scheduleCreateStatus.textContent = '등록 실패: ' + (e?.message || e); }
});

rankingRefreshNowBtn?.addEventListener('click', async () => {
  scheduleCreateStatus.textContent = '랭킹 즉시 갱신 요청 등록 중...';
  try { const id = await createSchedule('all', [], Date.now(), 'ranking'); scheduleCreateStatus.textContent = `즉시 갱신 요청 완료: ${id}`; await refreshSchedulesUI(); }
  catch (e) { scheduleCreateStatus.textContent = '요청 실패: ' + (e?.message || e); }
});

schedulesTableContainer?.addEventListener('click', async (e) => {
        const btn = e.target.closest('.btn-cancel-schedule');
  if (!btn) return;
  await cancelSchedule(btn.getAttribute('data-id'));
            await refreshSchedulesUI();
    });

schedulesBulkDeleteBtn?.addEventListener('click', async () => {
        const ids = Array.from(document.querySelectorAll('.sched-row:checked')).map(cb => cb.getAttribute('data-id'));
        if (!ids.length) { alert('삭제할 예약을 선택하세요.'); return; }
  await supabase.from('schedules').delete().in('id', ids);
        await refreshSchedulesUI();
    });

// ---------- Analysis helpers ----------
function getTranscriptServerUrl() {
  try { return localStorage.getItem('transcript_server_url') || '/api'; } catch { return '/api'; }
}
function showAnalysisBanner(msg) {
  analysisBanner?.classList.remove('hidden');
  if (analysisBannerText) analysisBannerText.textContent = msg || '';
  if (analysisProgressBar) analysisProgressBar.style.width = '0%';
  if (analysisLogEl) analysisLogEl.textContent = '';
}
function updateAnalysisProgress(done, total, suffix) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  if (analysisProgressBar) analysisProgressBar.style.width = pct + '%';
  if (analysisBannerText) analysisBannerText.textContent = `진행률 ${done}/${total} (${pct}%)` + (suffix ? ` — ${suffix}` : '');
}
function appendAnalysisLog(line) {
  if (!analysisLogEl) return; const t = new Date().toLocaleTimeString();
  analysisLogEl.textContent += `[${t}] ${line}\n`; analysisLogEl.scrollTop = analysisLogEl.scrollHeight;
}

async function fetchTranscriptByUrl(youtubeUrl) {
    const server = getTranscriptServerUrl();
    // STT fallback는 기본 비활성화; 네트워크 사용량 절감을 위해 명시적 요청 시만 활성화 (?stt=1)
    const res = await fetch(server.replace(/\/$/, '') + '/transcript?url=' + encodeURIComponent(youtubeUrl) + '&lang=ko,en');
    if (!res.ok) throw new Error('Transcript fetch failed: ' + res.status);
    const data = await res.json();
    return data.text || '';
}

// --- YouTube API helpers (분리된 기능)
async function fetchYoutubeViews(videoId, apiKey) {
  const url = new URL('https://www.googleapis.com/youtube/v3/videos');
  url.searchParams.set('part', 'statistics');
  url.searchParams.set('id', videoId);
  url.searchParams.set('key', apiKey);
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error('views api http ' + res.status);
  const data = await res.json();
  const item = (data.items || [])[0];
  const views = Number(item?.statistics?.viewCount || 0);
  return views;
}

// 좋아요/댓글수 동시 갱신
async function fetchYoutubeStats(videoId, apiKey) {
  const url = new URL('https://www.googleapis.com/youtube/v3/videos');
  url.searchParams.set('part', 'statistics');
  url.searchParams.set('id', videoId);
  url.searchParams.set('key', apiKey);
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error('stats api http ' + res.status);
  const data = await res.json();
  const item = (data.items || [])[0] || { statistics: {} };
  const stats = item.statistics || {};
  return {
    views: Number(stats.viewCount || 0),
    likes: Number(stats.likeCount || 0),
    comments: Number(stats.commentCount || 0)
  };
}

// 지수 백오프 재시도 래퍼
async function withRetry(fn, { retries = 3, baseDelayMs = 500 }) {
  let attempt = 0;
  while (true) {
    try { return await fn(); }
    catch (e) {
      attempt++;
      if (attempt > retries) throw e;
      const delay = Math.round(baseDelayMs * Math.pow(2, attempt - 1) * (1 + Math.random()*0.2));
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// --- 공용 처리기: 병렬 실행 + 키 로테이션 ---
function getStoredKeysForRotation() {
  const keys = getStoredYoutubeApiKeys();
  return keys.filter(k => !!k);
}

async function processInBatches(ids, worker, { concurrency = 6, onProgress } = {}) {
  let i = 0; let inFlight = 0; let done = 0; let failed = 0; let nextKeyIndex = 0; let startedAt = Date.now();
  const keys = getStoredKeysForRotation();
    const results = [];
  return await new Promise((resolve) => {
    const pump = () => {
      if (done + failed >= ids.length && inFlight === 0) return resolve({ done, failed, results });
      while (inFlight < concurrency && i < ids.length) {
        const id = ids[i++];
        const key = keys.length ? keys[nextKeyIndex++ % keys.length] : '';
        inFlight++;
        worker(id, key).then((r) => { results.push(r); done++; }).catch(() => { failed++; }).finally(() => {
          inFlight--;
          if (typeof onProgress === 'function') {
            const processed = done + failed;
            const pct = Math.round((processed / ids.length) * 100);
            const elapsed = (Date.now() - startedAt) / 1000;
            const rate = processed / Math.max(1, elapsed);
            const remain = ids.length - processed;
            const etaSec = Math.round(remain / Math.max(0.001, rate));
            onProgress({ processed, total: ids.length, pct, etaSec });
          }
          pump();
        });
      }
    };
    pump();
  });
}

function estimateDopamineLocal(sentence) {
  const s = String(sentence || '').toLowerCase();
  let score = 3;
  if (/충격|반전|경악|미친|대폭|폭로|소름|!|\?/.test(s)) score += 5;
  return Math.max(1, Math.min(10, score));
}

function cleanTranscriptToSentences(text) {
  let t = String(text || '');
  // 제거: 대괄호 안내, >>, 무음/음악 등
  t = t.replace(/\[[^\]]*\]/g, ' ')
       .replace(/^\s*>>.*/gm, ' ')
       .replace(/\b(음악|박수|웃음|침묵|배경음|기침)\b/gi, ' ');
  // 공백 정리
  t = t.replace(/\r/g, '\n').replace(/\n{2,}/g, '\n').replace(/[\t ]{2,}/g, ' ');
  // 한국어 종결어미 기반 문장 경계 보정: "요./다./죠./네./습니다/습니까/네요/군요" 뒤에 개행 삽입
  t = t.replace(/(요|다|죠|네|습니다|습니까|네요|군요)([.!?])/g, '$1$2\n');
  // 문장 분할
  let parts = t.split(/(?<=[.!?…]|\n)\s+/).map(s => s.trim()).filter(Boolean);
  // 너무 짧은 조각 병합
  const out = [];
  let buf = '';
  const MIN = 10;
  for (const p of parts) {
    const cur = (buf ? buf + ' ' : '') + p;
    if (cur.length < MIN) { buf = cur; continue; }
    out.push(cur); buf = '';
  }
  if (buf) out.push(buf);
  // 노이즈 라인 제거
  return out.filter(s => s.replace(/[^\p{L}\p{N}]/gu, '').length >= 3);
}

async function analyzeOneVideo(video) {
  // 저장된 대본만 사용. 재추출하지 않음
  appendAnalysisLog(`(${video.id}) 저장된 대본 사용...`);
  const transcript = String(video.transcript_text || '').trim();
  if (!transcript) {
    throw new Error('대본 없음: 먼저 대본 추출을 실행해야 합니다.');
  }
  const sentences = cleanTranscriptToSentences(transcript);
  const MAX = 300;
  const take = sentences.slice(0, MAX);
  const dopamine_graph = take.map(s => ({ sentence: s, level: estimateDopamineLocal(s), reason: 'heuristic' }));
  const updated = {
    id: video.id,
    analysis_transcript_len: transcript.length,
    dopamine_graph,
    // transcript_text는 유지 (재추출/변경 안 함)
    last_modified: Date.now()
  };
  return { updated };
}

async function runAnalysisForIds(ids) {
  analysisStatus.style.display = 'block'; analysisStatus.textContent = `분석 시작... (총 ${ids.length}개)`; analysisStatus.style.color = '';
  showAnalysisBanner(`총 ${ids.length}개 분석 시작 (소재→후킹→기승전결→그래프)`);
    let done = 0, failed = 0;
    for (const id of ids) {
        try {
      appendAnalysisLog(`(${id}) 서버 분석 요청 시작`);
      const res = await fetch('/api/analyze_one', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }) });
      let j = null; try { j = await res.json(); } catch {}
      if (!res.ok) {
        const stage = j && j.stage ? j.stage : '';
        const err = j && j.error ? j.error : '';
        const trace = j && j.trace ? String(j.trace).slice(0, 300) : '';
        appendAnalysisLog(`(${id}) 서버오류 http ${res.status} stage=${stage} ${err}`);
        if (trace) appendAnalysisLog(`trace: ${trace}`);
        // 환경 변수 진단
        try {
          const dbgRes = await fetch('/api/analyze_one/debug');
          const dbg = await dbgRes.json();
          const env = dbg && dbg.env ? dbg.env : {};
          appendAnalysisLog(`[env] SUPABASE_URL=${env.has_SUPABASE_URL?'OK':'MISS'}, SERVICE_ROLE=${env.has_SUPABASE_SERVICE_ROLE_KEY?'OK':'MISS'}, ANON=${env.has_SUPABASE_ANON_KEY?'OK':'MISS'}, GEMINI=${env.has_GEMINI_API_KEY?'OK':'MISS'}`);
        } catch {}
        throw new Error(`http ${res.status} ${err || ''}`.trim());
      }
      if (j && j.error) throw new Error(j.error);
      done++; analysisStatus.textContent = `진행중... ${done}/${ids.length}`; updateAnalysisProgress(done, ids.length, `id=${id}`); appendAnalysisLog(`(${id}) 서버 분석 완료`);
      await fetchAndDisplayData();
    } catch (e) { failed++; appendAnalysisLog(`(${id}) 오류: ${e?.message || e}`); }
  }
  analysisStatus.textContent = `분석 완료: 성공 ${done}, 실패 ${failed}`; analysisStatus.style.color = failed ? 'orange' : 'green';
    updateAnalysisProgress(ids.length, ids.length, `성공 ${done}, 실패 ${failed}`);
}

runAnalysisSelectedBtn?.addEventListener('click', async () => {
  const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
  if (!ids.length) { alert('분석할 항목을 선택하세요.'); return; }
        await runAnalysisForIds(ids);
    });

runAnalysisAllBtn?.addEventListener('click', async () => {
        const ids = currentData.map(v => v.id);
  if (!ids.length) { alert('분석할 데이터가 없습니다.'); return; }
  const ok = confirm(`전체 ${ids.length}개 항목에 대해 분석을 실행할까요? 비용이 발생할 수 있습니다.`);
  if (!ok) return;
        await runAnalysisForIds(ids);
    });

// ---------- Comments (basic, optional) ----------
function getStoredYoutubeApiKeys() { try { const raw = localStorage.getItem('youtube_api_keys_list') || ''; return raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean); } catch { return []; } }
function pickRotatingKey(keys, i) { return keys.length ? keys[i % keys.length] : ''; }
function extractVideoIdFromUrl(urlStr) { try { const u = new URL(urlStr); if (u.hostname.includes('youtu.be')) return u.pathname.split('/').pop(); if (u.searchParams.get('v')) return u.searchParams.get('v'); if (u.pathname.includes('/shorts/')) return u.pathname.split('/').pop(); return ''; } catch { return ''; } }

async function fetchYoutubeComments(videoId, maxCount, keys) {
  const out = []; let pageToken = ''; let reqIndex = 0;
    while (out.length < maxCount) {
        const key = pickRotatingKey(keys, reqIndex++);
    if (!key) throw new Error('YouTube API 키가 없습니다.');
    const remain = maxCount - out.length; const pageSize = Math.max(1, Math.min(100, remain));
        const url = new URL('https://www.googleapis.com/youtube/v3/commentThreads');
    url.searchParams.set('part', 'snippet'); url.searchParams.set('videoId', videoId); url.searchParams.set('maxResults', String(pageSize)); url.searchParams.set('order', 'relevance'); url.searchParams.set('key', key); if (pageToken) url.searchParams.set('pageToken', pageToken);
    const res = await fetch(url.toString()); if (!res.ok) break; const data = await res.json();
        const items = Array.isArray(data.items) ? data.items : [];
    for (const it of items) { const sn = it.snippet?.topLevelComment?.snippet; if (!sn) continue; out.push({ author: sn.authorDisplayName||'', text: sn.textOriginal||sn.textDisplay||'', likeCount: Number(sn.likeCount||0), publishedAt: sn.publishedAt||'' }); if (out.length >= maxCount) break; }
    if (out.length >= maxCount) break; pageToken = data.nextPageToken || ''; if (!pageToken) break;
    }
    return out;
}

runCommentsSelectedBtn?.addEventListener('click', async () => {
  const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
  if (!ids.length) { alert('댓글을 수집할 항목을 선택하세요.'); return; }
  const want = Math.max(1, Math.min(1000, Number(commentCountInput?.value || 50)));
  const keys = getStoredYoutubeApiKeys(); if (!keys.length) { alert('YouTube API 키를 설정하세요.'); return; }
  analysisStatus.style.display = 'block'; analysisStatus.textContent = `댓글 수집 시작... (${ids.length}개)`; analysisStatus.style.color = '';
  showAnalysisBanner(`댓글 수집 시작 (${ids.length}개)`);
  let done = 0;
  for (const id of ids) {
    try {
      const { data: row } = await supabase.from('videos').select('youtube_url,title').eq('id', id).single();
      const vid = extractVideoIdFromUrl(row?.youtube_url || ''); if (!vid) { appendAnalysisLog(`(${id}) YouTube URL 없음`); continue; }
      const comments = await fetchYoutubeComments(vid, want, keys);
      const top = comments.sort((a,b) => (b.likeCount||0)-(a.likeCount||0)).slice(0, 20);
      await supabase.from('videos').update({ comments_total: comments.length, comments_top: top, comments_fetched_at: new Date().toISOString(), last_modified: Date.now() }).eq('id', id);
      done++; updateAnalysisProgress(done, ids.length, row?.title || id); appendAnalysisLog(`(${id}) 댓글 ${comments.length}개`);
    } catch (e) { appendAnalysisLog(`(${id}) 댓글 오류: ${e?.message || e}`); }
  }
  analysisStatus.textContent = `댓글 수집 완료`;
});

// --- 분리된 버튼: 선택 대본 추출 (YouTube API 경유, Gemini 미사용)
ytTranscriptSelectedBtn?.addEventListener('click', async () => {
  const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
  if (!ids.length) { alert('대본을 추출할 항목을 선택하세요.'); return; }
  youtubeStatus.style.display = 'block'; youtubeStatus.textContent = `대본 추출 시작... (${ids.length}개)`; youtubeStatus.style.color = '';
  showAnalysisBanner(`대본 추출 시작 (${ids.length}개)`);
  const onlyMissing = !!ytTranscriptOnlyMissing?.checked;
  // 스키마: transcript_unavailable 컬럼 탐지(있으면 실패시 플래그 저장 및 다음번 자동 스킵)
  let canFlag = false; try { const probe = await supabase.from('videos').select('transcript_unavailable').limit(0); canFlag = !probe.error; } catch {}
  const worker = async (id) => {
    const { data: row, error } = await supabase.from('videos').select(canFlag ? 'youtube_url,transcript_text,transcript_unavailable' : 'youtube_url,transcript_text').eq('id', id).single();
    if (error) { ylog(`(${id}) fetch row error: ${error.message}`); throw error; }
    if (onlyMissing && row?.transcript_text && String(row.transcript_text).trim().length > 0) { ylog(`(${id}) skip (already has transcript)`); return; }
    if (canFlag && row?.transcript_unavailable) { ylog(`(${id}) skip (transcript unavailable flagged)`); return; }
    const url = row?.youtube_url || '';
    if (!url) { ylog(`(${id}) skip (no youtube_url)`); throw new Error('no url'); }
    try {
      const transcript = await fetchTranscriptByUrl(url);
      await supabase.from('videos').update({ transcript_text: transcript, analysis_transcript_len: transcript.length, last_modified: Date.now() }).eq('id', id);
      ylog(`(${id}) transcript saved (${transcript.length} chars)`);
      appendAnalysisLog(`(${id}) 대본 저장 ${transcript.length}자`);
        } catch (e) {
      ylog(`(${id}) transcript error: ${e?.message || e}`);
      appendAnalysisLog(`(${id}) 대본 오류: ${e?.message || e}`);
      // 404 또는 자막 없음 케이스는 플래그 저장하여 다음번 자동 스킵
      const msg = (e?.message || '').toString();
      if (canFlag && /404|no_transcript_or_stt/i.test(msg)) {
        try { await supabase.from('videos').update({ transcript_unavailable: true, last_modified: Date.now() }).eq('id', id); ylog(`(${id}) flagged transcript_unavailable`); } catch {}
      }
      throw e;
    }
  };
  const conc = Math.max(1, Math.min(20, Number(ytTranscriptConcInput?.value || 6)));
  const { done, failed } = await processInBatches(ids, worker, { concurrency: conc, onProgress: ({ processed, total, pct, etaSec }) => { youtubeStatus.textContent = `대본 추출 진행 ${pct}% (ETA ${etaSec}s)`; updateAnalysisProgress(processed, total, `ETA ${etaSec}s`); } });
  youtubeStatus.textContent = `대본 추출 완료: 성공 ${done}, 실패 ${failed}`; youtubeStatus.style.color = failed ? 'orange' : 'green';
    await fetchAndDisplayData();
});

// --- 분리된 버튼: 선택 조회수 갱신 (YouTube Data API)
ytViewsSelectedBtn?.addEventListener('click', async () => {
  const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.getAttribute('data-id'));
  if (!ids.length) { alert('조회수를 갱신할 항목을 선택하세요.'); return; }
  const keys = getStoredYoutubeApiKeys(); if (!keys.length) { alert('YouTube API 키를 설정하세요.'); return; }
  youtubeStatus.style.display = 'block'; youtubeStatus.textContent = `조회수 갱신 시작... (${ids.length}개)`; youtubeStatus.style.color = '';
  showAnalysisBanner(`조회수 갱신 시작 (${ids.length}개)`);
  const onlyMissing = !!ytViewsOnlyMissing?.checked;
  const excludeMin = Math.max(0, Number(ytViewsExcludeMin?.value || 0));
  const cutoffMs = excludeMin > 0 ? (Date.now() - excludeMin * 60 * 1000) : 0;
  const worker = async (id, key) => {
    const { data: row, error } = await supabase.from('videos').select('youtube_url,views_numeric,views_baseline_numeric,views,views_last_checked_at').eq('id', id).single();
    if (error) { ylog(`(${id}) fetch row error: ${error.message}`); throw error; }
    if (onlyMissing && row?.views_numeric) { ylog(`(${id}) skip (has views_numeric)`); return; }
    if (cutoffMs && Number(row?.views_last_checked_at || 0) > cutoffMs) { ylog(`(${id}) skip (recently updated)`); return; }
    const url = row?.youtube_url || '';
    const u = new URL(url);
    let videoId = u.searchParams.get('v') || '';
    if (!videoId && u.hostname.includes('youtu.be')) videoId = u.pathname.split('/').pop();
    if (!videoId && u.pathname.includes('/shorts/')) videoId = u.pathname.split('/').pop();
    if (!videoId) { ylog(`(${id}) skip (bad youtube_url)`); throw new Error('no videoId'); }
    let current, likes, comments;
    try {
      const stats = await withRetry(() => fetchYoutubeStats(videoId, key), { retries: 3, baseDelayMs: 600 });
      current = stats.views; likes = stats.likes; comments = stats.comments;
    } catch (e) {
      ylog(`(${id}) stats error: ${e?.message || e}`);
      throw e;
    }
    const baseline = Number(row?.views_baseline_numeric || 0);
    const prevCurrent = Number(row?.views_numeric || 0);
    const prevCheckedAt = Number(row?.views_last_checked_at || 0) || null;
    const patch = { 
      views_numeric: current,
      likes_numeric: likes,
      comments_total: comments,
      views_last_checked_at: Date.now()
    };
    if (prevCurrent > 0) {
      patch.views_prev_numeric = prevCurrent;
      if (prevCheckedAt) patch.views_prev_checked_at = prevCheckedAt;
    }
    if (!baseline) patch.views_baseline_numeric = current; // 최초 1회만 베이스라인 세팅
    try { await supabase.from('videos').update(patch).eq('id', id); ylog(`(${id}) stats saved (views=${current}, baseline=${baseline||current}, likes=${likes}, comments=${comments})`); }
    catch (e) { ylog(`(${id}) save error: ${e?.message || e}`); throw e; }
    appendAnalysisLog(`(${id}) 조회수 ${current.toLocaleString()} 저장`);
  };
  const conc = Math.max(1, Math.min(30, Number(ytViewsConcInput?.value || 10)));
  const { done, failed } = await processInBatches(ids, worker, { concurrency: conc, onProgress: ({ processed, total, pct, etaSec }) => { youtubeStatus.textContent = `조회수 갱신 진행 ${pct}% (ETA ${etaSec}s)`; updateAnalysisProgress(processed, total, `ETA ${etaSec}s`); } });
  youtubeStatus.textContent = `조회수 갱신 완료: 성공 ${done}, 실패 ${failed}`; youtubeStatus.style.color = failed ? 'orange' : 'green';
  await fetchAndDisplayData();
});

// --- 전체 처리 버튼들 ---
ytTranscriptAllBtn?.addEventListener('click', async () => {
  if (!confirm('전체 대본을 추출할까요? 요청이 많아 시간이 걸릴 수 있습니다.')) return;
  youtubeStatus.style.display = 'block'; youtubeStatus.textContent = '전체 대본 추출 시작...'; youtubeStatus.style.color = '';
  showAnalysisBanner('전체 대본 추출 시작');
  const ids = currentData.map(v => v.id);
  const onlyMissing = !!ytTranscriptOnlyMissing?.checked;
  let canFlag = false; try { const probe = await supabase.from('videos').select('transcript_unavailable').limit(0); canFlag = !probe.error; } catch {}
  const worker = async (id) => {
    const { data: row, error } = await supabase.from('videos').select(canFlag ? 'youtube_url,transcript_text,transcript_unavailable' : 'youtube_url,transcript_text').eq('id', id).single();
    if (error) { ylog(`(${id}) fetch row error: ${error.message}`); throw error; }
    if (onlyMissing && row?.transcript_text && String(row.transcript_text).trim().length > 0) { ylog(`(${id}) skip (already has transcript)`); return; }
    if (canFlag && row?.transcript_unavailable) { ylog(`(${id}) skip (transcript unavailable flagged)`); return; }
    const url = row?.youtube_url || '';
    if (!url) { ylog(`(${id}) skip (no youtube_url)`); throw new Error('no url'); }
    try {
      const transcript = await fetchTranscriptByUrl(url);
      await supabase.from('videos').update({ transcript_text: transcript, analysis_transcript_len: transcript.length, last_modified: Date.now() }).eq('id', id);
      ylog(`(${id}) transcript saved (${transcript.length} chars)`);
      appendAnalysisLog(`(${id}) 대본 저장 ${transcript.length}자`);
    } catch (e) {
      ylog(`(${id}) transcript error: ${e?.message || e}`);
      appendAnalysisLog(`(${id}) 대본 오류: ${e?.message || e}`);
      const msg = (e?.message || '').toString();
      if (canFlag && /404|no_transcript_or_stt/i.test(msg)) {
        try { await supabase.from('videos').update({ transcript_unavailable: true, last_modified: Date.now() }).eq('id', id); ylog(`(${id}) flagged transcript_unavailable`); } catch {}
      }
      throw e;
    }
  };
  const conc = Math.max(1, Math.min(20, Number(ytTranscriptConcInput?.value || 6)));
  const { done, failed } = await processInBatches(ids, worker, { concurrency: conc, onProgress: ({ processed, total, pct, etaSec }) => { youtubeStatus.textContent = `전체 대본 추출 진행 ${pct}% (ETA ${etaSec}s)`; updateAnalysisProgress(processed, total, `ETA ${etaSec}s`); } });
  youtubeStatus.textContent = `전체 대본 추출 완료: 성공 ${done}, 실패 ${failed}`; youtubeStatus.style.color = failed ? 'orange' : 'green';
  await fetchAndDisplayData();
});

ytViewsAllBtn?.addEventListener('click', async () => {
  const keys = getStoredYoutubeApiKeys(); if (!keys.length) { alert('YouTube API 키를 설정하세요.'); return; }
  if (!confirm('전체 조회수를 갱신할까요? 요청이 많아 시간이 걸릴 수 있습니다.')) return;
  youtubeStatus.style.display = 'block'; youtubeStatus.textContent = '전체 조회수 갱신 시작...'; youtubeStatus.style.color = '';
  showAnalysisBanner('전체 조회수 갱신 시작');
  const ids = currentData.map(v => v.id);
  const onlyMissing = !!ytViewsOnlyMissing?.checked;
  const excludeMin = Math.max(0, Number(ytViewsExcludeMin?.value || 0));
  const cutoffMs = excludeMin > 0 ? (Date.now() - excludeMin * 60 * 1000) : 0;
  const worker = async (id, key) => {
    const { data: row, error } = await supabase.from('videos').select('youtube_url,views_numeric,views_baseline_numeric,views,views_last_checked_at').eq('id', id).single();
    if (error) { ylog(`(${id}) fetch row error: ${error.message}`); throw error; }
    if (onlyMissing && row?.views_numeric) { ylog(`(${id}) skip (has views_numeric)`); return; }
    if (cutoffMs && Number(row?.views_last_checked_at || 0) > cutoffMs) { ylog(`(${id}) skip (recently updated)`); return; }
    const url = row?.youtube_url || '';
    const u = new URL(url);
    let videoId = u.searchParams.get('v') || '';
    if (!videoId && u.hostname.includes('youtu.be')) videoId = u.pathname.split('/').pop();
    if (!videoId && u.pathname.includes('/shorts/')) videoId = u.pathname.split('/').pop();
    if (!videoId) { ylog(`(${id}) skip (bad youtube_url)`); throw new Error('no videoId'); }
    let current, likes, comments;
    try {
      const stats = await withRetry(() => fetchYoutubeStats(videoId, key), { retries: 3, baseDelayMs: 600 });
      current = stats.views; likes = stats.likes; comments = stats.comments;
    } catch (e) {
      ylog(`(${id}) stats error: ${e?.message || e}`);
      throw e;
    }
    const baseline = Number(row?.views_baseline_numeric || 0);
    const prevCurrent = Number(row?.views_numeric || 0);
    const prevCheckedAt = Number(row?.views_last_checked_at || 0) || null;
    const patch = { 
      views_numeric: current,
      likes_numeric: likes,
      comments_total: comments,
      views_last_checked_at: Date.now()
    };
    if (prevCurrent > 0) {
      patch.views_prev_numeric = prevCurrent;
      if (prevCheckedAt) patch.views_prev_checked_at = prevCheckedAt;
    }
    if (!baseline) patch.views_baseline_numeric = current;
    try { await supabase.from('videos').update(patch).eq('id', id); ylog(`(${id}) stats saved (views=${current}, baseline=${baseline||current}, likes=${likes}, comments=${comments})`); }
    catch (e) { ylog(`(${id}) save error: ${e?.message || e}`); throw e; }
    appendAnalysisLog(`(${id}) 조회수 ${current.toLocaleString()} 저장`);
  };
  const conc = Math.max(1, Math.min(30, Number(ytViewsConcInput?.value || 10)));
  const { done, failed } = await processInBatches(ids, worker, { concurrency: conc, onProgress: ({ processed, total, pct, etaSec }) => { youtubeStatus.textContent = `전체 조회수 갱신 진행 ${pct}% (ETA ${etaSec}s)`; updateAnalysisProgress(processed, total, `ETA ${etaSec}s`); } });
  youtubeStatus.textContent = `전체 조회수 갱신 완료: 성공 ${done}, 실패 ${failed}`; youtubeStatus.style.color = failed ? 'orange' : 'green';
  await fetchAndDisplayData();
});

// ---------- Settings (local) ----------
function restoreLocalSettings() {
  try { const key = localStorage.getItem('gemini_api_key_secure') || ''; if (key) geminiKeyInput.value = key; } catch {}
  try { const url = localStorage.getItem('transcript_server_url') || ''; if (url) transcriptServerInput.value = url; } catch {}
  try { const yt = localStorage.getItem('youtube_api_keys_list') || ''; if (ytKeysTextarea) ytKeysTextarea.value = yt; } catch {}
}
if (saveGeminiKeyBtn) saveGeminiKeyBtn.addEventListener('click', () => { try { localStorage.setItem('gemini_api_key_secure', geminiKeyInput.value.trim()); geminiKeyStatus.textContent = '저장되었습니다.'; } catch {} });
if (testGeminiKeyBtn) testGeminiKeyBtn.addEventListener('click', async () => { const key = geminiKeyInput.value.trim() || localStorage.getItem('gemini_api_key_secure') || ''; if (!key) { geminiKeyStatus.textContent = '키를 입력하세요.'; return; } geminiKeyStatus.textContent = '테스트 중...'; try { const res = await fetch('https://generativelanguage.googleapis.com/v1/models?key=' + encodeURIComponent(key)); geminiKeyStatus.textContent = res.ok ? '키 통신 성공' : 'HTTP ' + res.status; } catch (e) { geminiKeyStatus.textContent = '테스트 실패: ' + (e?.message || e); } });
if (saveTranscriptServerBtn) saveTranscriptServerBtn.addEventListener('click', async () => { const url = (transcriptServerInput.value || '').trim(); if (!url) { transcriptServerStatus.textContent = '서버 주소를 입력하세요.'; return; } try { localStorage.setItem('transcript_server_url', url); const res = await fetch(url.replace(/\/$/, '') + '/health'); transcriptServerStatus.textContent = res.ok ? '서버 온라인' : '응답 오류'; } catch (e) { transcriptServerStatus.textContent = '연결 실패: ' + (e?.message || e); } });
if (ytKeysSaveBtn) ytKeysSaveBtn.addEventListener('click', async () => { try { localStorage.setItem('youtube_api_keys_list', ytKeysTextarea.value || ''); ytKeysStatus.textContent = '저장되었습니다.'; } catch (e) { ytKeysStatus.textContent = '저장 실패: ' + (e?.message || e); } });
if (ytKeysTestBtn) ytKeysTestBtn.addEventListener('click', async () => { ytKeysStatus.textContent = '테스트 중...'; const keys = (ytKeysTextarea.value || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean); if (!keys.length) { ytKeysStatus.textContent = '키를 입력하세요.'; return; } try { const key = keys[0]; const res = await fetch('https://www.googleapis.com/youtube/v3/videos?part=statistics&id=dQw4w9WgXcQ&key=' + encodeURIComponent(key)); ytKeysStatus.textContent = res.ok ? '키 통신 성공' : 'HTTP ' + res.status; } catch (e) { ytKeysStatus.textContent = '테스트 실패: ' + (e?.message || e); } });

// ---------- Utils ----------
function stableHash(str) { let h = 0; for (let i = 0; i < str.length; i++) { h = (h << 5) - h + str.charCodeAt(i); h |= 0; } return Math.abs(h).toString(36); }
function normalizeDate(v) { if (!v) return ''; if (typeof v === 'number') { const epoch = new Date(1899, 11, 30).getTime(); const ms = epoch + v * 86400000; try { return new Date(ms).toISOString().slice(0,10); } catch { return String(v); } } const s = String(v).trim().replace(/[./]/g, '-'); if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(s)) { try { return new Date(s).toISOString().slice(0,10); } catch { return s; } } return s; }
function normalizeUpdateDate(v) { if (!v) return ''; try { const s = String(v).trim(); if (!s) return ''; // 허용: YYYY-MM-DD, YYYY.MM.DD, M/D, M월 D일, 10월 6일 등
  const now = new Date();
  const pad = (n) => String(n).padStart(2,'0');
  const fmtLocal = (dt) => `${dt.getFullYear()}-${pad(dt.getMonth()+1)}-${pad(dt.getDate())}`;
  // Excel serial -> 날짜로 간주 (정수/실수)
  if (!isNaN(Number(s)) && s.replace(/\s/g,'') === String(Number(s))) {
    const epoch = new Date(1899, 11, 30).getTime(); const ms = epoch + Number(s) * 86400000; return fmtLocal(new Date(ms));
  }
  const s1 = s.replace(/\s+/g,' ').replace(/[.]/g,'-').replace(/[\/]/g,'-');
  // YYYY-MM-DD
  if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(s1)) { const dt = new Date(s1); if (!isNaN(dt.getTime())) return fmtLocal(dt); }
  // M-D -> 올해로 보정
  if (/^\d{1,2}-\d{1,2}$/.test(s1)) { const [m,d] = s1.split('-').map(n=>parseInt(n,10)); const dt = new Date(now.getFullYear(), m-1, d); return fmtLocal(dt); }
  // 한국어: 10월 6일
  const m = s.match(/(\d{1,2})\s*월\s*(\d{1,2})\s*일/);
  if (m) { const mm = parseInt(m[1],10), dd = parseInt(m[2],10); const dt = new Date(now.getFullYear(), mm-1, dd); return fmtLocal(dt); }
  // 마지막 시도: Date 파서
  const dt = new Date(s);
  if (!isNaN(dt.getTime())) return fmtLocal(dt);
  return '';
} catch { return ''; } }
function formatDateTimeLocal(d) { const pad = (n) => String(n).padStart(2,'0'); return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`; }
function escapeHtml(str) { return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

// bigint 컬럼 안전 변환
function toBigIntSafe(value) {
  const raw = (value ?? '').toString().trim();
  if (!raw) return 0;
  const digits = raw.replace(/[^0-9]/g, '');
  if (!digits) return 0;
  // supabase-js는 JS number를 그대로 전송하므로 bigint 컬럼에는 정수 문자열을 사용해도 허용됩니다.
  try { return BigInt(digits).toString(); } catch { return 0; }
}


