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
const dataSearchInput = document.getElementById('data-search-input');
const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
const runAnalysisSelectedBtn = document.getElementById('run-analysis-selected-btn');
const runAnalysisAllBtn = document.getElementById('run-analysis-all-btn');
const analysisStatus = document.getElementById('analysis-status');
const commentCountInput = document.getElementById('comment-count-input');
const runCommentsSelectedBtn = document.getElementById('run-comments-selected-btn');

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

let currentData = [];
let selectedFile = null;
let docIdToEdit = null;
let isBulkDelete = false;

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
      loginDebug.textContent += `[client] URL: ${import.meta.env?.VITE_SUPABASE_URL ? 'OK' : 'MISSING'}\n`;
      loginDebug.textContent += `[client] ANON: ${import.meta.env?.VITE_SUPABASE_ANON_KEY ? 'OK' : 'MISSING'}\n`;
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
    const { data, error } = await supabase.from('videos').select('*').order('date', { ascending: false });
    if (error) throw error;
    currentData = Array.isArray(data) ? data : [];
    renderTable(currentData);
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
  const table = document.createElement('table');
  table.className = 'data-table';
  table.innerHTML = `
    <thead>
      <tr>
        <th><input type="checkbox" id="select-all-checkbox" /></th>
        <th>썸네일</th><th>제목</th><th>채널</th><th>게시일</th><th>상태</th><th>관리</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map(v => `
        <tr data-id="${v.id}">
          <td><input type="checkbox" class="row-checkbox" data-id="${v.id}"></td>
          <td>${v.thumbnail ? `<img class="table-thumbnail" src="${v.thumbnail}">` : ''}</td>
          <td class="table-title">${escapeHtml(v.title || '')}</td>
          <td>${escapeHtml(v.channel || '')}</td>
          <td>${escapeHtml(v.date || '')}</td>
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

dataTableContainer.addEventListener('click', (e) => {
  const btnEdit = e.target.closest('.btn-edit');
  const btnDel = e.target.closest('.single-delete-btn');
  if (btnEdit) openEditModal(btnEdit.getAttribute('data-id'));
  if (btnDel) openConfirmModal(btnDel.getAttribute('data-id'), false);
});

dataSearchInput?.addEventListener('input', (e) => {
  const t = String(e.target.value || '').toLowerCase();
  const filtered = currentData.filter(v => (v.title || '').toLowerCase().includes(t) || (v.channel || '').toLowerCase().includes(t));
  renderTable(filtered);
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

  // 1) 입력 정규화
  const incoming = [];
  for (const row of data) {
    const hasAny = row && (row.Title || row.title || row['YouTube URL'] || row.youtube_url || row.Hash);
    if (!hasAny) continue;
    const url = row['YouTube URL'] || row['youtube_url'] || '';
    const computedHash = String(row.Hash || stableHash(String(url || row.Title || row.title || ''))).trim();
    if (!computedHash) continue;
    incoming.push({
      hash: computedHash,
      thumbnail: (row.Thumbnail || row.thumbnail || '').trim(),
      title: (row.Title || row.title || '').trim(),
      views: (row.Views || row.views || '').toString(),
      views_numeric: (row.Views_numeric ?? row.views_numeric),
      channel: (row.Channel || row.channel || '').trim(),
      date: normalizeDate(row.Date || row.date || ''),
      subscribers: (row.Subscribers || row.subscribers || '').trim(),
      subscribers_numeric: (row.Subscribers_numeric ?? row.subscribers_numeric),
      youtube_url: (url || '').trim(),
      group_name: row.group_name || '',
      template_type: row['템플릿 유형'] || row.template_type || ''
    });
  }
  if (!incoming.length) { uploadStatus.textContent = '업로드할 유효한 행이 없습니다.'; uploadStatus.style.color = 'orange'; return; }

  // 2) 기존 데이터 조회 (hash 기준)
  const BATCH = 500; let processed = 0;
  const hashList = incoming.map(r => r.hash);
  const existingByHash = new Map();
  for (let i = 0; i < hashList.length; i += BATCH) {
    const slice = hashList.slice(i, i + BATCH);
    const { data: rows } = await supabase
      .from('videos')
      .select('id,hash,thumbnail,title,views,views_numeric,channel,date,subscribers,subscribers_numeric,youtube_url,group_name,template_type')
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
      toInsert.push(payload);
    } else {
      // 기존: 누락된 필드만 채우기 (null/빈 문자열만 누락으로 간주)
      const upd = { id: exist.id };
      let has = false;
      const isEmpty = (v) => v == null || (typeof v === 'string' && v.trim() === '');
      if (item.thumbnail && isEmpty(exist.thumbnail)) { upd.thumbnail = item.thumbnail; has = true; }
      if (item.title && isEmpty(exist.title)) { upd.title = item.title; has = true; }
      if (item.views && isEmpty(exist.views)) { upd.views = item.views; has = true; }
      if (item.views_numeric != null && (exist.views_numeric == null || exist.views_numeric === '')) { upd.views_numeric = toBigIntSafe(item.views_numeric); has = true; }
      if (item.channel && isEmpty(exist.channel)) { upd.channel = item.channel; has = true; }
      if (item.date && isEmpty(exist.date)) { upd.date = item.date; has = true; }
      if (item.subscribers && isEmpty(exist.subscribers)) { upd.subscribers = item.subscribers; has = true; }
      if (item.subscribers_numeric != null && (exist.subscribers_numeric == null || exist.subscribers_numeric === '')) { upd.subscribers_numeric = toBigIntSafe(item.subscribers_numeric); has = true; }
      if (item.youtube_url && isEmpty(exist.youtube_url)) { upd.youtube_url = item.youtube_url; has = true; }
      if (item.group_name && isEmpty(exist.group_name)) { upd.group_name = item.group_name; has = true; }
      if (item.template_type && isEmpty(exist.template_type)) { upd.template_type = item.template_type; has = true; }
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
  const payload = { scope, ids: scope==='selected'? ids: [], run_at: new Date(runAt).toISOString(), type, status:'pending', created_at: new Date().toISOString(), updated_at: new Date().toISOString() };
  const { data, error } = await supabase.from('schedules').insert(payload).select('id').single();
  if (error) throw error; return data.id;
}

async function listSchedules() {
  const { data, error } = await supabase.from('schedules').select('*').order('run_at', { ascending: true });
  if (error) return [];
  return data || [];
}

async function cancelSchedule(id) {
  await supabase.from('schedules').update({ status:'canceled', updated_at: new Date().toISOString() }).eq('id', id);
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
          <td>${r.type === 'ranking' ? '랭킹' : '분석'}</td>
          <td>${r.scope === 'all' ? '전체' : `선택(${(r.ids||[]).length})`}</td>
          <td>${r.run_at ? new Date(r.run_at).toLocaleString() : ''}</td>
          <td>${r.status}</td>
          <td>${r.status === 'pending' ? `<button class="btn btn-danger btn-cancel-schedule" data-id="${r.id}">취소</button>` : ''}</td>
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
  const res = await fetch(server.replace(/\/$/, '') + '/transcript?url=' + encodeURIComponent(youtubeUrl) + '&lang=ko,en');
  if (!res.ok) throw new Error('Transcript fetch failed: ' + res.status);
  const data = await res.json();
  return data.text || '';
}

function estimateDopamineLocal(sentence) {
  const s = String(sentence || '').toLowerCase();
  let score = 3;
  if (/충격|반전|경악|미친|대폭|폭로|소름|!|\?/.test(s)) score += 5;
  return Math.max(1, Math.min(10, score));
}

async function analyzeOneVideo(video) {
  const youtubeUrl = video.youtube_url;
  if (!youtubeUrl) throw new Error('YouTube URL 없음');
  appendAnalysisLog(`(${video.id}) 자막 추출...`);
  const transcript = await fetchTranscriptByUrl(youtubeUrl);
  const sentences = transcript.split(/(?<=[.!?…])\s+/).filter(Boolean);
  const sample = sentences.filter((_, i) => i % 10 === 0);
  const dopamine_graph = sample.map(s => ({ sentence: s, level: estimateDopamineLocal(s), reason: 'heuristic' }));
  const updated = {
    id: video.id,
    analysis_transcript_len: transcript.length,
    dopamine_graph,
    transcript_text: transcript,
    last_modified: Date.now()
  };
  return { updated };
}

async function runAnalysisForIds(ids) {
  analysisStatus.style.display = 'block'; analysisStatus.textContent = `분석 시작... (총 ${ids.length}개)`; analysisStatus.style.color = '';
  showAnalysisBanner(`총 ${ids.length}개 분석 시작`);
  let done = 0, failed = 0;
  for (const id of ids) {
    try {
      const { data: row } = await supabase.from('videos').select('*').eq('id', id).single();
      if (!row) { failed++; continue; }
      const { updated } = await analyzeOneVideo({ id, ...row });
      const payload = { ...updated }; delete payload.id;
      const { error: upErr } = await supabase.from('videos').update(payload).eq('id', id);
      if (upErr) throw upErr;
      done++; analysisStatus.textContent = `진행중... ${done}/${ids.length}`; updateAnalysisProgress(done, ids.length, row.title || id); appendAnalysisLog(`(${id}) 저장 완료`);
    } catch (e) { failed++; appendAnalysisLog(`(${id}) 오류: ${e?.message || e}`); }
  }
  analysisStatus.textContent = `분석 완료: 성공 ${done}, 실패 ${failed}`; analysisStatus.style.color = failed ? 'orange' : 'green';
  updateAnalysisProgress(ids.length, ids.length, `성공 ${done}, 실패 ${failed}`);
  await fetchAndDisplayData();
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


