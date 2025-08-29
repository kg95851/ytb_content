import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth, onAuthStateChanged, signInWithEmailAndPassword, signOut } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import { getFirestore, collection, doc, getDocs, getDoc, writeBatch, deleteDoc, updateDoc, query, orderBy } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

// Firebase 초기화
const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

// DOM 요소
const loginView = document.getElementById('login-view');
const adminPanel = document.getElementById('admin-panel');
const logoutBtn = document.getElementById('logout-btn');
const tabs = document.querySelector('.tabs');
const tabLinks = document.querySelectorAll('.tab-link');
const tabContents = document.querySelectorAll('.tab-content');
const fileDropArea = document.getElementById('file-drop-area');
const fileInput = document.getElementById('file-input');
const fileNameDisplay = document.getElementById('file-name-display');
const uploadBtn = document.getElementById('upload-btn');
const uploadStatus = document.getElementById('upload-status');
const dataTableContainer = document.getElementById('data-table-container');
const dataSearchInput = document.getElementById('data-search-input');
const bulkDeleteBtn = document.getElementById('bulk-delete-btn');
const runAnalysisSelectedBtn = document.getElementById('run-analysis-selected-btn');
const runAnalysisAllBtn = document.getElementById('run-analysis-all-btn');
const analysisStatus = document.getElementById('analysis-status');
// 댓글 분석 UI 요소
const commentsAnalysisStatus = document.getElementById('comments-analysis-status');
const runCommentsSelectedBtn = document.getElementById('run-comments-selected-btn');
const commentCountInput = document.getElementById('comment-count-input');
// 상단 고정 배너 요소
const analysisBanner = document.getElementById('analysis-banner');
const analysisBannerText = document.getElementById('analysis-banner-text');
const analysisProgressBar = document.getElementById('analysis-progress-bar');
const analysisLogEl = document.getElementById('analysis-log');
const editModal = document.getElementById('edit-modal');
const editForm = document.getElementById('edit-form');
const saveEditBtn = document.getElementById('save-edit-btn');
const cancelEditBtn = document.getElementById('cancel-edit-btn');
const closeEditModalBtn = document.getElementById('close-edit-modal-btn');
const confirmModal = document.getElementById('confirm-modal');
const confirmModalTitle = document.getElementById('confirm-modal-title');
const confirmModalMessage = document.getElementById('confirm-modal-message');
const confirmDeleteBtn = document.getElementById('confirm-delete-btn');
const cancelDeleteBtn = document.getElementById('cancel-delete-btn');

let currentData = [];
let docIdToEdit = null;
let docIdToDelete = null;
let isBulkDelete = false;
let selectedFile = null;

// 인증 로직
onAuthStateChanged(auth, user => {
    if (user) {
        loginView.classList.add('hidden');
        adminPanel.classList.remove('hidden');
        fetchAndDisplayData();
    } else {
        loginView.classList.remove('hidden');
        adminPanel.classList.add('hidden');
    }
});

document.getElementById('login-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    
    // 1. 입력된 이메일 값이 올바른지 콘솔에 출력
    console.log("로그인 시도 이메일:", email);

    signInWithEmailAndPassword(auth, email, password)
      .then((userCredential) => {
        // 로그인이 성공한 경우
        console.log("로그인 성공!", userCredential.user);
      })
      .catch(error => {
        // 2. 로그인 실패 시 Firebase가 보낸 실제 에러 객체를 콘솔에 출력
        console.error("Firebase에서 받은 실제 에러:", error);

        // 화면에 에러 메시지 표시
        document.getElementById('login-error').textContent = '이메일 또는 비밀번호가 잘못되었습니다.';
    });
});

logoutBtn.addEventListener('click', () => signOut(auth));

// 탭 전환 로직
tabs.addEventListener('click', (e) => {
    if (e.target.classList.contains('tab-link')) {
        const tabId = e.target.getAttribute('data-tab');
        tabLinks.forEach(link => link.classList.remove('active'));
        tabContents.forEach(content => content.classList.remove('active'));
        e.target.classList.add('active');
        document.getElementById(tabId).classList.add('active');
    }
});

// 데이터 조회 및 표시 (Read)
const fetchAndDisplayData = async () => {
    dataTableContainer.innerHTML = '<p class="info-message">데이터를 불러오는 중...</p>';
    try {
        const q = query(collection(db, 'videos'), orderBy('date', 'desc'));
        const querySnapshot = await getDocs(q);
        currentData = querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        renderTable(currentData);
    } catch (error) {
        console.error("Error fetching data: ", error);
        dataTableContainer.innerHTML = '<p class="error-message">데이터를 불러오는 데 실패했습니다.</p>';
    }
};

const renderTable = (data) => {
    if (data.length === 0) {
        dataTableContainer.innerHTML = '<p class="info-message">표시할 데이터가 없습니다.</p>';
        return;
    }
    const table = document.createElement('table');
    table.className = 'data-table';
    table.innerHTML = `
        <thead>
            <tr>
                <th><input type="checkbox" id="select-all-checkbox"></th>
                <th>썸네일</th><th>제목</th><th>채널</th><th>게시일</th><th>상태</th><th>관리</th>
            </tr>
        </thead>
        <tbody>
            ${data.map(item => `
                <tr data-id="${item.id}">
                    <td><input type="checkbox" class="row-checkbox" data-id="${item.id}"></td>
                    <td><img src="${item.thumbnail}" alt="thumbnail" class="table-thumbnail"/></td>
                    <td class="table-title">${item.title}</td>
                    <td>${item.channel}</td>
                    <td>${item.date}</td>
                    <td>${Array.isArray(item.dopamine_graph) && item.dopamine_graph.length ? '<span class="group-tag" style="background:#10b981;">Graph</span>' : ''}</td>
                    <td class="action-buttons">
                        <button class="btn btn-edit" data-id="${item.id}">수정</button>
                        <button class="btn btn-danger single-delete-btn" data-id="${item.id}">삭제</button>
                    </td>
                </tr>
            `).join('')}
        </tbody>
    `;
    dataTableContainer.innerHTML = '';
    dataTableContainer.appendChild(table);

    // 전체 선택 체크박스 이벤트
    document.getElementById('select-all-checkbox').addEventListener('change', (e) => {
        document.querySelectorAll('.row-checkbox').forEach(checkbox => {
            checkbox.checked = e.target.checked;
        });
    });
};

// 데이터 검색
dataSearchInput.addEventListener('input', (e) => {
    const searchTerm = e.target.value.toLowerCase();
    const filteredData = currentData.filter(item => 
        (item.title && item.title.toLowerCase().includes(searchTerm)) ||
        (item.channel && item.channel.toLowerCase().includes(searchTerm))
    );
    renderTable(filteredData);
});

// 데이터 수정 (Update)
const openEditModal = async (id) => {
    docIdToEdit = id;
    const docRef = doc(db, 'videos', docIdToEdit);
    const docSnap = await getDoc(docRef);
    if (docSnap.exists()) {
        const data = docSnap.data();
        editForm.innerHTML = '';
        Object.keys(data).sort().forEach(key => {
            const raw = data[key];
            const isObject = raw && typeof raw === 'object';
            const value = isObject ? JSON.stringify(raw, null, 2) : (raw ?? '');
            const isLong = String(value).length > 100 || isObject;
            editForm.innerHTML += `
                <div class="form-group">
                    <label for="edit-${key}">${key}</label>
                    ${isLong
                        ? `<textarea id="edit-${key}" name="${key}" style="min-height:120px;">${value}</textarea>`
                        : `<input type="text" id="edit-${key}" name="${key}" value="${value}">`
                    }
                </div>
            `;
        });
        editModal.classList.remove('hidden');
    }
};
const closeEditModal = () => editModal.classList.add('hidden');

saveEditBtn.addEventListener('click', async () => {
    const updatedData = {};
    new FormData(editForm).forEach((value, key) => {
        try {
            // JSON 문자열로 보이는 값은 파싱 시도 (도파민 그래프 수동 편집 지원)
            if (/^\s*\[|\{/.test(String(value))) {
                updatedData[key] = JSON.parse(value);
            } else {
                updatedData[key] = value;
            }
        } catch {
            updatedData[key] = value;
        }
    });
    await updateDoc(doc(db, 'videos', docIdToEdit), updatedData);
    closeEditModal();
    fetchAndDisplayData();
});

cancelEditBtn.addEventListener('click', closeEditModal);
closeEditModalBtn.addEventListener('click', closeEditModal);

// 데이터 삭제 (Delete)
const openConfirmModal = (id, isBulk = false) => {
    isBulkDelete = isBulk;
    if (isBulk) {
        confirmModalTitle.textContent = '선택 삭제 확인';
        confirmModalMessage.textContent = '선택된 항목들을 정말로 삭제하시겠습니까?';
    } else {
        docIdToDelete = id;
        confirmModalTitle.textContent = '삭제 확인';
        confirmModalMessage.textContent = '정말로 삭제하시겠습니까?';
    }
    confirmModal.classList.remove('hidden');
};
const closeConfirmModal = () => confirmModal.classList.add('hidden');

confirmDeleteBtn.addEventListener('click', async () => {
    if (isBulkDelete) {
        const selectedIds = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.dataset.id);
        const deleteBatch = writeBatch(db);
        selectedIds.forEach(id => {
            deleteBatch.delete(doc(db, 'videos', id));
        });
        await deleteBatch.commit();
    } else {
        await deleteDoc(doc(db, 'videos', docIdToDelete));
    }
    closeConfirmModal();
    fetchAndDisplayData();
});
cancelDeleteBtn.addEventListener('click', closeConfirmModal);

dataTableContainer.addEventListener('click', (e) => {
    if (e.target.matches('.btn-edit')) openEditModal(e.target.dataset.id);
    if (e.target.matches('.single-delete-btn')) openConfirmModal(e.target.dataset.id, false);
});

bulkDeleteBtn.addEventListener('click', () => {
    const selectedIds = Array.from(document.querySelectorAll('.row-checkbox:checked'));
    if (selectedIds.length > 0) {
        openConfirmModal(null, true);
    } else {
        alert('삭제할 항목을 선택해주세요.');
    }
});

// ---------------- Gemini API & Transcript Server Settings ----------------
const GEMINI_KEY_STORAGE = 'gemini_api_key_secure';
const TRANSCRIPT_SERVER_STORAGE = 'transcript_server_url';

const geminiKeyInput = document.getElementById('gemini-api-key');
const saveGeminiKeyBtn = document.getElementById('save-gemini-key-btn');
const testGeminiKeyBtn = document.getElementById('test-gemini-key-btn');
const geminiKeyStatus = document.getElementById('gemini-key-status');

const transcriptServerInput = document.getElementById('transcript-server-url');
const saveTranscriptServerBtn = document.getElementById('save-transcript-server-btn');
const transcriptServerStatus = document.getElementById('transcript-server-status');
// 예약 UI 요소
const scheduleCreateBtn = document.getElementById('schedule-create-btn');
const scheduleRankingBtn = document.getElementById('schedule-ranking-btn');
const rankingRefreshNowBtn = document.getElementById('ranking-refresh-now-btn');
const scheduleCreateStatus = document.getElementById('schedule-create-status');
const schedulesTableContainer = document.getElementById('schedules-table-container');
const schedulesBulkDeleteBtn = document.getElementById('schedules-bulk-delete-btn');
const scheduleTimeInput = document.getElementById('schedule-time');
const scheduleLogEl = document.getElementById('schedule-log');
// YouTube API keys UI
const ytKeysTextarea = document.getElementById('youtube-api-keys');
const ytKeysSaveBtn = document.getElementById('save-youtube-keys-btn');
const ytKeysTestBtn = document.getElementById('test-youtube-keys-btn');
const ytKeysStatus = document.getElementById('youtube-keys-status');


function getStoredGeminiKey() {
    try { return localStorage.getItem(GEMINI_KEY_STORAGE) || ''; } catch { return ''; }
}
function setStoredGeminiKey(key) {
    try { localStorage.setItem(GEMINI_KEY_STORAGE, key || ''); } catch {}
}
function getTranscriptServerUrl() {
    try {
        const saved = localStorage.getItem(TRANSCRIPT_SERVER_STORAGE);
        return saved || '/api';
    } catch {
        return '/api';
    }
}
function setTranscriptServerUrl(url) {
    try { localStorage.setItem(TRANSCRIPT_SERVER_STORAGE, url || '/api'); } catch {}
}

// 초기화: 저장된 값 복원
window.addEventListener('DOMContentLoaded', () => {
    const savedKey = getStoredGeminiKey();
    if (geminiKeyInput && savedKey) geminiKeyInput.value = savedKey;
    const savedServer = getTranscriptServerUrl();
    if (transcriptServerInput) transcriptServerInput.value = savedServer;
    // YouTube keys restore
    try {
        const savedKeys = localStorage.getItem('youtube_api_keys_list') || '';
        if (ytKeysTextarea) ytKeysTextarea.value = savedKeys;
    } catch {}
    // 예약 시간 기본값: 현재 시간 + 30분
    if (scheduleTimeInput) {
        const now = new Date();
        now.setMinutes(now.getMinutes() + 30);
        scheduleTimeInput.value = formatDateTimeLocal(now);
    }
});

if (saveGeminiKeyBtn) {
    saveGeminiKeyBtn.addEventListener('click', () => {
        const key = geminiKeyInput.value.trim();
        if (!key) { geminiKeyStatus.textContent = '키를 입력하세요.'; return; }
        setStoredGeminiKey(key);
        geminiKeyStatus.textContent = 'Gemini API 키 저장 완료.';
    });
}

if (testGeminiKeyBtn) {
    testGeminiKeyBtn.addEventListener('click', async () => {
        const key = geminiKeyInput.value.trim() || getStoredGeminiKey();
        if (!key) { geminiKeyStatus.textContent = '키가 없습니다.'; return; }
        geminiKeyStatus.textContent = '테스트 중...';
        try {
            // 가벼운 ping: models endpoint 목록 질의
            const res = await fetch('https://generativelanguage.googleapis.com/v1/models?key=' + encodeURIComponent(key));
            if (!res.ok) throw new Error('HTTP ' + res.status);
            geminiKeyStatus.textContent = '키 통신 성공 (권한은 별도 확인 필요)';
        } catch (e) {
            geminiKeyStatus.textContent = '키 테스트 실패: ' + e.message;
        }
    });
}

if (saveTranscriptServerBtn) {
    saveTranscriptServerBtn.addEventListener('click', async () => {
        const url = (transcriptServerInput.value || '').trim();
        if (!url) { transcriptServerStatus.textContent = '서버 주소를 입력하세요.'; return; }
        setTranscriptServerUrl(url);
        transcriptServerStatus.textContent = '서버 주소 저장 완료. 상태 확인 중...';
        try {
            const endpoint = url.replace(/\/$/, '') + '/health';
            const res = await fetch(endpoint);
            transcriptServerStatus.textContent = res.ok ? '서버 온라인' : '서버 응답 오류';
        } catch (e) {
            transcriptServerStatus.textContent = '서버 연결 실패: ' + e.message;
        }
    });
}
function formatDateTimeLocal(d) {
    // yyyy-MM-ddTHH:mm 형식
    const pad = (n) => String(n).padStart(2, '0');
    const yyyy = d.getFullYear();
    const MM = pad(d.getMonth() + 1);
    const dd = pad(d.getDate());
    const HH = pad(d.getHours());
    const mm = pad(d.getMinutes());
    return `${yyyy}-${MM}-${dd}T${HH}:${mm}`;
}

function getSelectedIdsFromTable() {
    return Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.dataset.id);
}

// 예약 스키마: collection 'schedules'
// { id, scope: 'selected'|'all', ids?: string[], runAt: number(ms), status: 'pending'|'running'|'done'|'canceled', createdAt, updatedAt }

async function createSchedule(scope, ids, runAt) {
    const col = collection(db, 'schedules');
    // Firestore add with autogenerated id
    const jobType = (document.querySelector('input[name="schedule-type"]:checked')?.value) || 'analysis';
    const payload = {
        scope,
        ids: scope === 'selected' ? ids : [],
        runAt,
        type: jobType === 'ranking' ? 'ranking' : 'analysis',
        status: 'pending',
        createdAt: Date.now(),
        updatedAt: Date.now()
    };
    const newDoc = doc(col);
    await updateDoc(newDoc, payload).catch(async (e) => {
        // updateDoc requires exists; use set via batch
        const b = writeBatch(db);
        b.set(newDoc, payload);
        await b.commit();
    });
    return newDoc.id;
}

async function listSchedules() {
    const qCol = collection(db, 'schedules');
    const snap = await getDocs(qCol);
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
}

async function cancelSchedule(id) {
    await updateDoc(doc(db, 'schedules', id), { status: 'canceled', updatedAt: Date.now() });
}

function renderSchedulesTable(rows) {
    if (!schedulesTableContainer) return;
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
                <td>${new Date(r.runAt).toLocaleString()}</td>
                <td>${r.status}</td>
                <td>
                    ${r.status === 'pending' ? `<button class="btn btn-danger btn-cancel-schedule" data-id="${r.id}">취소</button>` : ''}
                </td>
            </tr>`).join('')}
        </tbody>
    </table>`;
    schedulesTableContainer.innerHTML = html;
    const selectAll = document.getElementById('sched-select-all');
    if (selectAll) selectAll.addEventListener('change', (e) => {
        document.querySelectorAll('.sched-row').forEach(cb => { cb.checked = e.target.checked; });
    });
}

async function refreshSchedulesUI() {
    const rows = await listSchedules();
    renderSchedulesTable(rows.sort((a,b) => a.runAt - b.runAt));
}

if (scheduleCreateBtn) {
    scheduleCreateBtn.addEventListener('click', async () => {
        const scope = (document.querySelector('input[name="schedule-scope"]:checked')?.value) || 'selected';
        const runAtStr = scheduleTimeInput?.value || '';
        if (!runAtStr) { scheduleCreateStatus.textContent = '실행 시각을 선택하세요.'; return; }
        const runAt = new Date(runAtStr).getTime();
        if (!isFinite(runAt) || runAt < Date.now() + 30_000) { scheduleCreateStatus.textContent = '현재 시각 + 30초 이후로 설정하세요.'; return; }
        let ids = [];
        if (scope === 'selected') {
            ids = getSelectedIdsFromTable();
            if (!ids.length) { scheduleCreateStatus.textContent = '선택 항목이 없습니다.'; return; }
        }
        scheduleCreateStatus.textContent = '예약 등록 중...';
        try {
            const id = await createSchedule(scope, ids, runAt);
            scheduleCreateStatus.textContent = `예약 등록 완료: ${id}`;
            await refreshSchedulesUI();
        } catch (e) {
            scheduleCreateStatus.textContent = '예약 등록 실패: ' + (e.message || e);
        }
    });
}

if (scheduleRankingBtn) {
    scheduleRankingBtn.addEventListener('click', async () => {
        if (!scheduleTimeInput?.value) { scheduleCreateStatus.textContent = '실행 시각을 선택하세요.'; return; }
        const runAt = new Date(scheduleTimeInput.value).getTime();
        if (!isFinite(runAt) || runAt < Date.now() + 30_000) { scheduleCreateStatus.textContent = '현재 시각 + 30초 이후로 설정하세요.'; return; }
        scheduleCreateStatus.textContent = '랭킹 예약 등록 중...';
        try {
            // 강제 타입/스코프 설정: ranking/all
            const col = collection(db, 'schedules');
            const payload = { scope: 'all', ids: [], runAt, type: 'ranking', status: 'pending', createdAt: Date.now(), updatedAt: Date.now() };
            const newDoc = doc(col);
            const b = writeBatch(db); b.set(newDoc, payload); await b.commit();
            scheduleCreateStatus.textContent = `랭킹 예약 완료: ${newDoc.id}`;
            await refreshSchedulesUI();
        } catch (e) {
            scheduleCreateStatus.textContent = '등록 실패: ' + (e.message || e);
        }
    });
}

if (ytKeysSaveBtn) {
    ytKeysSaveBtn.addEventListener('click', async () => {
        const raw = (ytKeysTextarea?.value || '').trim();
        try { localStorage.setItem('youtube_api_keys_list', raw); } catch {}
        ytKeysStatus.textContent = '저장되었습니다. 서버에 동기화 중...';
        // Firestore 시스템 설정에 키 저장 (서버리스에서 사용)
        try {
            const ref = doc(db, 'system', 'settings');
            await updateDoc(ref, { youtube_api_keys: raw, updatedAt: Date.now() }).catch(async () => {
                const b = writeBatch(db); b.set(ref, { youtube_api_keys: raw, updatedAt: Date.now() }, { merge: true }); await b.commit();
            });
            ytKeysStatus.textContent = '서버 저장 완료. (서버리스 갱신에 사용됩니다)';
        } catch (e) {
            ytKeysStatus.textContent = '서버 저장 실패: ' + (e.message || e);
        }
    });
}

if (ytKeysTestBtn) {
    ytKeysTestBtn.addEventListener('click', async () => {
        ytKeysStatus.textContent = '테스트 중...';
        const keys = (ytKeysTextarea?.value || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);
        if (!keys.length) { ytKeysStatus.textContent = '키를 입력하세요.'; return; }
        try {
            // 간단한 ping: videos list with dummy id (will 400 on bad key)
            const key = keys[0];
            const url = 'https://www.googleapis.com/youtube/v3/videos?part=statistics&id=dQw4w9WgXcQ&key=' + encodeURIComponent(key);
            const res = await fetch(url);
            ytKeysStatus.textContent = res.ok ? '키 통신 성공 (권한은 별도 확인 필요)' : 'HTTP ' + res.status;
        } catch (e) {
            ytKeysStatus.textContent = '테스트 실패: ' + (e.message || e);
        }
    });
}

if (rankingRefreshNowBtn) {
    rankingRefreshNowBtn.addEventListener('click', async () => {
        scheduleCreateStatus.textContent = '즉시 갱신 요청 생성 중... (서버리스 호출 대기)';
        try {
            const col = collection(db, 'schedules');
            const payload = { scope: 'all', ids: [], runAt: Date.now(), type: 'ranking', status: 'pending', createdAt: Date.now(), updatedAt: Date.now() };
            const newDoc = doc(col);
            const b = writeBatch(db); b.set(newDoc, payload); await b.commit();
            scheduleCreateStatus.textContent = `즉시 갱신 요청 생성 완료: ${newDoc.id}`;
            await refreshSchedulesUI();
            // 서버리스 크론 즉시 트리거 (배포 환경에서만 유효)
            try {
                appendScheduleLog(`랭킹 작업 전송 [${newDoc.id}] — 서버리스 호출 시도`);
                const res = await fetch('/api/cron_analyze');
                appendScheduleLog(`서버리스 응답 [${newDoc.id}] ${res.ok ? 'OK' : 'HTTP ' + res.status}`);
            } catch (e) {
                appendScheduleLog(`서버리스 호출 실패 [${newDoc.id}] ${e?.message || e}`);
            }
        } catch (e) {
            scheduleCreateStatus.textContent = '생성 실패: ' + (e.message || e);
        }
    });
}

if (schedulesTableContainer) {
    schedulesTableContainer.addEventListener('click', async (e) => {
        const btn = e.target.closest('.btn-cancel-schedule');
        if (btn) {
            const id = btn.getAttribute('data-id');
            await cancelSchedule(id);
            await refreshSchedulesUI();
        }
    });
}

if (schedulesBulkDeleteBtn) {
    schedulesBulkDeleteBtn.addEventListener('click', async () => {
        const ids = Array.from(document.querySelectorAll('.sched-row:checked')).map(cb => cb.getAttribute('data-id'));
        if (!ids.length) { alert('삭제할 예약을 선택하세요.'); return; }
        const b = writeBatch(db);
        ids.forEach(id => b.delete(doc(db, 'schedules', id)));
        await b.commit();
        await refreshSchedulesUI();
    });
}

// 간단한 클라이언트 사이드 스케줄러: 탭이 열려있을 때만 작동
let schedulerTimer = null;
async function pollAndRunSchedules() {
    try {
        const rows = await listSchedules();
        const now = Date.now();
        // 랭킹 작업이 기한 도래 시 서버리스 엔드포인트를 깨워준다
        const dueRanking = rows.filter(r => r.type === 'ranking' && (r.status === 'pending' || r.status === 'running') && r.runAt <= now + 1000);
        if (dueRanking.length) {
            try {
                appendScheduleLog(`랭킹 예약 감지 (${dueRanking.length}건) — 서버리스 호출`);
                const res = await fetch('/api/cron_analyze');
                appendScheduleLog(`서버리스 응답 ${res.ok ? 'OK' : 'HTTP ' + res.status}`);
            } catch (e) {
                appendScheduleLog(`서버리스 호출 실패 ${e?.message || e}`);
            }
        }
        // 분석 작업만 처리 (랭킹 작업은 서버리스 처리)
        const dueAnalysis = rows.filter(r => r.type !== 'ranking' && r.status === 'pending' && r.runAt <= now + 1000);
        for (const job of dueAnalysis) {
            // 랭킹 작업은 서버리스에서 처리 → 로컬에서는 건드리지 않음
            // (보호) 혹시 남아있으면 무시
            if (job.type === 'ranking') { appendScheduleLog(`랭킹 작업 감지 [${job.id}] — 서버리스 처리 대기`); continue; }
            // 분석 작업만 로컬에서 실행
            await updateDoc(doc(db, 'schedules', job.id), { status: 'running', updatedAt: Date.now() }).catch(()=>{});
            appendScheduleLog(`작업 시작 [${job.id}] (분석)`);
            try {
                if (job.scope === 'all') {
                    const ids = currentData.map(v => v.id);
                    if (ids.length) await runAnalysisForIds(ids);
                } else {
                    const ids = Array.isArray(job.ids) ? job.ids : [];
                    if (ids.length) await runAnalysisForIds(ids);
                }
                await updateDoc(doc(db, 'schedules', job.id), { status: 'done', updatedAt: Date.now() }).catch(()=>{});
                appendScheduleLog(`작업 완료 [${job.id}]`);
            } catch (e) {
                await updateDoc(doc(db, 'schedules', job.id), { status: 'canceled', error: String(e?.message || e), updatedAt: Date.now() }).catch(()=>{});
                appendScheduleLog(`작업 실패 [${job.id}] ${e?.message || e}`);
            }
        }
    } catch {}
}

function startScheduler() {
    if (schedulerTimer) return;
    schedulerTimer = setInterval(pollAndRunSchedules, 10_000);
}

function stopScheduler() {
    if (schedulerTimer) {
        clearInterval(schedulerTimer);
        schedulerTimer = null;
    }
}

// 스케줄 UI 초기 로드와 폴링 시작
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') { startScheduler(); refreshSchedulesUI(); }
});
if (document.visibilityState === 'visible') { startScheduler(); }

// ---------------- Analysis Runner ----------------
async function fetchTranscriptByUrl(youtubeUrl) {
    const server = getTranscriptServerUrl();
    const res = await fetch(server.replace(/\/$/, '') + '/transcript?url=' + encodeURIComponent(youtubeUrl) + '&lang=ko,en');
    if (!res.ok) throw new Error('Transcript fetch failed: ' + res.status);
    const data = await res.json();
    // 기대 형식: { text: "..." }
    return data.text || '';
}

function buildCategoryPrompt() {
    return (
`다음 대본을 기반으로 카테고리를 아래 형식으로만 한 줄씩 정확히 출력하세요. 다른 텍스트/머리말/설명 금지.
한국 대 카테고리: 
한국 중 카테고리: 
한국 소 카테고리: 
EN Main Category: 
EN Sub Category: 
EN Micro Topic: 
중국 대 카테고리: 
중국 중 카테고리: 
중국 소 카테고리: `
    );
}

function buildKeywordsPrompt() {
    return (
`아래 제공된 "제목"과 "대본"을 모두 참고하여, 원본 영상을 검색해 찾기 쉬운 핵심 검색 키워드를 한국어/영어/중국어로 각각 8~15개씩 추출하세요.
출력 형식은 JSON 객체만, 다른 설명/머리말/코드펜스 금지.
요구 형식: {"ko":["키워드1","키워드2",...],"en":["keyword1",...],"zh":["关键词1",...]}
규칙:
- 각 키워드는 1~4단어의 짧은 구로 작성
- 해시태그/특수문자/따옴표 제거, 불용어 제외
- 동일 의미/중복 표현은 하나만 유지
- 인명/채널명/브랜드/핵심 주제 포함
`);
}

function buildAnalysisPrompt() {
    return (
`[GPTs Instructions 최종안]\n\n페르소나 (Persona)\n\n당신은 "대본분석_룰루랄라릴리"입니다. 유튜브 대본을 분석하여 콘텐츠 전략 수립과 프롬프트 최적화를 돕는 최고의 전문가입니다. 당신의 답변은 항상 체계적이고, 깔끔하며, 사용자가 바로 활용할 수 있도록 완벽하게 구성되어야 합니다.\n\n핵심 임무 (Core Mission)\n\n사용자가 유튜브 대본(영어 또는 한국어)을 입력하면, 아래 4번 항목의 **[출력 템플릿]**을 단 하나의 글자나 기호도 틀리지 않고 그대로 사용하여 분석 결과를 제공해야 합니다.\n\n절대 규칙 (Golden Rules)\n\n규칙 1: 템플릿 복제 - 출력물의 구조, 디자인, 순서, 항목 번호, 이모지(✨, 📌, 🎬, 🧐, 💡, ✅, 🤔), 강조(), 구분선(*) 등 모든 시각적 요소를 아래 **[출력 템플릿]**과 완벽하게 동일하게 재현해야 합니다.\n\n규칙 2: 순서 및 항목 준수 - 항상 0번, 1번, 2번, 3번, 4번, 5번, 6번, 7번, 8번,9번 항목을 빠짐없이, 순서대로 포함해야 합니다.\n\n규칙 3: 표 형식 유지 - 분석 내용의 대부분은 마크다운 표(Table)로 명확하게 정리해야 합니다.\n\n규칙 4: 내용의 구체성 - 각 항목에 필요한 분석 내용을 충실히 채워야 합니다. 특히 프롬프트 비교 시, 단순히 '유사함'에서 그치지 말고 이유를 명확히 설명해야 합니다.\n\n출력 템플릿 (Output Template) - 이 틀을 그대로 사용하여 답변할 것\n\n✨ 룰루 GPTs 분석 템플릿 적용 결과\n\n0. 대본 번역 (영어 → 한국어)\n(여기에 자연스러운 구어체 한국어 번역문을 작성한다.)\n\n1. 대본 기승전결 분석\n| 구분 | 내용 |\n| :--- | :--- |\n| 기 (상황 도입) | (여기에 '기'에 해당하는 내용을 요약한다.) |\n| 승 (사건 전개) | (여기에 '승'에 해당하는 내용을 요약한다.) |\n| 전 (위기/전환) | (여기에 '전'에 해당하는 내용을 요약한다.) |\n| 결 (결말) | (여기에 '결'에 해당하는 내용을 요약한다.) |\n\n2. 기존 프롬프트와의 미스매치 비교표\n| 프롬프트 번호 | 기 (문제 제기) | 승 (예상 밖 전개) | 전 (몰입·긴장 유도) | 결 (결론/인사이트) | 특징 | 미스매치 여부 |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n| 001 | 욕망 자극 | 수상한 전개 | 반전 | 허무/반전 결말 | 욕망+반전+유머 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 002 | 일상 시작 | 실용적 해결 | 낯선 기술 | 꿀팁 or 정리 | 실용+공감 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 003 | 위기 상황 | 극한 도전 | 생존 위기 | 실패 or 생존법 | 생존+경고 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 004 | 문화 충돌 | 오해 과정 | 이해 확장 | 감동 | 문화+인식 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 005 | 이상 행동 | 분석 진행 | 시각 변화 | 진실 발견 | 반전+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 006 | 멀쩡해 보임 | 내부 파헤침 | 충격 실체 | 소비자 경고 | 사기+정보 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 007 | 실패할 도전 | 이상한 방식 | 몰입 상황 | 교훈 전달 | 도전+극복 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 008 | 자연 속 상황 | 생존 시도 | 변수 등장 | 생존 기술 | 자연+실용 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 009 | 흔한 장소 | 이상한 디테일 | 공포 증가 | 붕괴 경고 | 위기+공포 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 010 | '진짜일까?' | 실험/분석 | 반전 | 허세 or 실속 | 비교+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n\n3. 대본 vs 비슷하거나 똑같은 기존 프롬프트 비교\n→ 유사 프롬프트: (여기에 2번에서 '✅ 유사'로 표시한 프롬프트 번호와 제목을 기재한다.)\n| 구분 | 🎬 대본 내용 | 📌 기존 프롬프트 (00X번) |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (유사 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (유사 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (유사 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (유사 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (유사 프롬프트의 전반적인 특징) |\n차이점 요약\n→ (여기에 대본과 유사 프롬프트의 핵심적인 차이점을 명확하게 요약하여 작성한다.)\n\n4. 대본 vs 새롭게 제안한 프롬프트 비교\n제안 프롬프트 제목: “(여기에 대본에 가장 잘 맞는 새로운 프롬프트 제목을 창의적으로 작성한다.)” 스토리 구조\n| 구분 | 🎬 대본 내용 | 💡 제안 프롬프트 |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (새 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (새 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (새 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (새 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (새 프롬프트의 전반적인 특징) |\n이 프롬프트의 강점\n→ (여기에 제안한 프롬프트가 왜 대본에 더 적합한지, 어떤 강점이 있는지 2~3가지 포인트로 설명한다.)\n\n5. 결론 요약\n| 항목 | 내용 |\n| :--- | :--- |\n| 기존 프롬프트 매칭 | (여기에 가장 유사한 프롬프트 번호와 함께, '정확히 일치하는 구조 없음' 등의 요약평을 작성한다.) |\n| 추가 프롬프트 필요성 | 필요함 — (여기에 왜 새로운 프롬프트가 필요한지 이유를 구체적으로 작성한다.) |\n| 새 프롬프트 제안 | (여기에 4번에서 제안한 프롬프트 제목과 핵심 특징을 요약하여 작성한다.) |\n| 활용 추천 분야 | (여기에 새 프롬프트가 어떤 종류의 콘텐츠에 활용될 수 있는지 구체적인 예시를 3~4가지 제시한다.) |\n\n6. 궁금증 유발 및 해소 과정 분석\n| 구분 | 내용 분석 (대본에서 어떻게 표현되었나?) | 핵심 장치 및 기법 |\n| :--- | :--- | :--- |\n| 🤔 궁금증 유발 (Hook) | (시작 부분에서 시청자가 "왜?", "어떻게?"라고 생각하게 만든 구체적인 장면이나 대사를 요약합니다.) | (예: 의문제시형 후킹, 어그로 끌기, 모순된 상황 제시, 충격적인 비주얼 등 사용된 기법을 명시합니다.) |\n| 🧐 궁금증 증폭 (Deepening) | (중간 부분에서 처음의 궁금증이 더 커지거나, 새로운 의문이 더해지는 과정을 요약합니다.) | (예: 예상 밖의 변수 등장, 상반된 정보 제공, 의도적인 단서 숨기기 등 사용된 기법을 명시합니다.) |\n| 💡 궁금증 해소 (Payoff) | (결말 부분에서 궁금증이 해결되는 순간, 즉 '아하!'하는 깨달음을 주는 장면이나 정보를 요약합니다.) | (예: 반전 공개, 실험/분석 결과 제시, 명쾌한 원리 설명 등 사용된 기법을 명시합니다.) |\n\n7. 대본에서 전달하려는 핵심 메시지가 뭐야?\n\n8. 이야기 창작에 활용할 수 있도록, 원본 대본의 **'핵심 설정값'**을 아래 템플릿에 맞춰 추출하고 정리해 줘.\n[이야기 설정값 추출 템플릿]\n바꿀 수 있는 요소 (살)\n주인공 (누가):\n공간적 배경 (어디서):\n문제 발생 원인 (왜):\n갈등 대상 (누구와):\n유지할 핵심 요소 (뼈대)\n문제 상황:\n해결책:\n\n9. 이미지랑 같은 표 형식으로 만들어줘\n\n10. 여러 대본 동시 분석 요청\n...`
    );
}

function buildDopamineGraphPrompt() {
    return '다음 "문장 배열"에 대해, 각 문장별로 궁금증/도파민 유발 정도를 1~10 정수로 평가하고, 그 이유를 간단히 설명하세요. 반드시 JSON 배열로만, 요소는 {"sentence":"문장","level":정수,"reason":"이유"} 형태로 출력하세요. 여는 대괄호부터 닫는 대괄호까지 외 텍스트는 출력하지 마세요.';
}

function buildDopamineBatchPrompt(sentences) {
    const header = buildDopamineGraphPrompt();
    // 문장 배열(JSON)로 제공
    return header + '\n\n문장 배열:\n' + JSON.stringify(sentences);
}

function buildMaterialPrompt() {
    // 반드시 한 줄로 소재를 출력하도록 강제
    return '다음 대본의 핵심 소재를 한 문장으로 요약하세요. 반드시 한 줄로만, "소재: "로 시작하여 출력하세요. 다른 설명이나 불필요한 문자는 금지합니다.';
}

function splitTranscriptIntoSentences(text) {
    if (!text) return [];
    // 기본 정리
    let normalized = String(text).replace(/\r/g, '\n').replace(/\n{2,}/g, '\n').trim();
    // '>>' 제거 및 다중 공백 정리
    normalized = normalized.replace(/>{2,}/g, ' ').replace(/\s{2,}/g, ' ').trim();
    // 줄 단위로 나눈 뒤 숫자만 있는 라인은 제거
    const lines = normalized.split('\n').map(l => l.trim()).filter(l => l && !/^\d+(\.\d+)?$/.test(l));
    // 문장 내부 개행은 공백으로 합치고, 문장 끝(.,!,?, …) 뒤 개행은 그대로 경계로 유지되도록 마킹
    const joined = lines
        .map(l => l)
        .join('\n')
        // 문장 끝 표시: 마침표/물음표/느낌표/줄임표 뒤 개행을 특수 토큰으로 바꿈
        .replace(/([\.\?\!…])\s*\n+/g, '$1__SENT_BR__')
        // 그 외 개행은 공백으로 접합
        .replace(/\n+/g, ' ')
        // 특수 토큰은 실제 경계로 환원
        .replace(/__SENT_BR__/g, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim();
    // 문장 분할: 마침표/물음표/느낌표/줄임표 기준
    return joined
        .split(/(?<=[\.\?\!…])\s+/)
        .map(s => s.trim())
        .filter(Boolean);
}

async function analyzeDopamineByBatches(sentences, onLog) {
    const batchSize = 30;
    const results = [];
    let batchIndex = 0;
    for (let i = 0; i < sentences.length; i += batchSize) {
        batchIndex++;
        const batch = sentences.slice(i, i + batchSize);
        onLog && onLog(`도파민 분석 배치 ${batchIndex} (${i + 1}~${Math.min(i + batch.length, sentences.length)}/${sentences.length})`);
        const text = await callGeminiAPI(buildDopamineBatchPrompt(batch), '');
        let arr = [];
        try {
            arr = JSON.parse(text);
        } catch {
            const m = text.match(/\[([\s\S]*?)\]/);
            if (m) {
                try { arr = JSON.parse('[' + m[1] + ']'); } catch {}
            }
        }
        if (Array.isArray(arr)) {
            for (const item of arr) {
                const sentence = (item.sentence || item.text || '').toString();
                const levelNum = Number(item.level ?? item.score ?? 0);
                const level = isFinite(levelNum) ? Math.max(1, Math.min(10, Math.round(levelNum))) : 1;
                const reason = (item.reason || item.why || '').toString();
                if (sentence) results.push({ sentence, level, reason });
            }
        }
    }
    return results;
}

function showAnalysisBanner(message) {
    if (analysisBanner) analysisBanner.classList.remove('hidden');
    if (analysisBannerText) analysisBannerText.textContent = message || '';
    if (analysisProgressBar) analysisProgressBar.style.width = '0%';
    if (analysisLogEl) analysisLogEl.textContent = '';
}

function updateAnalysisProgress(done, total, suffixText) {
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    if (analysisProgressBar) analysisProgressBar.style.width = pct + '%';
    if (analysisBannerText) analysisBannerText.textContent = `진행률 ${done}/${total} (${pct}%)` + (suffixText ? ` — ${suffixText}` : '');
}

function appendAnalysisLog(line) {
    if (!analysisLogEl) return;
    const time = new Date().toLocaleTimeString();
    analysisLogEl.textContent += `[${time}] ${line}\n`;
    analysisLogEl.scrollTop = analysisLogEl.scrollHeight;
}

function appendScheduleLog(line) {
    if (!scheduleLogEl) return;
    const time = new Date().toLocaleTimeString();
    scheduleLogEl.textContent += `[${time}] ${line}\n`;
    scheduleLogEl.scrollTop = scheduleLogEl.scrollHeight;
}

async function callGeminiAPI(systemPrompt, userContent) {
    const key = getStoredGeminiKey();
    if (!key) throw new Error('Gemini API 키가 설정되지 않았습니다.');
    const model = 'models/gemini-1.5-pro-latest';
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/${model}:generateContent?key=${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            contents: [
                { role: 'user', parts: [{ text: systemPrompt + "\n\n" + userContent }] }
            ],
            generationConfig: { temperature: 0.3 }
        })
    });
    if (!res.ok) throw new Error('Gemini 호출 실패: ' + res.status);
    const data = await res.json();
    // v1beta response parsing
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    return text;
}

async function analyzeOneVideo(video) {
    const youtubeUrl = video.youtube_url;
    if (!youtubeUrl) throw new Error('YouTube URL 없음');
    appendAnalysisLog(`(${video.id}) 자막 추출 시작`);
    const transcript = await fetchTranscriptByUrl(youtubeUrl);
    appendAnalysisLog(`(${video.id}) 자막 길이 ${transcript.length}자`);
    appendAnalysisLog(`(${video.id}) 자막 미리보기: ${transcript.slice(0, 120).replace(/\n/g, ' ')}${transcript.length > 120 ? '...' : ''}`);
    const sentences = splitTranscriptIntoSentences(transcript);
    appendAnalysisLog(`(${video.id}) 문장 분해 ${sentences.length}개`);

    // 1) 소재 분석 (우선 수행)
    appendAnalysisLog(`(${video.id}) 소재 분석 시작`);
    const materialOnly = await callGeminiAPI(buildMaterialPrompt(), transcript);

    // 2) 템플릿 분석 (후킹요소/기승전결)
    appendAnalysisLog(`(${video.id}) 템플릿 분석 시작`);
    const analysisText = await callGeminiAPI(buildAnalysisPrompt(), transcript);

    // 3) 카테고리 분석 (KR/EN/CN)
    appendAnalysisLog(`(${video.id}) 카테고리 분석 시작`);
    const categoriesText = await callGeminiAPI(buildCategoryPrompt(), transcript);

    // 4) 키워드 분석 (KO/EN/ZH)
    appendAnalysisLog(`(${video.id}) 키워드 분석 시작`);
    const keywordsText = await callGeminiAPI(
        buildKeywordsPrompt(),
        `제목:\n${video.title || ''}\n\n대본:\n${transcript}`
    );

    // 5) 도파민 그래프 분석(JSON)
    appendAnalysisLog(`(${video.id}) 도파민 분석 시작`);
    const dopamineGraph = await analyzeDopamineByBatches(sentences, appendAnalysisLog);
    if (!Array.isArray(dopamineGraph) || dopamineGraph.length === 0) {
        appendAnalysisLog(`(${video.id}) 도파민 결과가 비어 있습니다 (파싱 실패 가능)`);
    }

    // 결과 매핑
    const updated = { ...video };
    updated.analysis_full = analysisText;
    updated.dopamine_graph = dopamineGraph;
    updated.analysis_transcript_len = transcript.length;
    updated.transcript_text = transcript;

    function extractLine(regex, text) {
        const m = text.match(regex); return m ? (m[1] || m[0]).trim() : '';
    }

    // 후킹요소/기승전결 (템플릿 분석 기반)
    const hookFromAnalysis = extractHookFromAnalysis(analysisText);
    updated.hooking = hookFromAnalysis || extractLine(/후킹\s*요소?\s*[:：]\s*(.+)/i, analysisText) || updated.hooking;
    const conciseNarrative = extractConciseNarrative(analysisText);
    updated.narrative_structure = conciseNarrative || '없음';

    // 카테고리 (KR/EN/CN)
    updated.kr_category_large = extractLine(/한국\s*대\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_large;
    updated.kr_category_medium = extractLine(/한국\s*중\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_medium;
    updated.kr_category_small = extractLine(/한국\s*소\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_small;
    updated.en_category_main = extractLine(/EN\s*Main\s*Category\s*[:：]\s*(.+)/i, categoriesText) || updated.en_category_main;
    updated.en_category_sub = extractLine(/EN\s*Sub\s*Category\s*[:：]\s*(.+)/i, categoriesText) || updated.en_category_sub;
    updated.en_micro_topic = extractLine(/EN\s*Micro\s*Topic\s*[:：]\s*(.+)/i, categoriesText) || updated.en_micro_topic;
    updated.cn_category_large = extractLine(/중국\s*대\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_large;
    updated.cn_category_medium = extractLine(/중국\s*중\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_medium;
    updated.cn_category_small = extractLine(/중국\s*소\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_small;

    // 키워드 (KO/EN/ZH)
    const { ko: kwKO, en: kwEN, zh: kwZH } = parseKeywordsText(keywordsText);
    if (kwKO.length || kwEN.length || kwZH.length) {
        appendAnalysisLog(`(${video.id}) 키워드 추출 완료 — KO:${kwKO.length}, EN:${kwEN.length}, ZH:${kwZH.length}`);
    } else {
        appendAnalysisLog(`(${video.id}) 키워드 결과가 비어 있습니다 (파싱 확인 필요)`);
    }
    updated.keywords_ko = kwKO;
    updated.keywords_en = kwEN;
    updated.keywords_zh = kwZH;

    // 소재: Gemini 강제 출력 + 비었을 때 보조 규칙
    let materialCandidate = extractLine(/소재\s*[:：]\s*(.+)/i, materialOnly) || (materialOnly || '').trim();
    if (!materialCandidate) {
        materialCandidate = inferMaterialFromContext(updated, transcript, analysisText, sentences);
    }
    updated.material = materialCandidate || updated.material || '';

    return { updated, raw: { categoriesText, analysisText, dopamineGraph, transcript } };
}

function extractHookFromAnalysis(analysisText) {
    try {
        const lines = String(analysisText).split('\n');
        for (const line of lines) {
            // 표 행 전체에서 Hook 행을 탐색
            const m = line.match(/^\|\s*[^|]*궁금증\s*유발[^|]*\|\s*([^|]+)\|/);
            if (m) return m[1].trim();
        }
    } catch {}
    return '';
}

function extractConciseNarrative(analysisText) {
    try {
        const lines = String(analysisText).split('\n').filter(l => l.trim().startsWith('|'));
        for (const line of lines) {
            const cells = line.split('|').map(s => s.trim());
            if (cells.length < 9) continue;
            const gi = cells[2];
            const seung = cells[3];
            const jeon = cells[4];
            const gyeol = cells[5];
            const feature = cells[6];
            const matchCell = cells[7];
            if (/✅/.test(matchCell)) return `기: ${gi} | 승: ${seung} | 전: ${jeon} | 결: ${gyeol} | 특징: ${feature}`;
        }
        return '';
    } catch { return ''; }
}

function inferMaterialFromContext(updated, transcript, analysisText, sentences) {
    // 카테고리 기반 우선
    const candidates = [
        updated.kr_category_small,
        updated.kr_category_medium,
        updated.kr_category_large,
        updated.en_micro_topic,
        updated.en_category_main
    ].filter(Boolean);
    if (candidates.length) return String(candidates[0]).slice(0, 60);
    // 대본 첫 문장 기반 보조
    const first = (Array.isArray(sentences) && sentences[0]) ? sentences[0] : (transcript || '').split(/\n+/)[0];
    return String(first || '').slice(0, 60);
}

function parseKeywordsText(text) {
    function sanitizeJson(input) {
        return String(input)
            .replace(/^```json\s*/i, '')
            .replace(/^```\s*/i, '')
            .replace(/```\s*$/i, '')
            .trim();
    }
    function normalizeArray(value) {
        const arr = Array.isArray(value) ? value :
            (typeof value === 'string' ? value.split(/[\n,，]/) : []);
        const seen = new Set();
        const out = [];
        for (const it of arr) {
            const v = (typeof it === 'string' ? it : (it && (it.keyword || it.text || it.name))) || '';
            const s = String(v).replace(/["'#]/g, '').trim();
            if (!s) continue;
            const key = s.toLowerCase();
            if (seen.has(key)) continue;
            seen.add(key);
            out.push(s);
        }
        return out.slice(0, 20);
    }
    try {
        let payload = sanitizeJson(text);
        let obj = null;
        try { obj = JSON.parse(payload); } catch {
            const m = payload.match(/\{[\s\S]*\}/);
            if (m) {
                try { obj = JSON.parse(m[0]); } catch {}
            }
        }
        const ko = normalizeArray(obj?.ko);
        const en = normalizeArray(obj?.en);
        const zh = normalizeArray(obj?.zh || obj?.cn);
        return { ko, en, zh };
    } catch {
        return { ko: [], en: [], zh: [] };
    }
}

async function runAnalysisForIds(ids) {
    analysisStatus.style.display = 'block';
    analysisStatus.style.color = '';
    analysisStatus.textContent = `분석 시작... (총 ${ids.length}개)`;
    showAnalysisBanner(`총 ${ids.length}개 분석 시작`);
    let done = 0, failed = 0;
    for (const id of ids) {
        try {
            const ref = doc(db, 'videos', id);
            const snap = await getDoc(ref);
            if (!snap.exists()) { failed++; continue; }
            const video = { id, ...snap.data() };
            appendAnalysisLog(`(${id}) 분석 시작`);
            const { updated } = await analyzeOneVideo(video);
            const payload = { ...updated };
            delete payload.id;
            await updateDoc(ref, payload);
            done++;
            analysisStatus.textContent = `진행중... ${done}/${ids.length} 완료`;
            updateAnalysisProgress(done, ids.length, `마지막 완료: ${video.title || id}`);
            appendAnalysisLog(`(${id}) 저장 완료`);
        } catch (e) {
            console.error('분석 실패', id, e);
            failed++;
            appendAnalysisLog(`(${id}) 오류: ${e.message || e}`);
        }
    }
    analysisStatus.style.color = failed ? 'orange' : 'green';
    analysisStatus.textContent = `분석 완료: 성공 ${done}, 실패 ${failed}`;
    updateAnalysisProgress(ids.length, ids.length, `성공 ${done}, 실패 ${failed}`);
    await fetchAndDisplayData();
}

if (runAnalysisSelectedBtn) {
    runAnalysisSelectedBtn.addEventListener('click', async () => {
        const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.dataset.id);
        if (ids.length === 0) { alert('분석할 항목을 선택하세요.'); return; }
        await runAnalysisForIds(ids);
    });
}

if (runAnalysisAllBtn) {
    runAnalysisAllBtn.addEventListener('click', async () => {
        const ids = currentData.map(v => v.id);
        if (ids.length === 0) { alert('분석할 데이터가 없습니다.'); return; }
        const confirmRun = confirm(`전체 ${ids.length}개 항목에 대해 분석을 실행할까요? 비용이 발생할 수 있습니다.`);
        if (!confirmRun) return;
        await runAnalysisForIds(ids);
    });
}

// ---------------- YouTube 댓글 수집/분석 ----------------
function getStoredYoutubeApiKeys() {
    try {
        const raw = localStorage.getItem('youtube_api_keys_list') || '';
        return raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    } catch {
        return [];
    }
}

function pickRotatingKey(keys, index) {
    if (!keys.length) return '';
    return keys[index % keys.length];
}

function extractVideoIdFromUrl(urlStr) {
    try {
        const url = new URL(urlStr);
        if (url.hostname.includes('youtu.be')) return url.pathname.split('/').pop();
        if (url.searchParams.get('v')) return url.searchParams.get('v');
        if (url.pathname.includes('/shorts/')) return url.pathname.split('/').pop();
        return '';
    } catch { return ''; }
}

async function fetchYoutubeComments(videoId, maxCount, keys) {
    const out = [];
    let pageToken = '';
    let reqIndex = 0;
    let failureStreak = 0;
    const maxFailures = Math.max(3, keys.length * 3);
    while (out.length < maxCount) {
        const key = pickRotatingKey(keys, reqIndex++);
        if (!key) throw new Error('YouTube API 키가 설정되지 않았습니다. 설정 탭에서 키를 저장하세요.');
        const remaining = maxCount - out.length;
        const pageSize = Math.max(1, Math.min(100, remaining));
        const url = new URL('https://www.googleapis.com/youtube/v3/commentThreads');
        url.searchParams.set('part', 'snippet,replies');
        url.searchParams.set('videoId', videoId);
        url.searchParams.set('maxResults', String(pageSize));
        url.searchParams.set('order', 'relevance');
        url.searchParams.set('key', key);
        if (pageToken) url.searchParams.set('pageToken', pageToken);
        const res = await fetch(url.toString());
        if (!res.ok) {
            failureStreak++;
            if (failureStreak >= maxFailures) {
                throw new Error('YouTube API 오류 반복: ' + res.status);
            }
            // 다른 키로 재시도
            continue;
        }
        const data = await res.json();
        failureStreak = 0;
        const items = Array.isArray(data.items) ? data.items : [];
        for (const it of items) {
            try {
                const sn = it.snippet?.topLevelComment?.snippet;
                if (!sn) continue;
                out.push({
                    author: sn.authorDisplayName || '',
                    text: sn.textDisplay || sn.textOriginal || '',
                    likeCount: Number(sn.likeCount || 0),
                    publishedAt: sn.publishedAt || '',
                    updatedAt: sn.updatedAt || '',
                    authorProfileImageUrl: sn.authorProfileImageUrl || '',
                    authorChannelUrl: sn.authorChannelUrl || ''
                });
                if (out.length >= maxCount) break;
            } catch {}
        }
        if (out.length >= maxCount) break;
        pageToken = data.nextPageToken || '';
        if (!pageToken) break;
    }
    return out;
}

function pickTopComments(comments, topN = 10) {
    const arr = Array.isArray(comments) ? comments.slice() : [];
    arr.sort((a, b) => (Number(b.likeCount || 0) - Number(a.likeCount || 0)));
    return arr.slice(0, Math.max(1, topN));
}

async function runCommentsAnalysisForIds(ids) {
    if (!commentsAnalysisStatus) return;
    commentsAnalysisStatus.style.display = 'block';
    commentsAnalysisStatus.style.color = '';
    const wantCount = Math.max(1, Math.min(1000, Number(commentCountInput?.value || 50)));
    const keys = getStoredYoutubeApiKeys();
    if (!keys.length) { commentsAnalysisStatus.textContent = 'YouTube API 키가 없습니다. 설정 탭에서 저장하세요.'; return; }
    commentsAnalysisStatus.textContent = `댓글 수집 시작... (총 ${ids.length}개, 각 ${wantCount}개)`;
    showAnalysisBanner(`댓글 수집: 총 ${ids.length}개 대상`);
    let done = 0, failed = 0;
    for (const id of ids) {
        try {
            const ref = doc(db, 'videos', id);
            const snap = await getDoc(ref);
            if (!snap.exists()) { failed++; continue; }
            const video = { id, ...snap.data() };
            const vid = extractVideoIdFromUrl(video.youtube_url || '');
            if (!vid) { failed++; appendAnalysisLog(`(${id}) YouTube ID 파싱 실패`); continue; }
            appendAnalysisLog(`(${id}) 댓글 수집 시작 (최대 ${wantCount}개)`);
            const comments = await fetchYoutubeComments(vid, wantCount, keys);
            appendAnalysisLog(`(${id}) 댓글 수집 완료 ${comments.length}개`);
            const top = pickTopComments(comments, 20);
            const payload = { comments_fetched_at: Date.now(), comments_total: comments.length, comments_top: top };
            await updateDoc(ref, payload);
            done++;
            updateAnalysisProgress(done, ids.length, `댓글 완료: ${video.title || id}`);
        } catch (e) {
            failed++;
            appendAnalysisLog(`(${id}) 댓글 오류: ${e.message || e}`);
        }
    }
    commentsAnalysisStatus.style.color = failed ? 'orange' : 'green';
    commentsAnalysisStatus.textContent = `댓글 수집 완료: 성공 ${done}, 실패 ${failed}`;
    await fetchAndDisplayData();
}

if (runCommentsSelectedBtn) {
    runCommentsSelectedBtn.addEventListener('click', async () => {
        const ids = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.dataset.id);
        if (ids.length === 0) { alert('댓글을 수집할 항목을 선택하세요.'); return; }
        await runCommentsAnalysisForIds(ids);
    });
}


// 파일 업로드 및 드래그앤드롭 로직
function handleFile(file) {
    if (file) {
        const validExtensions = ['csv', 'xlsx'];
        const fileExtension = file.name.split('.').pop().toLowerCase();
        if (validExtensions.includes(fileExtension)) {
            selectedFile = file;
            fileNameDisplay.textContent = `선택된 파일: ${file.name}`;
            fileNameDisplay.classList.add('active');
        } else {
            alert('CSV 또는 XLSX 파일만 업로드할 수 있습니다.');
            selectedFile = null;
            fileNameDisplay.textContent = '';
            fileNameDisplay.classList.remove('active');
        }
    }
}

fileInput.addEventListener('change', () => handleFile(fileInput.files[0]));

['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    fileDropArea.addEventListener(eventName, (e) => { e.preventDefault(); e.stopPropagation(); });
});
['dragenter', 'dragover'].forEach(eventName => {
    fileDropArea.addEventListener(eventName, () => fileDropArea.classList.add('dragover'));
});
['dragleave', 'drop'].forEach(eventName => {
    fileDropArea.addEventListener(eventName, () => fileDropArea.classList.remove('dragover'));
});
fileDropArea.addEventListener('drop', (e) => handleFile(e.dataTransfer.files[0]));

uploadBtn.addEventListener('click', () => {
    if (!selectedFile) {
        uploadStatus.textContent = 'CSV 또는 XLSX 파일을 선택해주세요.';
        uploadStatus.style.color = 'red';
        return;
    }
    uploadStatus.textContent = '파일 처리 중...';
    const fileExtension = selectedFile.name.split('.').pop().toLowerCase();
    if (fileExtension === 'csv') {
        Papa.parse(selectedFile, {
            header: true, skipEmptyLines: true,
            complete: (results) => processDataAndUpload(results.data),
            error: (err) => { uploadStatus.textContent = `CSV 파싱 오류: ${err.message}`; }
        });
    } else if (fileExtension === 'xlsx') {
        const reader = new FileReader();
        reader.onload = (e) => {
            const workbook = XLSX.read(e.target.result, { type: 'array' });
            const jsonData = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
            processDataAndUpload(jsonData);
        };
        reader.readAsArrayBuffer(selectedFile);
    }
});

async function processDataAndUpload(data) {
    uploadStatus.textContent = '데이터 등록 중...';
    const uploadBatch = writeBatch(db);
    let count = 0;
    data.forEach(row => {
        if (!row.Title || !row['YouTube URL']) return;
        const videoData = {
            thumbnail: row.Thumbnail || '',
            title: row.Title || '',
            views: row.Views || '',
            views_numeric: Number(row.Views_numeric) || 0,
            channel: row.Channel || '',
            date: row.Date || '',
            subscribers: row.Subscribers || '',
            subscribers_numeric: Number(row.Subscribers_numeric) || 0,
            hash: row.Hash || '',
            youtube_url: row['YouTube URL'] || '',
            group_name: row.group_name || '',
            // 템플릿 유형만 엑셀에서 유지
            template_type: row['템플릿 유형'] || ''
            // 아래 필드들은 엑셀에서 받지 않고, Gemini 분석으로 채웁니다
            // material, hooking, narrative_structure,
            // kr_category_large/medium/small,
            // en_category_main/sub, en_micro_topic,
            // cn_category_large/medium/small,
            // source_type
        };
        const docId = row.Hash || row.Title.replace(/[^a-zA-Z0-9]/g, '');
        uploadBatch.set(doc(db, 'videos', docId), videoData);
        count++;
    });
    await uploadBatch.commit();
    uploadStatus.textContent = `${count}개의 데이터 추가/업데이트 완료!`;
    uploadStatus.style.color = 'green';
    selectedFile = null;
    fileNameDisplay.textContent = '';
    fileNameDisplay.classList.remove('active');
    fetchAndDisplayData();
}
