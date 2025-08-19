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

function getStoredGeminiKey() {
    try { return localStorage.getItem(GEMINI_KEY_STORAGE) || ''; } catch { return ''; }
}
function setStoredGeminiKey(key) {
    try { localStorage.setItem(GEMINI_KEY_STORAGE, key || ''); } catch {}
}
function getTranscriptServerUrl() {
    try {
        const saved = localStorage.getItem(TRANSCRIPT_SERVER_STORAGE);
        if (saved) return saved;
        const isLocal = /localhost|127\.0\.0\.1/.test(window.location.hostname);
        return isLocal ? 'http://localhost:8787' : '/api';
    } catch {
        const isLocal = /localhost|127\.0\.0\.1/.test(window.location.hostname);
        return isLocal ? 'http://localhost:8787' : '/api';
    }
}
function setTranscriptServerUrl(url) {
    try { localStorage.setItem(TRANSCRIPT_SERVER_STORAGE, url || 'http://localhost:8787'); } catch {}
}

// 초기화: 저장된 값 복원
window.addEventListener('DOMContentLoaded', () => {
    const savedKey = getStoredGeminiKey();
    if (geminiKeyInput && savedKey) geminiKeyInput.value = savedKey;
    const savedServer = getTranscriptServerUrl();
    if (transcriptServerInput) transcriptServerInput.value = savedServer;
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

// ---------------- Analysis Runner ----------------
async function fetchTranscriptByUrl(youtubeUrl) {
    const server = getTranscriptServerUrl();
    const res = await fetch(server.replace(/\/$/, '') + '/transcript?url=' + encodeURIComponent(youtubeUrl));
    if (!res.ok) throw new Error('Transcript fetch failed: ' + res.status);
    const data = await res.json();
    // 기대 형식: { text: "..." }
    return data.text || '';
}

function buildCategoryPrompt() {
    return (
`한국의 대,중,소 카테고리, EN카테고리 MAIN CATEGORY, SUB CATEGORY, MICRO TOPIC, 중국 버전 대중소 카테고리도 같이 작성해줘`
    );
}

function buildAnalysisPrompt() {
    return (
`[GPTs Instructions 최종안]\n\n페르소나 (Persona)\n\n당신은 "대본분석_룰루랄라릴리"입니다. 유튜브 대본을 분석하여 콘텐츠 전략 수립과 프롬프트 최적화를 돕는 최고의 전문가입니다. 당신의 답변은 항상 체계적이고, 깔끔하며, 사용자가 바로 활용할 수 있도록 완벽하게 구성되어야 합니다.\n\n핵심 임무 (Core Mission)\n\n사용자가 유튜브 대본(영어 또는 한국어)을 입력하면, 아래 4번 항목의 **[출력 템플릿]**을 단 하나의 글자나 기호도 틀리지 않고 그대로 사용하여 분석 결과를 제공해야 합니다.\n\n절대 규칙 (Golden Rules)\n\n규칙 1: 템플릿 복제 - 출력물의 구조, 디자인, 순서, 항목 번호, 이모지(✨, 📌, 🎬, 🧐, 💡, ✅, 🤔), 강조(), 구분선(*) 등 모든 시각적 요소를 아래 **[출력 템플릿]**과 완벽하게 동일하게 재현해야 합니다.\n\n규칙 2: 순서 및 항목 준수 - 항상 0번, 1번, 2번, 3번, 4번, 5번, 6번, 7번, 8번,9번 항목을 빠짐없이, 순서대로 포함해야 합니다.\n\n규칙 3: 표 형식 유지 - 분석 내용의 대부분은 마크다운 표(Table)로 명확하게 정리해야 합니다.\n\n규칙 4: 내용의 구체성 - 각 항목에 필요한 분석 내용을 충실히 채워야 합니다. 특히 프롬프트 비교 시, 단순히 '유사함'에서 그치지 말고 이유를 명확히 설명해야 합니다.\n\n출력 템플릿 (Output Template) - 이 틀을 그대로 사용하여 답변할 것\n\n✨ 룰루 GPTs 분석 템플릿 적용 결과\n\n0. 대본 번역 (영어 → 한국어)\n(여기에 자연스러운 구어체 한국어 번역문을 작성한다.)\n\n1. 대본 기승전결 분석\n| 구분 | 내용 |\n| :--- | :--- |\n| 기 (상황 도입) | (여기에 '기'에 해당하는 내용을 요약한다.) |\n| 승 (사건 전개) | (여기에 '승'에 해당하는 내용을 요약한다.) |\n| 전 (위기/전환) | (여기에 '전'에 해당하는 내용을 요약한다.) |\n| 결 (결말) | (여기에 '결'에 해당하는 내용을 요약한다.) |\n\n2. 기존 프롬프트와의 미스매치 비교표\n| 프롬프트 번호 | 기 (문제 제기) | 승 (예상 밖 전개) | 전 (몰입·긴장 유도) | 결 (결론/인사이트) | 특징 | 미스매치 여부 |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n| 001 | 욕망 자극 | 수상한 전개 | 반전 | 허무/반전 결말 | 욕망+반전+유머 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 002 | 일상 시작 | 실용적 해결 | 낯선 기술 | 꿀팁 or 정리 | 실용+공감 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 003 | 위기 상황 | 극한 도전 | 생존 위기 | 실패 or 생존법 | 생존+경고 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 004 | 문화 충돌 | 오해 과정 | 이해 확장 | 감동 | 문화+인식 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 005 | 이상 행동 | 분석 진행 | 시각 변화 | 진실 발견 | 반전+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 006 | 멀쩡해 보임 | 내부 파헤침 | 충격 실체 | 소비자 경고 | 사기+정보 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 007 | 실패할 도전 | 이상한 방식 | 몰입 상황 | 교훈 전달 | 도전+극복 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 008 | 자연 속 상황 | 생존 시도 | 변수 등장 | 생존 기술 | 자연+실용 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 009 | 흔한 장소 | 이상한 디테일 | 공포 증가 | 붕괴 경고 | 위기+공포 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n| 010 | '진짜일까?' | 실험/분석 | 반전 | 허세 or 실속 | 비교+분석 | (대본과 비교하여 ✅ 또는 ❌ 유사로 표시) |\n\n3. 대본 vs 비슷하거나 똑같은 기존 프롬프트 비교\n→ 유사 프롬프트: (여기에 2번에서 '✅ 유사'로 표시한 프롬프트 번호와 제목을 기재한다.)\n| 구분 | 🎬 대본 내용 | 📌 기존 프롬프트 (00X번) |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (유사 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (유사 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (유사 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (유사 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (유사 프롬프트의 전반적인 특징) |\n차이점 요약\n→ (여기에 대본과 유사 프롬프트의 핵심적인 차이점을 명확하게 요약하여 작성한다.)\n\n4. 대본 vs 새롭게 제안한 프롬프트 비교\n제안 프롬프트 제목: “(여기에 대본에 가장 잘 맞는 새로운 프롬프트 제목을 창의적으로 작성한다.)” 스토리 구조\n| 구분 | 🎬 대본 내용 | 💡 제안 프롬프트 |\n| :--- | :--- | :--- |\n| 기 | (대본의 '기' 요약) | (새 프롬프트의 '기' 특징) |\n| 승 | (대본의 '승' 요약) | (새 프롬프트의 '승' 특징) |\n| 전 | (대본의 '전' 요약) | (새 프롬프트의 '전' 특징) |\n| 결 | (대본의 '결' 요약) | (새 프롬프트의 '결' 특징) |\n| 특징 | (대본의 전반적인 특징) | (새 프롬프트의 전반적인 특징) |\n이 프롬프트의 강점\n→ (여기에 제안한 프롬프트가 왜 대본에 더 적합한지, 어떤 강점이 있는지 2~3가지 포인트로 설명한다.)\n\n5. 결론 요약\n| 항목 | 내용 |\n| :--- | :--- |\n| 기존 프롬프트 매칭 | (여기에 가장 유사한 프롬프트 번호와 함께, '정확히 일치하는 구조 없음' 등의 요약평을 작성한다.) |\n| 추가 프롬프트 필요성 | 필요함 — (여기에 왜 새로운 프롬프트가 필요한지 이유를 구체적으로 작성한다.) |\n| 새 프롬프트 제안 | (여기에 4번에서 제안한 프롬프트 제목과 핵심 특징을 요약하여 작성한다.) |\n| 활용 추천 분야 | (여기에 새 프롬프트가 어떤 종류의 콘텐츠에 활용될 수 있는지 구체적인 예시를 3~4가지 제시한다.) |\n\n6. 궁금증 유발 및 해소 과정 분석\n| 구분 | 내용 분석 (대본에서 어떻게 표현되었나?) | 핵심 장치 및 기법 |\n| :--- | :--- | :--- |\n| 🤔 궁금증 유발 (Hook) | (시작 부분에서 시청자가 "왜?", "어떻게?"라고 생각하게 만든 구체적인 장면이나 대사를 요약합니다.) | (예: 의문제시형 후킹, 어그로 끌기, 모순된 상황 제시, 충격적인 비주얼 등 사용된 기법을 명시합니다.) |\n| 🧐 궁금증 증폭 (Deepening) | (중간 부분에서 처음의 궁금증이 더 커지거나, 새로운 의문이 더해지는 과정을 요약합니다.) | (예: 예상 밖의 변수 등장, 상반된 정보 제공, 의도적인 단서 숨기기 등 사용된 기법을 명시합니다.) |\n| 💡 궁금증 해소 (Payoff) | (결말 부분에서 궁금증이 해결되는 순간, 즉 '아하!'하는 깨달음을 주는 장면이나 정보를 요약합니다.) | (예: 반전 공개, 실험/분석 결과 제시, 명쾌한 원리 설명 등 사용된 기법을 명시합니다.) |\n\n7. 대본에서 전달하려는 핵심 메시지가 뭐야?\n\n8. 이야기 창작에 활용할 수 있도록, 원본 대본의 **'핵심 설정값'**을 아래 템플릿에 맞춰 추출하고 정리해 줘.\n[이야기 설정값 추출 템플릿]\n바꿀 수 있는 요소 (살)\n주인공 (누가):\n공간적 배경 (어디서):\n문제 발생 원인 (왜):\n갈등 대상 (누구와):\n유지할 핵심 요소 (뼈대)\n문제 상황:\n해결책:\n\n9. 이미지랑 같은 표 형식으로 만들어줘\n\n10. 여러 대본 동시 분석 요청\n...`
    );
}

function buildDopamineGraphPrompt() {
    return '다음 대본을 문장별로 분해하여 사람들의 궁금증/도파민 유발 정도를 1~10 레벨로 평가하고, [문장, 레벨, 이유] 형태의 JSON 배열로 출력하세요.';
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
    const transcript = await fetchTranscriptByUrl(youtubeUrl);

    // 5-1 카테고리 산출
    const categoriesText = await callGeminiAPI(buildCategoryPrompt(), transcript);

    // 5-2 소재/후킹/기승전결 등 템플릿 분석
    const analysisText = await callGeminiAPI(buildAnalysisPrompt(), transcript);

    // 3 도파민 그래프 분석(JSON)
    const dopText = await callGeminiAPI(buildDopamineGraphPrompt(), transcript);
    let dopamineGraph = [];
    try { dopamineGraph = JSON.parse(dopText); } catch { /* 모델이 마크다운 코드를 섞는 경우 정리 시도 */
        const jsonMatch = dopText.match(/\[([\s\S]*?)\]/);
        if (jsonMatch) {
            try { dopamineGraph = JSON.parse('[' + jsonMatch[1] + ']'); } catch {}
        }
    }

    // 간단 파싱 규칙(유연 처리): 카테고리 키워드 추출
    const updated = { ...video };
    // 관리자 수동 검수 전용 필드에 원문 저장
    updated.analysis_full = analysisText;
    updated.dopamine_graph = dopamineGraph;
    updated.analysis_transcript_len = transcript.length;

    // 사용자가 기대한 필드 매핑을 위해 키워드 탐색(간단)
    function extractLine(regex, text) {
        const m = text.match(regex); return m ? (m[1] || m[0]).trim() : '';
    }

    // 예: "한국 대 카테고리: XXX" 같은 형식을 기대. 없다면 빈값 유지
    updated.kr_category_large = extractLine(/한국\s*대\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_large;
    updated.kr_category_medium = extractLine(/한국\s*중\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_medium;
    updated.kr_category_small = extractLine(/한국\s*소\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.kr_category_small;
    updated.en_category_main = extractLine(/EN\s*Main\s*Category\s*[:：]\s*(.+)/i, categoriesText) || updated.en_category_main;
    updated.en_category_sub = extractLine(/EN\s*Sub\s*Category\s*[:：]\s*(.+)/i, categoriesText) || updated.en_category_sub;
    updated.en_micro_topic = extractLine(/EN\s*Micro\s*Topic\s*[:：]\s*(.+)/i, categoriesText) || updated.en_micro_topic;
    updated.cn_category_large = extractLine(/중국\s*대\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_large;
    updated.cn_category_medium = extractLine(/중국\s*중\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_medium;
    updated.cn_category_small = extractLine(/중국\s*소\s*카테고리\s*[:：]\s*(.+)/i, categoriesText) || updated.cn_category_small;

    // 소재, 후킹, 기승전결은 analysisText에서 키워드로 추출(템플릿 유지 시 수월)
    updated.material = extractLine(/소재\s*[:：]\s*(.+)/i, analysisText) || updated.material;
    updated.hooking = extractLine(/후킹\s*요소?\s*[:：]\s*(.+)/i, analysisText) || updated.hooking;
    updated.narrative_structure = extractLine(/기승전결\s*구조\s*[:：]\s*(.+)/i, analysisText) || updated.narrative_structure;

    return { updated, raw: { categoriesText, analysisText, dopText, transcript } };
}

async function runAnalysisForIds(ids) {
    analysisStatus.style.display = 'block';
    analysisStatus.style.color = '';
    analysisStatus.textContent = `분석 시작... (총 ${ids.length}개)`;
    let done = 0, failed = 0;
    for (const id of ids) {
        try {
            const ref = doc(db, 'videos', id);
            const snap = await getDoc(ref);
            if (!snap.exists()) { failed++; continue; }
            const video = { id, ...snap.data() };
            const { updated } = await analyzeOneVideo(video);
            const payload = { ...updated };
            delete payload.id;
            await updateDoc(ref, payload);
            done++;
            analysisStatus.textContent = `진행중... ${done}/${ids.length} 완료`;
        } catch (e) {
            console.error('분석 실패', id, e);
            failed++;
        }
    }
    analysisStatus.style.color = failed ? 'orange' : 'green';
    analysisStatus.textContent = `분석 완료: 성공 ${done}, 실패 ${failed}`;
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
            kr_category_large: row['한국 대 카테고리'] || '',
            kr_category_medium: row['한국 중 카테고리'] || '',
            kr_category_small: row['한국 소 카테고리'] || '',
            en_category_main: row['EN Main Category'] || '',
            en_category_sub: row['EN Sub Category'] || '',
            en_micro_topic: row['EN Micro Topic'] || '',
            cn_category_large: row['중국 대 카테고리'] || '',
            cn_category_medium: row['중국 중 카테고리'] || '',
            cn_category_small: row['중국 소 카테고리'] || '',
            template_type: row['템플릿 유형'] || '',
            narrative_structure: row['기승전결'] || '',
            material: row['소재'] || '',
            source_type: row['원본'] || '',
            hooking: row['후킹'] || ''
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
