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
    signInWithEmailAndPassword(auth, email, password).catch(error => {
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
                <th>썸네일</th><th>제목</th><th>채널</th><th>게시일</th><th>관리</th>
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
            editForm.innerHTML += `
                <div class="form-group">
                    <label for="edit-${key}">${key}</label>
                    <input type="text" id="edit-${key}" name="${key}" value="${data[key]}">
                </div>
            `;
        });
        editModal.classList.remove('hidden');
    }
};
const closeEditModal = () => editModal.classList.add('hidden');

saveEditBtn.addEventListener('click', async () => {
    const updatedData = {};
    new FormData(editForm).forEach((value, key) => { updatedData[key] = value; });
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
