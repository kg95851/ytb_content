import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, collection, getDocs, query } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

// Firebase 초기화
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

// DOM 요소 (video-grid 대신 video-table-body 사용)
const videoTableBody = document.getElementById('video-table-body');
// 페이지 이동 시 스크롤 위치 조정을 위해 컨테이너 참조
const videoTableContainer = document.getElementById('video-table-container');
const searchInput = document.getElementById('searchInput');
const formTypeFilter = document.getElementById('form-type-filter');
const startDateFilter = document.getElementById('start-date-filter');
const endDateFilter = document.getElementById('end-date-filter');
const sortFilter = document.getElementById('sort-filter');
const paginationContainer = document.getElementById('pagination-container');

let allVideos = [];
let filteredVideos = [];
let currentPage = 1;
const itemsPerPage = 100;

// Firestore에서 비디오 데이터 가져오기
const fetchVideos = async () => {
    // 로딩 상태 표시
    if (videoTableBody) videoTableBody.innerHTML = '<tr><td colspan="7" class="info-message">데이터를 불러오는 중...</td></tr>';
    try {
        const videosCollection = collection(db, 'videos');
        const q = query(videosCollection);
        const querySnapshot = await getDocs(q);
        allVideos = querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        applyFiltersAndSort();
    } catch (error) {
        console.error("Error fetching videos: ", error);
        if (videoTableBody) {
             videoTableBody.innerHTML = '<tr><td colspan="7" class="error-message">데이터를 불러오는 데 실패했습니다. Firebase 설정을 확인해주세요.</td></tr>';
        }
    }
};

// 필터링 및 정렬 적용 함수 (기존 로직 유지)
const applyFiltersAndSort = () => {
    filteredVideos = [...allVideos];

    // 1. 검색어 필터
    const searchTerm = searchInput.value.toLowerCase();
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
    
    // 2. 폼 유형 필터
    const formType = formTypeFilter.value;
    if (formType !== 'all') {
        filteredVideos = filteredVideos.filter(video => video.group_name === formType);
    }

    // 3. 게시일 필터
    const startDate = startDateFilter.value;
    const endDate = endDateFilter.value;
    if (startDate) {
        filteredVideos = filteredVideos.filter(video => video.date && video.date >= startDate);
    }
    if (endDate) {
        filteredVideos = filteredVideos.filter(video => video.date && video.date <= endDate);
    }

    // 4. 정렬
    const sortValue = sortFilter.value;
    filteredVideos.sort((a, b) => {
        switch (sortValue) {
            case 'views_desc': return (b.views_numeric || 0) - (a.views_numeric || 0);
            case 'views_asc': return (a.views_numeric || 0) - (b.views_numeric || 0);
            case 'subs_desc': return (b.subscribers_numeric || 0) - (a.subscribers_numeric || 0);
            case 'subs_asc': return (a.subscribers_numeric || 0) - (b.subscribers_numeric || 0);
            case 'date_desc':
            default:
                 // 날짜 비교 시 유효성 검사 강화
                const dateA = a.date ? new Date(a.date) : new Date(0);
                const dateB = b.date ? new Date(b.date) : new Date(0);
                return dateB - dateA;
        }
    });
    
    currentPage = 1;
    displayVideosPage();
    renderPagination();
};

// 현재 페이지의 비디오 목록을 표시하는 함수 (테이블 뷰로 업데이트됨)
const displayVideosPage = () => {
    if (!videoTableBody) return;

    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageVideos = filteredVideos.slice(startIndex, endIndex);

    if (pageVideos.length === 0) {
        videoTableBody.innerHTML = '<tr><td colspan="7" class="info-message">조건에 맞는 영상이 없습니다.</td></tr>';
        return;
    }

    // 테이블 행 생성
    const rowsHtml = pageVideos.map(video => {
        const thumbnailHTML = video.thumbnail
            ? `<img src="${video.thumbnail}" alt="${video.title}" class="table-thumbnail" loading="lazy" onerror="this.outerHTML = \`<div class='no-thumbnail-placeholder'>이미지 없음</div>\`">`
            : `<div class="no-thumbnail-placeholder">이미지 없음</div>`;

        // 카테고리 표시 (대 카테고리 기준)
        const category = video.kr_category_large || '없음';

        // '자세히 보기' 링크 추가 (target="_blank"로 새 창 열기)
        return `
            <tr>
                <td>${thumbnailHTML}</td>
                <td class="table-title">${video.title || '제목 없음'}</td>
                <td>${video.channel || '채널 없음'}</td>
                <td>${(video.views_numeric || 0).toLocaleString()}회</td>
                <td>${video.date || '없음'}</td>
                <td>${category}</td>
                <td>
                    <a href="details.html?id=${video.id}" class="btn btn-details" target="_blank">자세히 보기</a>
                </td>
            </tr>
        `;
    }).join('');

    videoTableBody.innerHTML = rowsHtml;
};

// 페이지네이션 UI 렌더링 함수
const renderPagination = () => {
    paginationContainer.innerHTML = '';
    const totalPages = Math.ceil(filteredVideos.length / itemsPerPage);

    if (totalPages <= 1) return;

    const changePage = (newPage) => {
        currentPage = newPage;
        displayVideosPage();
        renderPagination();
        // 페이지 변경 시 테이블 상단으로 스크롤
        if (videoTableContainer) {
            videoTableContainer.scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else {
            window.scrollTo(0, 0);
        }
    };

    const prevButton = document.createElement('button');
    prevButton.textContent = '이전';
    prevButton.className = 'pagination-btn';
    prevButton.disabled = currentPage === 1;
    prevButton.addEventListener('click', () => {
        if (currentPage > 1) {
            changePage(currentPage - 1);
        }
    });

    const nextButton = document.createElement('button');
    nextButton.textContent = '다음';
    nextButton.className = 'pagination-btn';
    nextButton.disabled = currentPage === totalPages;
    nextButton.addEventListener('click', () => {
        if (currentPage < totalPages) {
            changePage(currentPage + 1);
        }
    });
    
    const pageInfo = document.createElement('span');
    pageInfo.className = 'page-info';
    pageInfo.textContent = `${currentPage} / ${totalPages}`;

    paginationContainer.appendChild(prevButton);
    paginationContainer.appendChild(pageInfo);
    paginationContainer.appendChild(nextButton);
};


// 필터 이벤트 리스너 등록 (기존과 동일)
[searchInput, formTypeFilter, startDateFilter, endDateFilter, sortFilter].forEach(el => {
    if (el) {
        el.addEventListener('input', applyFiltersAndSort);
        if (el.tagName === 'SELECT' || el.type === 'date') {
            el.addEventListener('change', applyFiltersAndSort);
        }
    }
});

// 초기 데이터 로드
fetchVideos();
