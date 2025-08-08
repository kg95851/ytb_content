import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, collection, getDocs, query } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

// Firebase 초기화
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

// DOM 요소
const videoGrid = document.getElementById('video-grid');
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
    try {
        const videosCollection = collection(db, 'videos');
        const q = query(videosCollection);
        const querySnapshot = await getDocs(q);
        allVideos = querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        applyFiltersAndSort();
    } catch (error) {
        console.error("Error fetching videos: ", error);
        videoGrid.innerHTML = '<p class="error-message">데이터를 불러오는 데 실패했습니다. Firebase 설정을 확인해주세요.</p>';
    }
};

// 필터링 및 정렬 적용 함수
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
                return new Date(b.date) - new Date(a.date);
        }
    });
    
    currentPage = 1;
    displayVideosPage();
    renderPagination();
};

// 현재 페이지의 비디오 목록을 표시하는 함수
const displayVideosPage = () => {
    videoGrid.innerHTML = '';
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageVideos = filteredVideos.slice(startIndex, endIndex);

    if (pageVideos.length === 0) {
        videoGrid.innerHTML = '<p class="info-message">조건에 맞는 영상이 없습니다.</p>';
        return;
    }

    pageVideos.forEach(video => {
        const card = document.createElement('div');
        card.className = 'video-card';

        const kr_categories = [video.kr_category_large, video.kr_category_medium, video.kr_category_small].filter(Boolean).join(' > ');
        
        const thumbnailHTML = video.thumbnail
            ? `<img src="${video.thumbnail}" alt="${video.title}" loading="lazy" onerror="this.outerHTML = \`<div class='no-thumbnail'>이미지 없음</div>\`">`
            : `<div class="no-thumbnail">이미지 없음</div>`;

        card.innerHTML = `
            <div class="card-thumbnail">
                ${thumbnailHTML}
                <a href="${video.youtube_url}" target="_blank" class="youtube-link">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.72"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.72-1.72"></path></svg>
                    <span>바로가기</span>
                </a>
            </div>
            <div class="card-content">
                <div class="card-header">
                    <span class="channel-name">${video.channel || '채널 정보 없음'}</span>
                    <span class="group-tag">${video.group_name || ''}</span>
                </div>
                <h3 class="card-title">${video.title}</h3>
                <div class="card-meta">
                    <div class="meta-item">
                        <span class="meta-label">조회수</span>
                        <span class="meta-value">${(video.views_numeric || 0).toLocaleString()}회</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">구독자</span>
                        <span class="meta-value">${(video.subscribers_numeric || 0).toLocaleString()}명</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">게시일</span>
                        <span class="meta-value">${video.date || '없음'}</span>
                    </div>
                </div>
                <p class="card-category">${kr_categories || '카테고리 없음'}</p>
                <div class="card-extra-info">
                    <div class="extra-item"><strong>소재:</strong> ${video.material || '없음'}</div>
                    <div class="extra-item"><strong>템플릿 유형:</strong> ${video.template_type || '없음'}</div>
                    <div class="extra-item"><strong>원본:</strong> ${video.source_type || '없음'}</div>
                    <div class="extra-item"><strong>후킹:</strong> ${video.hooking || '없음'}</div>
                    <div class="extra-item"><strong>기승전결:</strong> ${video.narrative_structure || '없음'}</div>
                </div>
                <button class="more-info-btn">더보기</button>
            </div>
        `;
        videoGrid.appendChild(card);
    });
};

// 페이지네이션 UI 렌더링 함수
const renderPagination = () => {
    paginationContainer.innerHTML = '';
    const totalPages = Math.ceil(filteredVideos.length / itemsPerPage);

    if (totalPages <= 1) return;

    const prevButton = document.createElement('button');
    prevButton.textContent = '이전';
    prevButton.className = 'pagination-btn';
    prevButton.disabled = currentPage === 1;
    prevButton.addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            displayVideosPage();
            renderPagination();
            window.scrollTo(0, 0);
        }
    });

    const nextButton = document.createElement('button');
    nextButton.textContent = '다음';
    nextButton.className = 'pagination-btn';
    nextButton.disabled = currentPage === totalPages;
    nextButton.addEventListener('click', () => {
        if (currentPage < totalPages) {
            currentPage++;
            displayVideosPage();
            renderPagination();
            window.scrollTo(0, 0);
        }
    });
    
    const pageInfo = document.createElement('span');
    pageInfo.className = 'page-info';
    pageInfo.textContent = `${currentPage} / ${totalPages}`;

    paginationContainer.appendChild(prevButton);
    paginationContainer.appendChild(pageInfo);
    paginationContainer.appendChild(nextButton);
};


// '더보기' 버튼에 대한 이벤트 위임
videoGrid.addEventListener('click', (e) => {
    if (e.target.classList.contains('more-info-btn')) {
        const button = e.target;
        const card = button.closest('.video-card');
        if (card) {
            card.classList.toggle('expanded');
            button.textContent = card.classList.contains('expanded') ? '숨기기' : '더보기';
        }
    }
});

// 필터 이벤트 리스너 등록
[searchInput, formTypeFilter, startDateFilter, endDateFilter, sortFilter].forEach(el => {
    el.addEventListener('input', applyFiltersAndSort);
    if (el.tagName === 'SELECT') {
        el.addEventListener('change', applyFiltersAndSort);
    }
});

// 초기 데이터 로드
fetchVideos();
