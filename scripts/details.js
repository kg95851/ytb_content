import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getFirestore, doc, getDoc } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

import { firebaseConfig } from './firebase-config.js';

// Firebase 초기화
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

const detailsContent = document.getElementById('details-content');

// URL에서 비디오 ID 가져오기
const getVideoIdFromUrl = () => {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('id');
};

// 비디오 상세 정보 가져오기 및 표시
const fetchAndDisplayDetails = async () => {
    const videoId = getVideoIdFromUrl();

    if (!videoId) {
        detailsContent.innerHTML = '<p class="error-message">잘못된 접근입니다. 비디오 ID가 필요합니다.</p>';
        return;
    }

    try {
        const docRef = doc(db, 'videos', videoId);
        const docSnap = await getDoc(docRef);

        if (docSnap.exists()) {
            const video = docSnap.data();
            renderDetails(video);
        } else {
            detailsContent.innerHTML = '<p class="error-message">해당 비디오를 찾을 수 없습니다.</p>';
        }
    } catch (error) {
        console.error("Error fetching video details: ", error);
        detailsContent.innerHTML = '<p class="error-message">데이터를 불러오는 데 실패했습니다.</p>';
    }
};

// 상세 정보 렌더링 함수
const renderDetails = (video) => {
    // 페이지 제목 설정
    document.title = `${video.title} - 콘텐츠 상세 정보`;

    // 카테고리 조합
    const kr_categories = [video.kr_category_large, video.kr_category_medium, video.kr_category_small].filter(Boolean).join(' > ');
    const en_categories = [video.en_category_main, video.en_category_sub, video.en_micro_topic].filter(Boolean).join(' > ');
    const cn_categories = [video.cn_category_large, video.cn_category_medium, video.cn_category_small].filter(Boolean).join(' > ');

    // YouTube 임베드 플레이어 생성 시도
    let videoPlayerHTML = '';
    try {
        const url = new URL(video.youtube_url);
        let ytVideoId = url.searchParams.get('v');
        // 짧은 URL (youtu.be) 또는 Shorts URL 처리
        if (!ytVideoId && (url.hostname.includes('youtu.be') || url.hostname.includes('youtube.com/shorts/'))) {
            ytVideoId = url.pathname.split('/').pop();
        }
        
        if (ytVideoId) {
            videoPlayerHTML = `<iframe src="https://www.youtube.com/embed/${ytVideoId}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>`;
        }
    } catch (e) {
        console.error("Could not parse YouTube URL for embedding:", e);
    }

    // 임베드 실패 시 썸네일 표시 및 링크 제공
    if (!videoPlayerHTML) {
        const thumbnail = video.thumbnail
            ? `<img src="${video.thumbnail}" alt="${video.title}">`
            : `<div class="no-thumbnail-placeholder" style="height: 100%;">영상 또는 이미지 없음</div>`;
        videoPlayerHTML = `<a href="${video.youtube_url}" target="_blank">${thumbnail}</a>`;
    }

    detailsContent.innerHTML = `
        <div class="details-container">
            <div class="video-player-container">
                ${videoPlayerHTML}
            </div>
            <div class="details-info">
                <h1>${video.title || '제목 없음'}</h1>
                <div class="details-meta-bar">
                    <div class="meta-item"><strong>채널</strong> <span>${video.channel || '없음'}</span></div>
                    <div class="meta-item"><strong>게시일</strong> <span>${video.date || '없음'}</span></div>
                    <div class="meta-item"><strong>조회수</strong> <span>${(video.views_numeric || 0).toLocaleString()}회</span></div>
                    <div class="meta-item"><strong>구독자</strong> <span>${(video.subscribers_numeric || 0).toLocaleString()}명</span></div>
                    <div class="meta-item"><strong>폼 유형</strong> <span class="group-tag">${video.group_name || '없음'}</span></div>
                </div>
                
                <h2>상세 분석 정보</h2>
                <div class="details-grid">
                    ${renderDetailItem('소재', video.material)}
                    ${renderDetailItem('템플릿 유형', video.template_type)}
                    ${renderDetailItem('원본 유형', video.source_type)}
                    ${renderDetailItem('후킹 요소', video.hooking)}
                    ${renderDetailItem('기승전결 구조', video.narrative_structure)}
                    ${renderDetailItem('한국 카테고리', kr_categories)}
                    ${renderDetailItem('영문 카테고리', en_categories)}
                    ${renderDetailItem('중국 카테고리', cn_categories)}
                </div>
            </div>
        </div>
    `;
};

// 상세 항목 렌더링 헬퍼
const renderDetailItem = (label, value) => {
    return `
        <div class="detail-item">
            <span class="detail-label">${label}</span>
            <span class="detail-value">${value || '없음'}</span>
        </div>
    `;
};


// 페이지 로드 시 실행
fetchAndDisplayDetails();
