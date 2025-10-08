import"./style-HOslubru.js";import{initializeApp as I}from"https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";import{getFirestore as P,doc as H,getDoc as D}from"https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";import{f as R}from"./firebase-config-D0anIP3T.js";const j=I(R),N=P(j),E=document.getElementById("details-content"),v=document.getElementById("dopamine-graph"),L=document.getElementById("top-comments"),U=()=>new URLSearchParams(window.location.search).get("id"),B=async()=>{const e=U();if(!e){E.innerHTML='<p class="error-message">잘못된 접근입니다. 비디오 ID가 필요합니다.</p>';return}try{const n=H(N,"videos",e),r=await D(n);if(r.exists()){const s=r.data();z(s)}else E.innerHTML='<p class="error-message">해당 비디오를 찾을 수 없습니다.</p>'}catch(n){console.error("Error fetching video details: ",n),E.innerHTML='<p class="error-message">데이터를 불러오는 데 실패했습니다.</p>'}},z=e=>{document.title=`${e.title} - 콘텐츠 상세 정보`;const n=[e.kr_category_large,e.kr_category_medium,e.kr_category_small].filter(Boolean).join(" > "),r=[e.en_category_main,e.en_category_sub,e.en_micro_topic].filter(Boolean).join(" > "),s=[e.cn_category_large,e.cn_category_medium,e.cn_category_small].filter(Boolean).join(" > ");let t="";try{const o=new URL(e.youtube_url);let l=o.searchParams.get("v");!l&&(o.hostname.includes("youtu.be")||o.hostname.includes("youtube.com/shorts/"))&&(l=o.pathname.split("/").pop()),l&&(t=`<iframe src="https://www.youtube.com/embed/${l}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>`)}catch(o){console.error("Could not parse YouTube URL for embedding:",o)}if(!t){const o=e.thumbnail?`<img src="${e.thumbnail}" alt="${e.title}">`:'<div class="no-thumbnail-placeholder" style="height: 100%;">영상 또는 이미지 없음</div>';t=`<a href="${e.youtube_url}" target="_blank">${o}</a>`}const p=Array.isArray(e.keywords_ko)?e.keywords_ko:[],u=Array.isArray(e.keywords_en)?e.keywords_en:[],g=Array.isArray(e.keywords_zh)?e.keywords_zh:[];if(E.innerHTML=`
        <div class="details-container">
            <div class="video-player-container">
                ${t}
            </div>
            <div class="details-info">
                <h1>${e.title||"제목 없음"}</h1>
                <div class="details-meta-bar">
                    <div class="meta-item"><strong>채널</strong> <span>${e.channel||"없음"}</span></div>
                    <div class="meta-item"><strong>게시일</strong> <span>${e.date||"없음"}</span></div>
                    <div class="meta-item"><strong>조회수</strong> <span>${(e.views_numeric||0).toLocaleString()}회</span></div>
                    <div class="meta-item"><strong>구독자</strong> <span>${(e.subscribers_numeric||0).toLocaleString()}명</span></div>
                    <div class="meta-item"><strong>폼 유형</strong> <span class="group-tag">${e.group_name||"없음"}</span></div>
                </div>
                
                <h2>상세 분석 정보</h2>
                <div class="details-grid">
                    ${k("소재",e.material)}
                    ${k("템플릿 유형",e.template_type)}
                    ${k("후킹 요소",e.hooking)}
                    ${k("기승전결 구조",e.narrative_structure)}
                    ${k("한국 카테고리",n)}
                    ${k("영문 카테고리",r)}
                    ${k("중국 카테고리",s)}
                </div>
                ${p.length||u.length||g.length?`
                <h2 style="margin-top:1.5rem;">검색 키워드</h2>
                <div class="keyword-cards-grid">
                    ${T("한국어",p)}
                    ${T("English",u)}
                    ${T("中文",g)}
                </div>`:""}
                ${e.analysis_full?`<h2 style="margin-top:1.5rem;">분석 카드</h2><div class="analysis-cards-grid">${q(K(e.analysis_full))}</div>`:""}
            </div>
        </div>
    `,Array.isArray(e.dopamine_graph)&&e.dopamine_graph.length&&v){v.innerHTML="";const o=document.createElement("div");o.className="dopamine-legend",o.innerHTML=`
            <span><i class="dopamine-dot dot-low"></i> 1-3 낮음</span>
            <span><i class="dopamine-dot dot-mid"></i> 4-6 중간</span>
            <span><i class="dopamine-dot dot-high"></i> 7-9 높음</span>
        `,v.appendChild(o);const l=document.createElement("div");l.innerHTML='<div style="font-weight:600;">문장</div><div style="font-weight:600;">레벨</div><div style="font-weight:600;">시각화</div>',l.style.display="grid",l.style.gridTemplateColumns="1fr auto auto",l.style.marginBottom="8px",v.appendChild(l),e.dopamine_graph.forEach(y=>{const $=document.createElement("div");$.textContent=y.sentence||y.text||"";const h=Number(y.level??y.score??0),f=document.createElement("div");f.textContent=String(h);const a=document.createElement("div");a.style.height="10px",a.style.width=Math.max(5,Math.min(100,Math.round(h*10)))+"px";let d="#94a3b8";h>=4&&h<=6&&(d="#f59e0b"),h>=7&&h<=9&&(d="#ef4444"),h>=10&&(d="#b91c1c"),a.style.background=d,a.style.borderRadius="4px";const i=document.createElement("div");i.style.display="grid",i.style.gridTemplateColumns="1fr auto auto",i.style.alignItems="center",i.style.gap="12px",i.appendChild($),i.appendChild(f),i.appendChild(a),v.appendChild(i)});const c=document.createElement("div");c.id="dopamine-chart-container",c.style.marginTop="16px";const m=document.createElement("canvas");m.id="dopamine-chart",m.height=220,c.appendChild(m),v.appendChild(c),A(m,e.dopamine_graph),window.addEventListener("resize",()=>A(m,e.dopamine_graph))}if(L){const o=Array.isArray(e.comments_top)?e.comments_top:[];if(!o.length)L.innerHTML='<p class="info-message">수집된 인기 댓글이 없습니다. 관리자에서 댓글분석을 실행하세요.</p>';else{const l=o.map(c=>{const m=b(c.author||""),y=b((c.text||"").replace(/<br\s*\/?>/gi,`
`).replace(/<[^>]+>/g,"")),$=Number(c.likeCount||0).toLocaleString(),h=c.publishedAt?new Date(c.publishedAt).toLocaleString():"",f=c.authorProfileImageUrl?`<img src="${c.authorProfileImageUrl}" alt="${m}" style="width:24px;height:24px;border-radius:50%;object-fit:cover;">`:"",a=c.authorChannelUrl?`<a href="${c.authorChannelUrl}" target="_blank" rel="noopener noreferrer">`:"",d=c.authorChannelUrl?"</a>":"";return`
                <div class="detail-item">
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">${f}${a}<strong>${m}</strong>${d}<span style="color:#6b7280; font-size:12px;">${h}</span></div>
                    <div style="white-space:pre-wrap;">${y}</div>
                    <div style="margin-top:6px;color:#6b7280; font-size:12px;">좋아요 ${$}</div>
                </div>`}).join("");L.innerHTML=`<div class="details-grid">${l}</div>`}}},k=(e,n,r=!1)=>`
        <div class="${r?"detail-item analysis-full":"detail-item"}">
            <span class="detail-label">${e}</span>
            <span class="detail-value">${n||"없음"}</span>
        </div>
    `;function b(e){return String(e).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}function K(e){try{const n=String(e).split(`
`),r=[];for(const s of n){const t=s.trimEnd();if(/^\s*9\./.test(t)||/^\s*10\./.test(t))break;/^\s*\[gpts/i.test(t)||/페르소나|핵심 임무|절대 규칙|출력 템플릿/.test(t)||r.push(t)}return r.join(`
`).trim()}catch{return e||""}}function W(e){const n=[],r=String(e).split(`
`);let s=null;for(const t of r){const p=t.match(/^\s*([0-8])\.(.*)$/);p?(s&&n.push(s),s={idx:Number(p[1]),title:p[2].trim(),lines:[]}):s&&s.lines.push(t)}return s&&n.push(s),n}function q(e){const n=W(e);return n.length?n.filter(r=>r.idx!==0&&r.idx!==2).map(r=>{const s=`${r.idx}. ${r.title||""}`.trim(),t=F(r.lines.join(`
`).trim());return`
        <div class="analysis-card">
            <div class="analysis-card-header">${b(s)}</div>
            <div class="analysis-card-body">
                <pre class="analysis-pre">${b(t)}</pre>
            </div>
        </div>`}).join(""):""}function F(e){const n=String(e).split(`
`),r=[];let s=0;for(;s<n.length;){const t=n[s];if(/^\s*\|/.test(t)){const p=[];for(;s<n.length&&/^\s*\|/.test(n[s]);)p.push(n[s]),s++;const u=p.map(o=>o.replace(/^\s*\|/,"").replace(/\|\s*$/,"")).map(o=>o.split("|").map(l=>l.trim())),g=u.filter(o=>!o.every(l=>/^:?-{3,}:?$/.test(l)));u.length>=2&&u[1].every(o=>/^:?-{3,}:?$/.test(o))&&g.shift(),g.forEach(o=>{o.length===2?r.push(`- ${o[0]}: ${o[1]}`):r.push(`- ${o.filter(Boolean).join(" · ")}`)})}else r.push(t),s++}return r.join(`
`).trim()}function T(e,n){const s=(Array.isArray(n)?n:[]).map(t=>`<span class="keyword-chip" data-keyword="${b(String(t))}">${b(String(t))}</span>`).join("");return`
    <div class="keyword-card">
        <div class="keyword-card-header">${b(e)}</div>
        <div class="keyword-card-body">${s||'<span class="keyword-chip empty">키워드 없음</span>'}</div>
    </div>`}let w=null,M=null;function _(){w&&w.parentNode&&w.parentNode.removeChild(w),w=null,M=null,window.removeEventListener("scroll",_,!0),window.removeEventListener("resize",_,!0)}function Y(e,n){if(M===e&&w){_();return}_();const r=encodeURIComponent(String(n||"")),s=document.createElement("div");s.className="keyword-menu",s.innerHTML=`
        <div class="keyword-menu-title">검색: ${b(n)}</div>
        <a class="platform-link" href="https://www.youtube.com/results?search_query=${r}" target="_blank" rel="noopener noreferrer">YouTube</a>
        <a class="platform-link" href="https://www.instagram.com/explore/search/keyword/?q=${r}" target="_blank" rel="noopener noreferrer">Instagram</a>
        <a class="platform-link" href="https://www.tiktok.com/search?q=${r}" target="_blank" rel="noopener noreferrer">TikTok</a>
        <a class="platform-link" href="https://www.douyin.com/search/${r}" target="_blank" rel="noopener noreferrer">抖音 Douyin</a>
        <a class="platform-link" href="https://www.xiaohongshu.com/search_result?keyword=${r}" target="_blank" rel="noopener noreferrer">小红书 RED</a>
    `,document.body.appendChild(s),w=s,M=e;const t=e.getBoundingClientRect(),p=window.scrollY+t.bottom+6,u=window.scrollX+t.left;s.style.top=p+"px",s.style.left=u+"px",s.addEventListener("click",g=>{g.stopPropagation()}),window.addEventListener("scroll",_,!0),window.addEventListener("resize",_,!0)}document.addEventListener("click",e=>{const n=e.target.closest(".keyword-chip");if(n&&!n.classList.contains("empty")){e.preventDefault(),e.stopPropagation();const r=n.getAttribute("data-keyword")||n.textContent||"";Y(n,r.trim());return}w&&!e.target.closest(".keyword-menu")&&_()});function A(e,n){if(!e||!e.getContext||!Array.isArray(n)||n.length===0)return;const r=e.parentElement,s=Math.max(320,(r==null?void 0:r.clientWidth)||640);e.width=s;const t=e.getContext("2d");t.clearRect(0,0,e.width,e.height);const p=40,u=16,g=16,o=28,l=e.width-p-u,c=e.height-g-o,m=0,y=10,$=n.length;function h(a){return $===1?p+l/2:p+a/($-1)*l}function f(a){const i=(Math.max(m,Math.min(y,Number(a)||0))-m)/(y-m);return g+(1-i)*c}t.strokeStyle="#e5e7eb",t.lineWidth=1,t.beginPath(),[0,3,6,9,10].forEach(a=>{const d=f(a);t.moveTo(p,d),t.lineTo(e.width-u,d)}),t.stroke(),t.fillStyle="#6b7280",t.font="12px system-ui, -apple-system, Segoe UI, Roboto, Noto Sans KR, Arial",[0,3,6,9,10].forEach(a=>{const d=f(a);t.fillText(String(a),8,d+4)}),t.strokeStyle="#2563eb",t.lineWidth=2,t.beginPath(),n.forEach((a,d)=>{const i=h(d),x=f(a.level??a.score??0);d===0?t.moveTo(i,x):t.lineTo(i,x)}),t.stroke(),n.forEach((a,d)=>{const i=Number(a.level??a.score??0),x=h(d),S=f(i);let C="#94a3b8";i>=4&&i<=6&&(C="#f59e0b"),i>=7&&i<=9&&(C="#ef4444"),i>=10&&(C="#b91c1c"),t.fillStyle=C,t.beginPath(),t.arc(x,S,3,0,Math.PI*2),t.fill()})}B();
