import"./style-HOslubru.js";function z(){try{return localStorage.getItem("my_gemini_key")||""}catch{return""}}function _(e){try{localStorage.setItem("my_gemini_key",e||"")}catch{}}function $(e,i){const s=(i||"").split(/\r?\n/).map(t=>t.trim()).filter(Boolean);if(!s.length)return e;const n=`

[사용자 체크리스트]
`+s.map((t,r)=>`${r+1}. ${t}`).join(`
`)+`

위 체크리스트 항목을 반드시 분석에 반영하세요.`;return e+n}async function b(e,i){var m,a,o,c,g,u;const s=(((m=document.getElementById("my-gemini-key"))==null?void 0:m.value)||z()).trim();if(!s)throw new Error("개인 Gemini 키를 입력하세요.");_(s);const t=await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key=${encodeURIComponent(s)}`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({contents:[{role:"user",parts:[{text:e+`

`+i}]}],generationConfig:{temperature:.3}})});if(!t.ok)throw new Error("Gemini 호출 실패: "+t.status);const r=await t.json();return((u=(g=(c=(o=(a=r==null?void 0:r.candidates)==null?void 0:a[0])==null?void 0:o.content)==null?void 0:c.parts)==null?void 0:g[0])==null?void 0:u.text)||""}function P(e){function i(n){return String(n).replace(/^```json\s*/i,"").replace(/^```\s*/i,"").replace(/```\s*$/i,"").trim()}function s(n){const t=Array.isArray(n)?n:typeof n=="string"?n.split(/[\n,，]/):[],r=new Set,m=[];for(const a of t){const o=(typeof a=="string"?a:a&&(a.keyword||a.text||a.name))||"",c=String(o).replace(/["'#]/g,"").trim();if(!c)continue;const g=c.toLowerCase();r.has(g)||(r.add(g),m.push(c))}return m.slice(0,20)}try{let n=i(e),t=null;try{t=JSON.parse(n)}catch{const r=n.match(/\{[\s\S]*\}/);if(r)try{t=JSON.parse(r[0])}catch{}}return{ko:s(t==null?void 0:t.ko),en:s(t==null?void 0:t.en),zh:s((t==null?void 0:t.zh)||(t==null?void 0:t.cn))}}catch{return{ko:[],en:[],zh:[]}}}function G(e){if(!e)return[];let i=String(e).replace(/\r/g,`
`).replace(/\n{2,}/g,`
`).trim();return i=i.replace(/>{2,}/g," ").replace(/\s{2,}/g," ").trim(),i.split(`
`).map(t=>t.trim()).filter(t=>t&&!/^\d+(\.\d+)?$/.test(t)).join(`
`).replace(/([\.\?\!…])\s*\n+/g,"$1__SENT__").replace(/\n+/g," ").replace(/__SENT__/g," ").replace(/\s{2,}/g," ").trim().split(new RegExp("(?<=[\\.\\?\\!…])\\s+")).map(t=>t.trim()).filter(Boolean)}function v(e){return String(e).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}function B(e,i){const n=(Array.isArray(i)?i:[]).map(t=>`<span class="keyword-chip" data-keyword="${v(String(t))}">${v(String(t))}</span>`).join("");return`<div class="keyword-card"><div class="keyword-card-header">${v(e)}</div><div class="keyword-card-body">${n||'<span class="keyword-chip empty">키워드 없음</span>'}</div></div>`}function H(e){const i=String(e).split(`
`),s=[];let n=0;for(;n<i.length;){const t=i[n];if(/^\s*\|/.test(t)){const r=[];for(;n<i.length&&/^\s*\|/.test(i[n]);)r.push(i[n]),n++;const m=r.map(o=>o.replace(/^\s*\|/,"").replace(/\|\s*$/,"")).map(o=>o.split("|").map(c=>c.trim())),a=m.filter(o=>!o.every(c=>/^:?-{3,}:?$/.test(c)));m.length>=2&&m[1].every(o=>/^:?-{3,}:?$/.test(o))&&a.shift(),a.forEach(o=>{o.length===2?s.push(`- ${o[0]}: ${o[1]}`):s.push(`- ${o.filter(Boolean).join(" · ")}`)})}else s.push(t),n++}return s.join(`
`).trim()}function K(e){const i=R(e);return i.length?i.filter(s=>s.idx!==0&&s.idx!==2).map(s=>{const n=`${s.idx}. ${s.title||""}`.trim(),t=H(s.lines.join(`
`).trim());return`<div class="analysis-card"><div class="analysis-card-header">${v(n)}</div><div class="analysis-card-body"><pre class="analysis-pre">${v(t)}</pre></div></div>`}).join(""):""}function R(e){const i=[],s=String(e).split(`
`);let n=null;for(const t of s){const r=t.match(/^\s*([0-8])\.(.*)$/);r?(n&&i.push(n),n={idx:Number(r[1]),title:r[2].trim(),lines:[]}):n&&n.lines.push(t)}return n&&i.push(n),i}function U(e,i){if(!e||!e.getContext||!Array.isArray(i)||i.length===0)return;const s=e.parentElement,n=Math.max(320,(s==null?void 0:s.clientWidth)||640);e.width=n;const t=e.getContext("2d");t.clearRect(0,0,e.width,e.height);const r=40,m=16,a=16,o=28,c=e.width-r-m,g=e.height-a-o,u=0,x=10,w=i.length,y=l=>w===1?r+c/2:r+l/(w-1)*c,E=l=>{const d=(Math.max(u,Math.min(x,Number(l)||0))-u)/(x-u);return a+(1-d)*g};t.strokeStyle="#e5e7eb",t.lineWidth=1,t.beginPath(),[0,3,6,9,10].forEach(l=>{const p=E(l);t.moveTo(r,p),t.lineTo(e.width-m,p)}),t.stroke(),t.fillStyle="#6b7280",t.font="12px system-ui, -apple-system, Segoe UI, Roboto, Noto Sans KR, Arial",[0,3,6,9,10].forEach(l=>{const p=E(l);t.fillText(String(l),8,p+4)}),t.strokeStyle="#2563eb",t.lineWidth=2,t.beginPath(),i.forEach((l,p)=>{const d=y(p),k=E(l.level??l.score??0);p===0?t.moveTo(d,k):t.lineTo(d,k)}),t.stroke(),i.forEach((l,p)=>{const d=Number(l.level??l.score??0),k=y(p),C=E(d);let S="#94a3b8";d>=4&&d<=6&&(S="#f59e0b"),d>=7&&d<=9&&(S="#ef4444"),d>=10&&(S="#b91c1c"),t.fillStyle=S,t.beginPath(),t.arc(k,C,3,0,Math.PI*2),t.fill()})}async function W(){var c,g,u,x;const e=document.getElementById("my-status"),i=(((c=document.getElementById("my-title"))==null?void 0:c.value)||"").trim(),s=(((g=document.getElementById("my-transcript"))==null?void 0:g.value)||"").trim(),n=(((u=document.getElementById("my-checklist"))==null?void 0:u.value)||"").trim();if(!s){e.textContent="대본을 입력하세요.";return}e.textContent="분석 시작...";const t=`다음 대본을 기반으로 카테고리를 아래 형식으로만 한 줄씩 정확히 출력하세요. 다른 텍스트/머리말/설명 금지.
한국 대 카테고리: 
한국 중 카테고리: 
한국 소 카테고리: 
EN Main Category: 
EN Sub Category: 
EN Micro Topic: 
중국 대 카테고리: 
중국 중 카테고리: 
중국 소 카테고리: `,r=`아래 제공된 "제목"과 "대본"을 모두 참고하여, 원본 영상을 검색해 찾기 쉬운 핵심 검색 키워드를 한국어/영어/중국어로 각각 8~15개씩 추출하세요.
출력 형식은 JSON 객체만, 다른 설명/머리말/코드펜스 금지.
요구 형식: {"ko":["키워드1","키워드2",...],"en":["keyword1",...],"zh":["关键词1",...]}
규칙:
- 각 키워드는 1~4단어의 짧은 구로 작성
- 해시태그/특수문자/따옴표 제거, 불용어 제외
- 동일 의미/중복 표현은 하나만 유지
- 인명/채널명/브랜드/핵심 주제 포함
`,m='다음 대본의 핵심 소재를 한 문장으로 요약하세요. 반드시 한 줄로만, "소재: "로 시작하여 출력하세요. 다른 설명이나 불필요한 문자는 금지합니다.',a='다음 "문장 배열"에 대해, 각 문장별로 궁금증/도파민 유발 정도를 1~10 정수로 평가하고, 그 이유를 간단히 설명하세요. 반드시 JSON 배열로만, 요소는 {"sentence":"문장","level":정수,"reason":"이유"} 형태로 출력하세요. 여는 대괄호부터 닫는 대괄호까지 외 텍스트는 출력하지 마세요.',o="...";try{let h=function(f,j){const N=String(j).match(f);return N?(N[1]||N[0]).trim():""};const w=await b($(m,n),s);e.textContent="카테고리 분석 중...";const y=await b($(t,n),s);e.textContent="키워드 분석 중...";const E=await b($(r,n),`제목:
${i}

대본:
${s}`);e.textContent="도파민 분석 중...";const l=G(s),p=await b($(a,n),`문장 배열:
`+JSON.stringify(l));let d=[];try{d=JSON.parse(p)}catch{const f=p.match(/\[([\s\S]*?)\]/);if(f)try{d=JSON.parse("["+f[1]+"]")}catch{}}e.textContent="템플릿 분석 중...";const k=await b($(o,n),s),C=document.getElementById("my-details-grid"),S=P(E),O=[h(/한국\s*대\s*카테고리\s*[:：]\s*(.+)/i,y),h(/한국\s*중\s*카테고리\s*[:：]\s*(.+)/i,y),h(/한국\s*소\s*카테고리\s*[:：]\s*(.+)/i,y)].filter(Boolean).join(" > "),J=[h(/EN\s*Main\s*Category\s*[:：]\s*(.+)/i,y),h(/EN\s*Sub\s*Category\s*[:：]\s*(.+)/i,y),h(/EN\s*Micro\s*Topic\s*[:：]\s*(.+)/i,y)].filter(Boolean).join(" > "),L=[h(/중국\s*대\s*카테고리\s*[:：]\s*(.+)/i,y),h(/중국\s*중\s*카테고리\s*[:：]\s*(.+)/i,y),h(/중국\s*소\s*카테고리\s*[:：]\s*(.+)/i,y)].filter(Boolean).join(" > ");C.innerHTML=`
            <div class="detail-item"><span class="detail-label">소재</span><span class="detail-value">${v((((x=w.match(/소재\s*[:：]\s*(.+)/i))==null?void 0:x[1])||w||"").trim())}</span></div>
            <div class="detail-item"><span class="detail-label">한국 카테고리</span><span class="detail-value">${v(O)}</span></div>
            <div class="detail-item"><span class="detail-label">영문 카테고리</span><span class="detail-value">${v(J)}</span></div>
            <div class="detail-item"><span class="detail-label">중국 카테고리</span><span class="detail-value">${v(L)}</span></div>
        `,document.getElementById("my-keywords").innerHTML=B("한국어",S.ko)+B("English",S.en)+B("中文",S.zh),document.getElementById("my-analysis-cards").innerHTML=K(k);const I=document.getElementById("my-dopa-graph");I.innerHTML="";const T=document.createElement("canvas");T.height=220,I.appendChild(T),U(T,Array.isArray(d)?d.map(f=>({sentence:f.sentence||f.text||"",level:Number(f.level||f.score||0)})):[]),e.textContent="완료"}catch(w){document.getElementById("my-status").textContent="오류: "+(w.message||w)}}var M;(M=document.getElementById("my-run-btn"))==null||M.addEventListener("click",W);var A;(A=document.getElementById("my-gemini-key"))==null||A.addEventListener("change",e=>_(e.target.value));
