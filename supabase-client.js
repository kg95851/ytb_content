// ESM CDN을 사용해 번들러 없이도 동작하도록 처리
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

function getMeta(name) {
  try { return document.querySelector(`meta[name="${name}"]`)?.content || ''; } catch { return ''; }
}

let url = '';
try { url = (import.meta && import.meta.env && import.meta.env.VITE_SUPABASE_URL) || ''; } catch {}
if (!url && typeof window !== 'undefined') url = window.SUPABASE_URL || '';
if (!url) url = getMeta('supabase-url');

let anonKey = '';
try { anonKey = (import.meta && import.meta.env && import.meta.env.VITE_SUPABASE_ANON_KEY) || ''; } catch {}
if (!anonKey && typeof window !== 'undefined') anonKey = window.SUPABASE_ANON_KEY || '';
if (!anonKey) anonKey = getMeta('supabase-anon-key');

function createStub(message) {
  const err = () => ({ error: new Error(message) });
  return {
    auth: { signInWithPassword: async () => err(), signOut: async () => ({}), getSession: async () => ({ data: { session: null } }) },
    from: () => ({ select: async () => err(), update: async () => err(), insert: async () => err(), upsert: async () => err(), delete: async () => err(), order: () => ({ select: async () => err() }), range: () => ({ select: async () => err() }) }),
    storage: { from: () => ({ upload: async () => err(), getPublicUrl: () => ({ data: { publicUrl: '' } }) }) }
  };
}

export const supabase = (() => {
  if (!url || !anonKey) {
    console.error('Supabase 설정이 비어 있습니다. VITE_SUPABASE_URL / VITE_SUPABASE_ANON_KEY 또는 meta 태그(supabase-url, supabase-anon-key)를 설정하세요.');
    return createStub('Supabase 설정 누락');
  }
  try {
    return createClient(url, anonKey);
  } catch (e) {
    console.error('Supabase 초기화 실패:', e);
    return createStub('Supabase 초기화 실패');
  }
})();
