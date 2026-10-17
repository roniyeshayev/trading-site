/* Storage layer for the site: gives the pages the same small API they had inside Claude
   (db collections/documents, file assets, downloads), backed by the owner's Supabase project.
   Every page that needs data loads supabase-js, config.js and this file before its own script. */
(function () {
  'use strict';
  const CFG = window.SITE_CFG;
  const sb = window.supabase.createClient(CFG.url, CFG.key, { auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true } });
  window.__sb = sb;
  const PAGE = location.pathname.split('/').pop() || 'index.html';
  const loginUrl = () => 'index.html?next=' + encodeURIComponent(PAGE + location.search);

  /* ---------- session ---------- */
  let uid = null;
  const ready = sb.auth.getSession().then(({ data }) => {
    const s = data && data.session;
    if (!s) { if (!window.SITE_PUBLIC) location.replace(loginUrl()); return new Promise(() => {}); }
    uid = s.user.id; window.__user = s.user; return s;
  });
  sb.auth.onAuthStateChange((ev) => { if (ev === 'SIGNED_OUT' && !window.SITE_PUBLIC) location.replace('index.html'); });

  const clone = (v) => v == null ? v : JSON.parse(JSON.stringify(v));
  const rid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
  const wrap = (e) => { const err = new Error((e && e.message) || 'שגיאה בשמירה'); err.code = (e && (e.name === 'TypeError' || /fetch|network/i.test(e.message || ''))) ? 'unavailable' : 'internal'; err.cause = e; return err; };

  /* ---------- documents: one table, rows keyed by (owner, collection path, id) ---------- */
  const cache = new Map();   /* collection path -> Map(id -> data) */
  const subs = new Map();    /* collection path -> Set(callback) */
  const live = new Map();    /* collection path -> realtime channel */
  const snapOf = (col) => { const m = cache.get(col) || new Map(); const docs = [...m].map(([id, d]) => ({ id, exists: true, data: () => clone(d) })); return { docs, size: docs.length, empty: !docs.length, forEach: (f) => docs.forEach(f) }; };
  const emit = (col) => { const s = subs.get(col); if (s) for (const cb of [...s]) { try { cb(snapOf(col)); } catch (e) { console.error(e); } } };
  async function load(col) {
    await ready; const m = new Map();
    for (let from = 0; ; from += 1000) {
      const { data, error } = await sb.from('docs').select('id,data').eq('col', col).range(from, from + 999);
      if (error) throw wrap(error);
      for (const r of data) m.set(r.id, r.data);
      if (data.length < 1000) break;
    }
    cache.set(col, m); return m;
  }
  function putLocal(col, id, data) { if (!cache.has(col)) return; const m = cache.get(col); if (data === undefined) { if (!m.has(id)) return; m.delete(id); } else { if (JSON.stringify(m.get(id)) === JSON.stringify(data)) return; m.set(id, data); } emit(col); }
  function watch(col) {
    if (live.has(col)) return;
    const ch = sb.channel('docs:' + col + ':' + rid())
      .on('postgres_changes', { event: '*', schema: 'public', table: 'docs', filter: 'col=eq.' + col }, (p) => {
        if (p.eventType === 'DELETE') putLocal(col, p.old && p.old.id, undefined);
        else if (p.new) putLocal(col, p.new.id, p.new.data);
      }).subscribe();
    live.set(col, ch);
  }
  function Col(path) {
    return {
      path, id: path.split('/').pop(),
      doc: (id) => Doc(path, id || rid()),
      async get() { await load(path); return snapOf(path); },
      onSnapshot(cb, onErr) {
        let s = subs.get(path); if (!s) subs.set(path, s = new Set());
        s.add(cb);
        (cache.has(path) ? Promise.resolve() : load(path)).then(() => { if (s.has(cb)) cb(snapOf(path)); }, (e) => { if (onErr) onErr(wrap(e)); });
        ready.then(() => watch(path));
        return () => s.delete(cb);
      }
    };
  }
  function Doc(col, id) {
    const self = {
      id, path: col + '/' + id,
      collection: (sub) => Col(col + '/' + id + '/' + sub),
      async get() {
        await ready;
        const { data, error } = await sb.from('docs').select('data').eq('col', col).eq('id', id).maybeSingle();
        if (error) throw wrap(error);
        return { id, exists: !!data, data: () => data ? clone(data.data) : undefined };
      },
      async set(d) {
        await ready; const v = clone(d) || {};
        putLocal(col, id, v);
        const { error } = await sb.from('docs').upsert({ owner: uid, col, id, data: v, updated_at: new Date().toISOString() });
        if (error) throw wrap(error);
      },
      async update(d) {
        await ready;
        let cur = cache.has(col) ? cache.get(col).get(id) : undefined;
        if (cur === undefined) { const g = await self.get(); cur = g.exists ? g.data() : {}; }
        return self.set(Object.assign(clone(cur) || {}, clone(d)));
      },
      async delete() {
        await ready; putLocal(col, id, undefined);
        const { error } = await sb.from('docs').delete().eq('col', col).eq('id', id);
        if (error) throw wrap(error);
      }
    };
    return self;
  }
  const db = { collection: (p) => Col(p), doc: (p) => { const i = p.lastIndexOf('/'); return Doc(p.slice(0, i), p.slice(i + 1)); } };

  /* ---------- files (market data): compressed text rows in an owner-only table ---------- */
  const assets = {
    async upload(blob, opts) {
      await ready; const id = 'a' + rid();
      const content = typeof blob === 'string' ? blob : await blob.text();
      const { error } = await sb.from('blobs').upsert({ owner: uid, id, content });
      if (error) throw wrap(error);
      return { id, url: '/_blob/' + id };
    },
    async delete(id) { await ready; const { error } = await sb.from('blobs').delete().eq('id', id); if (error) throw wrap(error); }
  };
  /* the pages read their files as fetch('/_blob/<id>') */
  const nativeFetch = window.fetch.bind(window);
  window.fetch = (u, o) => {
    const s = typeof u === 'string' ? u : (u && u.url) || '';
    const k = s.split('/_blob/')[1];
    if (!k) return nativeFetch(u, o);
    return ready.then(() => sb.from('blobs').select('content').eq('id', decodeURIComponent(k)).maybeSingle())
      .then(({ data, error }) => (error || !data) ? new Response('', { status: 404 }) : new Response(data.content, { headers: { 'content-type': 'text/plain' } }));
  };

  const downloads = {
    async save({ filename, data, mimeType }) {
      const blob = data instanceof Blob ? data : new Blob([data], { type: mimeType || 'text/plain;charset=utf-8' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = filename || 'download';
      document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1500);
      return { status: 'saved' };
    }
  };
  const user = { isOwner: async () => true, current: async () => { await ready; return window.__user; } };

  window.claude = { use: async (n) => { await ready; return n === 'db' ? db : n === 'assets' ? assets : n === 'downloads' ? downloads : n === 'user' ? user : null; } };
  window.siteStore = { sb, ready, db, assets, uid: () => uid };

  /* ---------- top bar shared by the pages ---------- */
  function navBar() {
    if (window.SITE_NO_NAV) return;
    const css = document.createElement('style');
    css.textContent = `
      html body{box-sizing:border-box;padding-top:38px}
      .site-nav{position:fixed;inset:0 0 auto 0;height:38px;z-index:1000;display:flex;align-items:center;gap:4px;padding:0 12px;
        background:#0B0F17;border-bottom:1px solid #1E2635;font:500 13.5px/1 "IBM Plex Sans Hebrew","Rubik",system-ui,sans-serif;direction:rtl}
      .site-nav .brand{color:#E7EBF3;font-weight:700;margin-inline-end:14px;letter-spacing:.01em;text-decoration:none}
      .site-nav a.tab{color:#93A0B7;text-decoration:none;padding:7px 12px;border-radius:7px}
      .site-nav a.tab:hover{color:#E7EBF3;background:#151C29}
      .site-nav a.tab[aria-current="page"]{color:#fff;background:#1E2847}
      .site-nav .sp{flex:1}
      .site-nav button{background:none;border:1px solid #283246;color:#93A0B7;border-radius:7px;padding:6px 10px;font:inherit;cursor:pointer}
      .site-nav button:hover{color:#E7EBF3;border-color:#3A475F}
      @media (max-width:520px){.site-nav .brand{display:none}}`;
    document.head.appendChild(css);
    const nav = document.createElement('nav'); nav.className = 'site-nav'; nav.setAttribute('aria-label', 'ניווט');
    const tab = (href, label) => `<a class="tab" href="${href}"${PAGE === href ? ' aria-current="page"' : ''}>${label}</a>`;
    nav.innerHTML = `<a class="brand" href="index.html">${CFG.name || 'המסחר שלי'}</a>${tab('backtest.html', 'בקטסט')}${tab('journal.html', 'יומן מסחר')}<span class="sp"></span><button type="button" id="siteOut">יציאה</button>`;
    document.body.prepend(nav);
    nav.querySelector('#siteOut').onclick = async () => { await sb.auth.signOut(); location.replace('index.html'); };
  }
  if (document.body) navBar(); else document.addEventListener('DOMContentLoaded', navBar);
})();
