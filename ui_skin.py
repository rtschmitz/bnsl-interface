BNSL_GAME_CSS = r"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Inter:wght@400;500;600&display=swap');

:root{
  --bg0:#070A12;
  --bg1:#0A1020;
  --panel: rgba(18, 26, 48, .62);
  --panel2: rgba(12, 18, 34, .74);
  --stroke: rgba(140, 170, 255, .18);
  --stroke2: rgba(255,255,255,.08);

  --text:#EAF0FF;
  --muted: rgba(234,240,255,.68);

  --accent:#7C5CFF;
  --accent2:#2EF2FF;
  --good:#36F9A2;
  --warn:#FF4D6D;
  --gold:#FFD166;

  --shadow: 0 18px 60px rgba(0,0,0,.55);
  --shadow2: 0 10px 30px rgba(0,0,0,.45);

  --r12: 12px;
  --r16: 16px;
  --r20: 20px;
  --bnsl-page-max: 1600px;
  --bnsl-table-min-width: 920px;
}

*{ box-sizing:border-box; }
html{
  min-height:100%;
  -webkit-text-size-adjust:100%;
  text-size-adjust:100%;
}
body{
  min-height:100%;
  margin:0;
  color: var(--text);
  background:
    radial-gradient(1100px 680px at 70% -10%, rgba(124,92,255,.25), transparent 55%),
    radial-gradient(900px 620px at 20% 0%, rgba(46,242,255,.16), transparent 52%),
    radial-gradient(900px 700px at 70% 80%, rgba(255,209,102,.08), transparent 55%),
    linear-gradient(180deg, var(--bg0), var(--bg1));
  background-attachment: fixed;
  font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  overflow-x:auto;
  overflow-y:auto;
}
body::before{
  content:"";
  position:fixed; inset:-40vmax;
  background:
    conic-gradient(from 210deg,
      rgba(124,92,255,.10),
      rgba(46,242,255,.10),
      rgba(255,77,109,.08),
      rgba(124,92,255,.10)
    );
  filter: blur(60px);
  opacity:.65;
  animation: drift 14s ease-in-out infinite alternate;
  pointer-events:none;
  z-index:-3;
}
@keyframes drift{
  from{ transform: translate3d(-3%, -2%, 0) rotate(-4deg); }
  to{   transform: translate3d( 3%,  2%, 0) rotate( 6deg); }
}
body::after{
  content:"";
  position:fixed; inset:0;
  background:
    linear-gradient(to bottom, rgba(255,255,255,.05), rgba(255,255,255,0) 2px) 0 0/100% 6px,
    linear-gradient(to right, rgba(124,92,255,.10), rgba(0,0,0,0) 20%) 0 0/260px 260px,
    radial-gradient(circle at 50% 50%, rgba(46,242,255,.10), transparent 55%);
  mix-blend-mode: overlay;
  opacity:.25;
  pointer-events:none;
  z-index:-2;
}

.page,
.wrap{
  width:100%;
  max-width: var(--bnsl-page-max);
  margin: 0 auto;
  padding: clamp(12px, 2vw, 22px) clamp(10px, 2vw, 18px) 40px;
  min-width:0;
}

.brand{
  display:flex;
  align-items:flex-end;
  justify-content:space-between;
  gap: 18px;
  margin: 10px 0 16px;
  min-width:0;
}
.brand h1,
h1{
  margin:0;
  font-family: Rajdhani, Inter, system-ui;
  font-weight:700;
  letter-spacing:.6px;
  line-height:1;
}
.brand h1{ font-size: clamp(28px, 5vw, 38px); }
h1{ font-size: clamp(26px, 4vw, 34px); }
h2{ font-family: Rajdhani, Inter, system-ui; letter-spacing:.4px; }
.brand .sub{ margin-top:6px; color: var(--muted); font-size:13px; }
.brand .right{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; justify-content:flex-end; min-width:0; }

a{ color: inherit; }

.badge,
.chip{
  display:inline-flex;
  align-items:center;
  gap:6px;
  font-size: 12px;
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(124,92,255,.14);
  border: 1px solid rgba(124,92,255,.28);
  color: rgba(234,240,255,.92);
  letter-spacing:.2px;
  white-space:nowrap;
}
.chip{ background:rgba(255,255,255,.06); border-color:rgba(140,170,255,.22); }

.panel,
.panel-lite{
  max-width:100%;
  background: var(--panel);
  border: 1px solid var(--stroke);
  border-radius: var(--r20);
  box-shadow: var(--shadow);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  overflow:visible;
  min-width:0;
}
.panel.pad,
.panel-lite{ padding: 14px; }
.panel-lite{ margin:12px 0 16px; }

.topbar,
.row,
.tabs,
.tabrow{
  display:flex;
  flex-wrap:wrap;
  gap:10px;
  align-items:center;
  min-width:0;
}
.tabs{ justify-content:space-between; }

.pill{
  display:inline-flex;
  gap:10px;
  align-items:center;
  max-width:100%;
  min-width:0;
  padding: 10px 12px;
  border-radius: 999px;
  background: rgba(10, 16, 32, .55);
  border: 1px solid var(--stroke2);
  box-shadow: 0 6px 20px rgba(0,0,0,.25);
  color: var(--text);
}
.muted,
.subtle{ color: var(--muted); opacity:1; }
.kbd{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-width:1.6em;
  min-height:1.45em;
  padding:0 .4em;
  border-radius:7px;
  border:1px solid rgba(255,255,255,.16);
  background:rgba(0,0,0,.28);
  color:var(--text);
  font-size:.9em;
}

select,
textarea,
input[type="text"],
input[type="number"],
input[type="email"],
input[type="search"],
input:not([type]){
  max-width:100%;
  background: rgba(5, 8, 16, .55);
  color: var(--text);
  border: 1px solid rgba(140,170,255,.22);
  border-radius: 12px;
  padding: 10px 12px;
  outline: none;
  transition: border-color .18s ease, box-shadow .18s ease;
}
select:focus,
textarea:focus,
input:focus{
  border-color: rgba(46,242,255,.55);
  box-shadow: 0 0 0 3px rgba(46,242,255,.12);
}
input::placeholder{ color: rgba(234,240,255,.35); }
label{ max-width:100%; }

button,
.btn,
.tab{
  position:relative;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  gap:8px;
  min-height:38px;
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid rgba(140,170,255,.22);
  background: linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.02));
  color: var(--text);
  cursor:pointer;
  transition: transform .12s ease, box-shadow .18s ease, border-color .18s ease;
  box-shadow: 0 10px 26px rgba(0,0,0,.25);
  text-decoration:none;
  font: inherit;
  font-weight:600;
  letter-spacing:.2px;
  white-space:nowrap;
}
button:hover,
.btn:hover,
.tab:hover{
  transform: translateY(-1px);
  border-color: rgba(46,242,255,.45);
  box-shadow: 0 14px 36px rgba(0,0,0,.35);
}
button:active,
.btn:active,
.tab:active{ transform: translateY(0px) scale(.99); }
button[disabled],
.btn[disabled]{ opacity:.45; cursor:not-allowed; transform:none; box-shadow:none; }
.btn.primary,
button.primary,
.tab.active{
  border-color: rgba(124,92,255,.55);
  background:
    radial-gradient(900px 140px at 30% 50%, rgba(46,242,255,.14), transparent 60%),
    linear-gradient(180deg, rgba(124,92,255,.40), rgba(124,92,255,.12));
}
.btn.good,
button.good{
  border-color: rgba(54,249,162,.50);
  background: linear-gradient(180deg, rgba(54,249,162,.35), rgba(54,249,162,.10));
}
.btn.danger,
button.danger,
.danger{
  border-color: rgba(255,77,109,.55);
  background: linear-gradient(180deg, rgba(255,77,109,.35), rgba(255,77,109,.10));
}

hr.sep{ border:none; border-top: 1px solid rgba(255,255,255,.08); margin: 14px 0; }

/* Shared navigation injected by the script below. */
.bnsl-global-nav{
  display:flex;
  gap:8px;
  flex-wrap:wrap;
  align-items:center;
  margin: 0 0 14px;
  padding:10px;
  border-radius: var(--r20);
  border:1px solid rgba(140,170,255,.16);
  background: rgba(5, 8, 16, .36);
  box-shadow: var(--shadow2);
}
.bnsl-global-nav a{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:36px;
  padding:8px 11px;
  border-radius:12px;
  border:1px solid rgba(140,170,255,.18);
  background:rgba(255,255,255,.04);
  color:var(--text);
  text-decoration:none;
  font-size:13px;
  font-weight:700;
  white-space:nowrap;
}
.bnsl-global-nav a:hover{ border-color:rgba(46,242,255,.45); }
.bnsl-global-nav a.active{
  border-color: rgba(46,242,255,.55);
  background: rgba(46,242,255,.12);
}

/* Tables: every unwrapped table is wrapped by the script below. */
.table-wrap{
  width:100%;
  max-width:100%;
  min-width:0;
  overflow-x:auto;
  overflow-y:visible;
  -webkit-overflow-scrolling: touch;
  overscroll-behavior-x: contain;
  border-radius: 16px;
  border: 1px solid rgba(140,170,255,.12);
  background: rgba(0,0,0,.10);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.025);
}
.table-wrap:focus-within{
  box-shadow: 0 0 0 3px rgba(46,242,255,.10), inset 0 0 0 1px rgba(255,255,255,.025);
}
.table-scroll-hint{
  display:none;
  margin: 8px 2px 0;
  color: var(--muted);
  font-size:12px;
}
.table-wrap[data-scrollable="true"] + .table-scroll-hint{ display:block; }

table{
  width:100%;
  min-width: var(--bnsl-table-min-width);
  border-collapse:separate;
  border-spacing:0;
}
.table-wrap > table{
  width:100%;
  min-width: max(var(--bnsl-table-min-width), 100%);
  max-width:none;
}
thead th,
th{
  position: sticky;
  top: 0;
  z-index: 2;
  text-align:left;
  font-size:12px;
  letter-spacing:.5px;
  text-transform: uppercase;
  color: rgba(234,240,255,.78);
  background: rgba(8, 12, 24, .92);
  border-bottom: 1px solid rgba(140,170,255,.18);
  padding: 12px 12px;
  vertical-align:middle;
}
tbody td,
td{
  padding: 12px 12px;
  border-bottom: 1px solid rgba(255,255,255,.06);
  vertical-align: middle;
}
tr.row-hover,
tbody tr{ background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.00)); }
tr.row-hover:hover,
tbody tr:hover{
  background:
    radial-gradient(900px 160px at 20% 50%, rgba(46,242,255,.10), transparent 60%),
    linear-gradient(180deg, rgba(124,92,255,.10), rgba(255,255,255,0));
}
th[style], td[style]{ max-width:none; }
.actions,
.controls,
.claim-order-actions{ display:flex; gap:6px; flex-wrap:wrap; align-items:center; }
.num{ text-align:right; white-space:nowrap; }

@media (max-width: 900px){
  .brand{ align-items:flex-start; flex-direction:column; }
  .brand .right{ justify-content:flex-start; }
  .page,.wrap{ padding-left:10px; padding-right:10px; }
  .panel.pad,.panel-lite{ padding:12px; }
}

@media (max-width: 700px){
  .topbar > .pill,
  .topbar > label,
  .topbar > input,
  .topbar > select,
  .topbar > button,
  .topbar > .btn,
  .row > .pill,
  .row > label,
  .row > input,
  .row > select,
  .row > button,
  .row > .btn{
    flex: 1 1 100%;
  }
  .pill{ border-radius:14px; flex-wrap:wrap; }
  button,
  .btn,
  select,
  textarea,
  input[type="text"],
  input[type="number"],
  input[type="email"],
  input[type="search"],
  input:not([type]){
    width:100%;
  }
  .bnsl-global-nav{
    flex-wrap:nowrap;
    overflow-x:auto;
    -webkit-overflow-scrolling:touch;
    padding:8px;
  }
  .bnsl-global-nav a{ flex:0 0 auto; }
  .table-scroll-hint{ display:block; }
  th, td{ padding:10px 10px; }
}

@media (prefers-reduced-motion: reduce){
  body::before{ animation:none; }
  button,.btn,.tab,select,input,textarea{ transition:none; }
}
</style>
<script>
(function () {
  const links = [
    ["Home", "/"],
    ["Draft", "/draft/"],
    ["Rule V", "/rulev/"],
    ["Free Agency", "/fa/"],
    ["Roster", "/roster/"],
    ["Waivers", "/waivers/"],
    ["Financials", "/financials/"],
    ["Trades", "/trades/"],
    ["Admin", "/admin/"]
  ];

  function installRuntimeOverrides() {
    if (document.getElementById("bnsl-runtime-overrides")) return;
    const style = document.createElement("style");
    style.id = "bnsl-runtime-overrides";
    style.textContent = `
      body { overflow-x:auto !important; }
      .page, .wrap { width:100% !important; max-width:var(--bnsl-page-max) !important; min-width:0 !important; }
      .panel, .panel-lite { max-width:100% !important; overflow:visible !important; }
      .frame-shell, .viewport { max-width:100% !important; overflow:visible !important; }
      .table-wrap { width:100% !important; max-width:100% !important; overflow-x:auto !important; overflow-y:visible !important; -webkit-overflow-scrolling:touch !important; }
      .table-wrap > table { max-width:none !important; }
      table { border-collapse:separate; border-spacing:0; }
      button:not(.btn):not(.tab) { color:var(--text); }
    `;
    document.head.appendChild(style);
  }

  function currentSection(path) {
    if (path === "/") return "/";
    const first = path.split("/").filter(Boolean)[0] || "";
    return first ? `/${first}/` : "/";
  }

  function injectNav() {
    if (document.querySelector("[data-bnsl-nav]")) return;
    const nav = document.createElement("nav");
    nav.className = "bnsl-global-nav";
    nav.setAttribute("data-bnsl-nav", "true");
    nav.setAttribute("aria-label", "BNSL sections");
    const active = currentSection(window.location.pathname);
    for (const [label, href] of links) {
      const a = document.createElement("a");
      a.href = href;
      a.textContent = label;
      if (href === active) a.classList.add("active");
      nav.appendChild(a);
    }

    const host = document.querySelector(".page, .wrap") || document.body;
    const brand = host.querySelector(":scope > .brand");
    if (brand && brand.nextSibling) {
      host.insertBefore(nav, brand.nextSibling);
    } else if (brand) {
      host.appendChild(nav);
    } else {
      host.insertBefore(nav, host.firstChild);
    }
  }

  function tableMinWidth(table) {
    const cols = (table.tHead && table.tHead.rows[0] && table.tHead.rows[0].cells.length)
      || (table.rows[0] && table.rows[0].cells.length)
      || 0;
    if (cols >= 17) return "1680px";
    if (cols >= 15) return "1520px";
    if (cols >= 13) return "1380px";
    if (cols >= 10) return "1120px";
    if (cols >= 8) return "980px";
    return "760px";
  }

  function markScrollable(wrapper) {
    const table = wrapper.querySelector("table");
    if (!table) return;
    const update = () => {
      wrapper.dataset.scrollable = wrapper.scrollWidth > wrapper.clientWidth + 4 ? "true" : "false";
    };
    update();
    if ("ResizeObserver" in window) {
      const ro = new ResizeObserver(update);
      ro.observe(wrapper);
      ro.observe(table);
    } else {
      window.addEventListener("resize", update, { passive:true });
    }
  }

  function wrapTables(root) {
    (root || document).querySelectorAll("table").forEach(table => {
      if (table.closest(".table-wrap")) return;
      table.style.setProperty("--bnsl-table-min-width", tableMinWidth(table));
      const wrapper = document.createElement("div");
      wrapper.className = "table-wrap";
      wrapper.tabIndex = 0;
      wrapper.setAttribute("role", "region");
      wrapper.setAttribute("aria-label", "Scrollable table");
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
      const hint = document.createElement("div");
      wrapper.parentNode.insertBefore(hint, wrapper.nextSibling);
      markScrollable(wrapper);
    });
  }

  function install() {
    installRuntimeOverrides();
    injectNav();
    wrapTables(document);
    if ("MutationObserver" in window && !window.__bnslResponsiveObserver) {
      window.__bnslResponsiveObserver = new MutationObserver(mutations => {
        for (const m of mutations) {
          for (const node of m.addedNodes) {
            if (node.nodeType === 1) wrapTables(node);
          }
        }
      });
      window.__bnslResponsiveObserver.observe(document.body, { childList:true, subtree:true });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", install);
  } else {
    install();
  }
})();
</script>
"""
