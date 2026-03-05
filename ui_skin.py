# ui_skin.py
BNSL_GAME_CSS = r"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Inter:wght@400;500;600&display=swap');

:root{
  --bg0:#070A12;
  --bg1:#0A1020;
  --panel: rgba(18, 26, 48, .62);
  --panel2: rgba(12, 18, 34, .70);
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
}

*{ box-sizing:border-box; }
html, body{ height:100%; }
body{
  margin:0;
  color: var(--text);
  background:
    radial-gradient(1100px 680px at 70% -10%, rgba(124,92,255,.25), transparent 55%),
    radial-gradient(900px 620px at 20% 0%, rgba(46,242,255,.16), transparent 52%),
    radial-gradient(900px 700px at 70% 80%, rgba(255,209,102,.08), transparent 55%),
    linear-gradient(180deg, var(--bg0), var(--bg1));
  font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  overflow-x:hidden;
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

.page{ max-width: 1400px; margin: 0 auto; padding: 22px 18px 40px; }

.brand{ display:flex; align-items:flex-end; justify-content:space-between; gap: 18px; margin: 10px 0 16px; }
.brand h1{ margin:0; font-family: Rajdhani, Inter, system-ui; font-weight:700; letter-spacing:.6px; font-size:34px; line-height:1; }
.brand .sub{ margin-top:6px; color: var(--muted); font-size:13px; }
.brand .right{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }

.badge{
  font-size: 12px; padding: 6px 10px; border-radius: 999px;
  background: rgba(124,92,255,.14); border: 1px solid rgba(124,92,255,.28);
  color: rgba(234,240,255,.92); letter-spacing:.2px; white-space:nowrap;
}

.panel{
  background: var(--panel);
  border: 1px solid var(--stroke);
  border-radius: var(--r20);
  box-shadow: var(--shadow);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  overflow:hidden;
}
.panel.pad{ padding: 14px; }

.topbar{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; }

.pill{
  display:inline-flex; gap:10px; align-items:center;
  padding: 10px 12px; border-radius: 999px;
  background: rgba(10, 16, 32, .55);
  border: 1px solid var(--stroke2);
  box-shadow: 0 6px 20px rgba(0,0,0,.25);
  color: var(--text);
}
.muted{ color: var(--muted); }

select, input[type="text"], input[type="number"]{
  background: rgba(5, 8, 16, .55);
  color: var(--text);
  border: 1px solid rgba(140,170,255,.22);
  border-radius: 12px;
  padding: 10px 12px;
  outline: none;
  transition: border-color .18s ease, box-shadow .18s ease;
}
select:focus, input:focus{
  border-color: rgba(46,242,255,.55);
  box-shadow: 0 0 0 3px rgba(46,242,255,.12);
}
input::placeholder{ color: rgba(234,240,255,.35); }

.btn{
  position:relative;
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid rgba(140,170,255,.22);
  background: linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.02));
  color: var(--text);
  cursor:pointer;
  transition: transform .12s ease, box-shadow .18s ease, border-color .18s ease;
  box-shadow: 0 10px 26px rgba(0,0,0,.25);
}
.btn:hover{
  transform: translateY(-1px);
  border-color: rgba(46,242,255,.45);
  box-shadow: 0 14px 36px rgba(0,0,0,.35);
}
.btn:active{ transform: translateY(0px) scale(.99); }
.btn[disabled]{ opacity:.45; cursor:not-allowed; transform:none; box-shadow:none; }
.btn.primary{
  border-color: rgba(124,92,255,.55);
  background: linear-gradient(180deg, rgba(124,92,255,.55), rgba(124,92,255,.18));
}
.btn.good{
  border-color: rgba(54,249,162,.50);
  background: linear-gradient(180deg, rgba(54,249,162,.35), rgba(54,249,162,.10));
}
.btn.danger{
  border-color: rgba(255,77,109,.55);
  background: linear-gradient(180deg, rgba(255,77,109,.35), rgba(255,77,109,.10));
}

hr.sep{ border:none; border-top: 1px solid rgba(255,255,255,.08); margin: 14px 0; }

.table-wrap{ overflow:auto; }
table{ width:100%; border-collapse:separate; border-spacing:0; min-width: 980px; }
thead th{
  position: sticky; top: 0; z-index: 2;
  text-align:left; font-size:12px; letter-spacing:.5px; text-transform: uppercase;
  color: rgba(234,240,255,.78);
  background: rgba(8, 12, 24, .85);
  border-bottom: 1px solid rgba(140,170,255,.18);
  padding: 12px 12px;
}
tbody td{ padding: 12px 12px; border-bottom: 1px solid rgba(255,255,255,.06); vertical-align: middle; }
tr.row-hover{ background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.00)); }
tr.row-hover:hover{
  background:
    radial-gradient(900px 160px at 20% 50%, rgba(46,242,255,.10), transparent 60%),
    linear-gradient(180deg, rgba(124,92,255,.10), rgba(255,255,255,0));
}

@media (max-width: 900px){
  .brand{ align-items:flex-start; flex-direction:column; }
  table{ min-width: 860px; }
}
@media (prefers-reduced-motion: reduce){
  body::before{ animation:none; }
  .btn, select, input{ transition:none; }
}
</style>
"""
