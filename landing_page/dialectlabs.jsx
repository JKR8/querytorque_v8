import { useState, useEffect } from "react";

// ── ICONS ──
const Icon = ({ children, size = 20, ...props }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" {...props}>{children}</svg>
);
const ArrowRight = ({ size = 16 }) => <Icon size={size}><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></Icon>;
const ArrowUpRight = ({ size = 16 }) => <Icon size={size}><line x1="7" y1="17" x2="17" y2="7"/><polyline points="7 7 17 7 17 17"/></Icon>;
const MenuIcon = ({ size = 20 }) => <Icon size={size}><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/></Icon>;
const XIcon = ({ size = 20 }) => <Icon size={size}><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></Icon>;
const Check = ({ size = 16 }) => <Icon size={size}><polyline points="20 6 9 17 4 12"/></Icon>;
const Zap = ({ size = 16 }) => <Icon size={size}><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></Icon>;
const Shield = ({ size = 16 }) => <Icon size={size}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></Icon>;
const Database = ({ size = 16 }) => <Icon size={size}><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></Icon>;
const GitBranch = ({ size = 16 }) => <Icon size={size}><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></Icon>;
const Activity = ({ size = 16 }) => <Icon size={size}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></Icon>;
const Terminal = ({ size = 16 }) => <Icon size={size}><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></Icon>;
const Send = ({ size = 16 }) => <Icon size={size}><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></Icon>;
const Lock = ({ size = 16 }) => <Icon size={size}><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></Icon>;
const BarChart = ({ size = 16 }) => <Icon size={size}><line x1="12" y1="20" x2="12" y2="10"/><line x1="18" y1="20" x2="18" y2="4"/><line x1="6" y1="20" x2="6" y2="16"/></Icon>;
const FileText = ({ size = 16 }) => <Icon size={size}><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></Icon>;
const RefreshCw = ({ size = 16 }) => <Icon size={size}><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></Icon>;
const Mail = ({ size = 16 }) => <Icon size={size}><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></Icon>;
const Ghost = ({ size = 16 }) => <Icon size={size}><path d="M9 10h.01M15 10h.01M12 2a8 8 0 0 0-8 8v12l3-3 2 2 3-3 3 3 2-2 3 3V10a8 8 0 0 0-8-8z"/></Icon>;

const scrollTo = (id) => document.getElementById(id)?.scrollIntoView({ behavior: "smooth" });

// ── STYLES ──
const styles = `
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600;9..40,700&display=swap');
:root {
  --bg:#0a0a0b; --bg2:#111113; --bg3:#18181b; --brd:#27272a; --brd2:#1e1e21;
  --t1:#fafafa; --t2:#a1a1aa; --t3:#71717a; --acc:#e4e4e7; --accd:#52525b;
  --grn:#22c55e; --grnd:#166534; --amb:#f59e0b; --red:#ef4444;
  --mono:'DM Mono',monospace; --sans:'DM Sans',-apple-system,system-ui,sans-serif;
}
* { box-sizing:border-box; margin:0; padding:0; }
.dl-root { background:var(--bg); color:var(--t1); font-family:var(--sans); line-height:1.6; -webkit-font-smoothing:antialiased; min-height:100vh; }
.dl-root ::selection { background:#27272a; }
.container { max-width:1120px; margin:0 auto; padding:0 24px; }
@media(min-width:768px){ .container{padding:0 40px;} }
.mono { font-family:var(--mono); }
.reveal { opacity:0; transform:translateY(16px); transition:opacity .6s cubic-bezier(.16,1,.3,1),transform .6s cubic-bezier(.16,1,.3,1); }
.revealed { opacity:1!important; transform:translateY(0)!important; }
.rd1{transition-delay:.1s} .rd2{transition-delay:.2s} .rd3{transition-delay:.3s} .rd4{transition-delay:.4s}
.card { background:var(--bg2); border:1px solid var(--brd); border-radius:8px; padding:24px; transition:border-color .3s; }
.card:hover { border-color:var(--accd); }
.btn-p { display:inline-flex; align-items:center; gap:8px; padding:10px 20px; font-size:14px; font-weight:500; font-family:var(--sans); color:var(--bg); background:var(--t1); border:1px solid var(--t1); border-radius:6px; cursor:pointer; transition:all .2s; }
.btn-p:hover { background:var(--acc); border-color:var(--acc); }
.btn-s { display:inline-flex; align-items:center; gap:8px; padding:10px 20px; font-size:14px; font-weight:500; font-family:var(--sans); color:var(--t2); background:transparent; border:1px solid var(--brd); border-radius:6px; cursor:pointer; transition:all .2s; }
.btn-s:hover { color:var(--t1); border-color:var(--accd); background:var(--bg2); }
.tag { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; font-size:11px; font-family:var(--mono); letter-spacing:.05em; text-transform:uppercase; border-radius:4px; border:1px solid; }
.label { font-size:11px; font-family:var(--mono); color:var(--t3); letter-spacing:.1em; text-transform:uppercase; display:block; margin-bottom:12px; }
.input-f { width:100%; padding:10px 14px; font-size:14px; font-family:var(--sans); color:var(--t1); background:var(--bg); border:1px solid var(--brd); border-radius:6px; outline:none; transition:border-color .2s; }
.input-f:focus { border-color:var(--accd); }
.input-f::placeholder { color:var(--t3); }
.code-block { background:var(--bg); border:1px solid var(--brd); border-radius:6px; padding:16px 20px; font-family:var(--mono); font-size:13px; line-height:1.7; color:var(--t2); overflow-x:auto; }
.redacted { background:var(--brd); color:transparent; border-radius:2px; user-select:none; display:inline-block; }
.grid-bg { background-image:linear-gradient(var(--brd2) 1px,transparent 1px),linear-gradient(90deg,var(--brd2) 1px,transparent 1px); background-size:60px 60px; }
.glow-g { box-shadow:0 0 20px rgba(34,197,94,.12),0 0 60px rgba(34,197,94,.04); }
.status-dot { width:6px; height:6px; border-radius:50%; background:var(--grn); display:inline-block; position:relative; flex-shrink:0; }
@keyframes pulse-ring { 0%{transform:scale(1);opacity:.4} 100%{transform:scale(2.5);opacity:0} }
.status-dot::after { content:''; position:absolute; inset:-3px; border-radius:50%; border:1px solid var(--grn); animation:pulse-ring 2s ease-out infinite; }
@media(max-width:767px){ .hide-m{display:none!important;} }
@media(min-width:768px){ .hide-d{display:none!important;} }
.grain::after { content:''; position:fixed; inset:0; pointer-events:none; z-index:9999; opacity:.02; background:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E"); }
`;

function Navigation() {
  const [scrolled, setScrolled] = useState(false);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 20);
    window.addEventListener("scroll", fn);
    return () => window.removeEventListener("scroll", fn);
  }, []);
  const links = [{ l: "QueryTorque", id: "querytorque" }, { l: "Research", id: "research" }, { l: "Contact", id: "contact" }];
  return (
    <nav style={{ position:"fixed",top:0,left:0,right:0,zIndex:100,background:scrolled?"rgba(10,10,11,.85)":"transparent",backdropFilter:scrolled?"blur(12px)":"none",borderBottom:scrolled?"1px solid var(--brd2)":"1px solid transparent",transition:"all .3s" }}>
      <div className="container" style={{ display:"flex",alignItems:"center",justifyContent:"space-between",height:64 }}>
        <button onClick={()=>scrollTo("top")} style={{ background:"none",border:"none",cursor:"pointer" }}>
          <span className="mono" style={{ fontSize:15,fontWeight:500,color:"var(--t1)",letterSpacing:".08em" }}>dialect labs</span>
        </button>
        <div className="hide-m" style={{ display:"flex",alignItems:"center",gap:32 }}>
          {links.map(l=>(
            <button key={l.id} onClick={()=>scrollTo(l.id)} style={{ background:"none",border:"none",cursor:"pointer",fontSize:13,color:"var(--t3)",fontFamily:"var(--sans)" }}>{l.l}</button>
          ))}
          <button onClick={()=>scrollTo("contact")} className="btn-s" style={{ padding:"7px 16px",fontSize:13 }}>Request access</button>
        </div>
        <button className="hide-d" onClick={()=>setOpen(!open)} style={{ background:"none",border:"none",cursor:"pointer",color:"var(--t1)" }}>
          {open ? <XIcon size={20}/> : <MenuIcon size={20}/>}
        </button>
      </div>
      {open && (
        <div style={{ background:"var(--bg)",borderBottom:"1px solid var(--brd)",padding:"16px 24px" }}>
          {links.map(l=>(
            <button key={l.id} onClick={()=>{scrollTo(l.id);setOpen(false);}} style={{ display:"block",width:"100%",textAlign:"left",background:"none",border:"none",cursor:"pointer",padding:"12px 0",fontSize:14,color:"var(--t2)",fontFamily:"var(--sans)",borderBottom:"1px solid var(--brd2)" }}>{l.l}</button>
          ))}
        </div>
      )}
    </nav>
  );
}

function Hero() {
  const [cv, setCv] = useState(true);
  useEffect(() => { const iv = setInterval(()=>setCv(v=>!v),530); return ()=>clearInterval(iv); }, []);
  return (
    <section id="top" style={{ minHeight:"100vh",display:"flex",alignItems:"center",position:"relative",overflow:"hidden" }}>
      <div className="grid-bg" style={{ position:"absolute",inset:0,opacity:.4 }}/>
      <div style={{ position:"absolute",top:"-20%",left:"50%",transform:"translateX(-50%)",width:800,height:800,borderRadius:"50%",background:"radial-gradient(circle,rgba(34,197,94,.04) 0%,transparent 70%)",pointerEvents:"none" }}/>
      <div className="container" style={{ position:"relative",paddingTop:120,paddingBottom:80 }}>
        <div className="reveal" style={{ marginBottom:48 }}>
          <div style={{ display:"inline-flex",alignItems:"center",gap:10 }}>
            <span className="status-dot"/>
            <span className="mono" style={{ fontSize:11,color:"var(--t3)",letterSpacing:".1em",textTransform:"uppercase" }}>Systems online</span>
          </div>
        </div>
        <div className="reveal rd1">
          <h1 className="mono" style={{ fontSize:"clamp(32px,5.5vw,64px)",fontWeight:400,lineHeight:1.15,letterSpacing:"-.02em",color:"var(--t1)",maxWidth:800,marginBottom:24 }}>
            We build systems<br/>that reason about<br/>your code<span style={{ color:"var(--grn)",opacity:cv?1:0,transition:"opacity .1s" }}>_</span>
          </h1>
        </div>
        <div className="reveal rd2" style={{ maxWidth:520,marginBottom:48 }}>
          <p style={{ fontSize:16,lineHeight:1.7,color:"var(--t2)" }}>Dialect Labs develops autonomous remediation infrastructure for data systems. Our engines detect, diagnose, and resolve performance failures — without human intervention.</p>
        </div>
        <div className="reveal rd3" style={{ display:"flex",flexWrap:"wrap",gap:12,marginBottom:80 }}>
          <button onClick={()=>scrollTo("querytorque")} className="btn-p">Explore QueryTorque <ArrowRight size={16}/></button>
          <button onClick={()=>scrollTo("research")} className="btn-s">Our approach</button>
        </div>
        <div className="reveal rd4" style={{ maxWidth:600 }}>
          <div className="code-block">
            <div style={{ display:"flex",gap:6,marginBottom:16 }}>
              <span style={{ width:8,height:8,borderRadius:"50%",background:"#ef4444",opacity:.7 }}/>
              <span style={{ width:8,height:8,borderRadius:"50%",background:"#f59e0b",opacity:.7 }}/>
              <span style={{ width:8,height:8,borderRadius:"50%",background:"#22c55e",opacity:.7 }}/>
            </div>
            <div>
              <span style={{ color:"var(--grn)" }}>$</span> ado optimize --engine snowflake --benchmark tpc-ds<br/>
              <span style={{ opacity:.5 }}>▸ parsing 99 queries → DAG</span><br/>
              <span style={{ opacity:.5 }}>▸ retrieving gold examples (FAISS k=5)</span><br/>
              <span style={{ opacity:.5 }}>▸ generating candidates (n=4, parallel)</span><br/>
              <span style={{ opacity:.5 }}>▸ validating: semantic ✓  runtime ✓</span><br/><br/>
              <span style={{ color:"var(--grn)" }}>✓ 84/99 queries optimized</span><br/>
              <span style={{ opacity:.5 }}>  avg speedup: </span><span style={{ color:"var(--t1)" }}>4.7×</span><br/>
              <span style={{ opacity:.5 }}>  median gain: </span><span style={{ color:"var(--t1)" }}>85%</span><br/>
              <span style={{ opacity:.5 }}>  regressions:  </span><span style={{ color:"var(--grn)" }}>0</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Products() {
  return (
    <section style={{ padding:"80px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div className="reveal" style={{ marginBottom:48 }}>
          <span className="label">Products</span>
          <h2 style={{ fontSize:"clamp(24px,3vw,36px)",fontWeight:600,letterSpacing:"-.02em" }}>Two systems. One mission.</h2>
        </div>
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,400px),1fr))",gap:16 }}>
          <div className="reveal rd1 card glow-g" style={{ cursor:"pointer",position:"relative",overflow:"hidden" }} onClick={()=>scrollTo("querytorque")}>
            <div style={{ position:"absolute",top:0,left:0,right:0,height:2,background:"var(--grn)" }}/>
            <div style={{ display:"flex",justifyContent:"space-between",marginBottom:20 }}>
              <div>
                <span className="tag" style={{ color:"var(--grn)",borderColor:"var(--grnd)",background:"rgba(34,197,94,.05)",marginBottom:4 }}><span className="status-dot" style={{ width:4,height:4 }}/> live</span>
                <h3 className="mono" style={{ fontSize:22,fontWeight:500,marginTop:4 }}>QueryTorque</h3>
              </div>
              <ArrowUpRight size={18}/>
            </div>
            <p style={{ fontSize:14,color:"var(--t2)",lineHeight:1.65,marginBottom:20 }}>Autonomous SQL optimization engine. Detects slow queries, generates optimized rewrites, validates for correctness, and deploys — no human in the loop.</p>
            <div style={{ display:"flex",gap:24 }}>
              {[{v:"85%",l:"avg gain"},{v:"4.7×",l:"speedup"},{v:"0",l:"regressions",c:"var(--grn)"}].map((s,i)=>(
                <div key={i}><span className="mono" style={{ fontSize:24,fontWeight:500,color:s.c||"var(--t1)" }}>{s.v}</span><span style={{ display:"block",fontSize:11,color:"var(--t3)",marginTop:2 }}>{s.l}</span></div>
              ))}
            </div>
          </div>
          <div className="reveal rd2 card" style={{ position:"relative",overflow:"hidden" }}>
            <div style={{ position:"absolute",top:0,left:0,right:0,height:2,background:"var(--accd)" }}/>
            <div style={{ display:"flex",justifyContent:"space-between",marginBottom:20 }}>
              <div>
                <span className="tag" style={{ color:"var(--t3)",borderColor:"var(--brd)",background:"var(--bg)" }}>coming soon</span>
                <h3 className="mono" style={{ fontSize:22,fontWeight:500,display:"flex",alignItems:"center",gap:8,marginTop:4 }}>Phantom <Ghost size={18}/></h3>
              </div>
              <Lock size={18}/>
            </div>
            <p style={{ fontSize:14,color:"var(--t2)",lineHeight:1.65,marginBottom:20 }}>
              <span className="redacted">&nbsp;████████████████&nbsp;</span> for <span className="redacted">&nbsp;██████&nbsp;</span> systems. Applies the same autonomous remediation architecture to a <span className="redacted">&nbsp;████████████&nbsp;</span> problem space.
            </p>
            <div style={{ display:"flex",gap:24,marginBottom:20 }}>
              <div><span className="mono redacted" style={{ fontSize:24,fontWeight:500 }}>&nbsp;██&nbsp;</span><span style={{ display:"block",fontSize:11,color:"var(--t3)",marginTop:2 }}><span className="redacted">&nbsp;████████&nbsp;</span></span></div>
              <div><span className="mono redacted" style={{ fontSize:24,fontWeight:500 }}>&nbsp;██&nbsp;</span><span style={{ display:"block",fontSize:11,color:"var(--t3)",marginTop:2 }}><span className="redacted">&nbsp;████████&nbsp;</span></span></div>
            </div>
            <button onClick={()=>scrollTo("contact")} className="btn-s" style={{ padding:"7px 14px",fontSize:12 }}>Join waitlist <ArrowRight size={14}/></button>
          </div>
        </div>
      </div>
    </section>
  );
}

function QueryTorqueSection() {
  const pipeline = [
    { s:"01",t:"Parse",d:"SQL → DAG via DagBuilder. CostAnalyzer profiles execution cost per node.",icon:<Terminal size={18}/> },
    { s:"02",t:"Retrieve",d:"FAISS similarity search over gold examples with engine-specific filtering.",icon:<Database size={18}/> },
    { s:"03",t:"Generate",d:"N candidate rewrites via parallel LLM calls with structured prompting.",icon:<Zap size={18}/> },
    { s:"04",t:"Validate",d:"Syntax + semantic + runtime validation. 1-1-2-2 pattern. Zero regressions.",icon:<Shield size={18}/> },
  ];
  const caps = [
    { icon:<Activity size={18}/>,t:"SQL Optimization",d:"Autonomous rewrites for Snowflake, Databricks, PostgreSQL, BigQuery, Redshift, SQL Server." },
    { icon:<RefreshCw size={18}/>,t:"Continuous Learning",d:"Every optimization feeds the FAISS index. Regression patterns catalogued and avoided." },
    { icon:<Shield size={18}/>,t:"Quality Gates",d:"Semantic + runtime validation. Promotion thresholds. Instant rollback." },
    { icon:<GitBranch size={18}/>,t:"CI/CD Integration",d:"Hooks into deployment pipelines. Review optimized queries like code." },
    { icon:<FileText size={18}/>,t:"Auto-Documentation",d:"Full audit trail: what changed, why, measured impact." },
  ];
  return (
    <section id="querytorque" style={{ padding:"100px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div className="reveal" style={{ marginBottom:64,maxWidth:640 }}>
          <span className="label" style={{ color:"var(--grn)" }}>QueryTorque</span>
          <h2 style={{ fontSize:"clamp(28px,3.5vw,44px)",fontWeight:600,letterSpacing:"-.02em",lineHeight:1.15,marginBottom:16 }}>We fix the code.<br/>Automatically.</h2>
          <p style={{ fontSize:16,color:"var(--t2)",lineHeight:1.7 }}>Autonomous remediation engine for SQL. Monitors your warehouse, identifies underperforming queries, generates optimized rewrites, and validates every change before deployment.</p>
        </div>
        <div className="reveal" style={{ marginBottom:80 }}>
          <span className="label">The Pipeline</span>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,240px),1fr))",gap:1,background:"var(--brd)" }}>
            {pipeline.map((p,i)=>(
              <div key={i} className={`reveal rd${i+1}`} style={{ background:"var(--bg2)",padding:28 }}>
                <div style={{ display:"flex",justifyContent:"space-between",marginBottom:16 }}>
                  <span className="mono" style={{ fontSize:11,color:"var(--t3)" }}>{p.s}</span>
                  <span style={{ color:"var(--t3)" }}>{p.icon}</span>
                </div>
                <h4 className="mono" style={{ fontSize:16,fontWeight:500,marginBottom:8 }}>{p.t}</h4>
                <p style={{ fontSize:13,color:"var(--t3)",lineHeight:1.6 }}>{p.d}</p>
              </div>
            ))}
          </div>
        </div>
        <div className="reveal" style={{ marginBottom:80 }}>
          <span className="label">Capabilities</span>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,300px),1fr))",gap:16 }}>
            {caps.map((c,i)=>(
              <div key={i} className="card">
                <div style={{ color:"var(--t3)",marginBottom:12 }}>{c.icon}</div>
                <h4 style={{ fontSize:14,fontWeight:600,marginBottom:6 }}>{c.t}</h4>
                <p style={{ fontSize:13,color:"var(--t3)",lineHeight:1.6 }}>{c.d}</p>
              </div>
            ))}
          </div>
        </div>
        <div className="reveal card" style={{ position:"relative",overflow:"hidden" }}>
          <div style={{ position:"absolute",top:0,left:0,right:0,height:2,background:"linear-gradient(90deg,var(--grn),var(--amb))" }}/>
          <div style={{ display:"flex",flexWrap:"wrap",justifyContent:"space-between",alignItems:"center",gap:24 }}>
            <div style={{ flex:"1 1 400px" }}>
              <span className="label" style={{ color:"var(--amb)" }}>Benchmarks</span>
              <h3 style={{ fontSize:20,fontWeight:600,marginBottom:8 }}>TPC-DS & DSB Results</h3>
              <p style={{ fontSize:14,color:"var(--t2)",lineHeight:1.6 }}>Industry-standard benchmarks. Full methodology disclosure. Results incoming.</p>
            </div>
            <span className="tag" style={{ color:"var(--amb)",borderColor:"rgba(245,158,11,.3)",background:"rgba(245,158,11,.05)",padding:"6px 14px",fontSize:12 }}>Publishing soon</span>
          </div>
        </div>
      </div>
    </section>
  );
}

function Research() {
  return (
    <section id="research" style={{ padding:"100px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div className="reveal" style={{ marginBottom:64,maxWidth:640 }}>
          <span className="label">Research</span>
          <h2 style={{ fontSize:"clamp(24px,3vw,36px)",fontWeight:600,letterSpacing:"-.02em",lineHeight:1.2,marginBottom:16 }}>Why autonomous remediation</h2>
          <p style={{ fontSize:16,color:"var(--t2)",lineHeight:1.7 }}>Most database optimization is reactive: a human finds a slow query, rewrites it by hand, tests it, and hopes. This doesn't scale.</p>
        </div>
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,320px),1fr))",gap:32 }}>
          <div className="reveal">
            <div style={{ borderLeft:"2px solid var(--red)",paddingLeft:20 }}>
              <h3 className="mono" style={{ fontSize:13,fontWeight:500,color:"var(--red)",marginBottom:12,letterSpacing:".03em" }}>The problem</h3>
              {[{s:"67%",l:"of production queries run slower than necessary"},{s:"$2.3M",l:"average annual waste from unoptimized queries"},{s:"4.2h",l:"average time to manually optimize one query"},{s:"73%",l:"of slow queries are never addressed"}].map((x,i)=>(
                <div key={i} style={{ marginBottom:16 }}>
                  <span className="mono" style={{ fontSize:20,fontWeight:500 }}>{x.s}</span>
                  <span style={{ display:"block",fontSize:13,color:"var(--t3)",marginTop:2 }}>{x.l}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="reveal rd1">
            <div style={{ borderLeft:"2px solid var(--grn)",paddingLeft:20 }}>
              <h3 className="mono" style={{ fontSize:13,fontWeight:500,color:"var(--grn)",marginBottom:12,letterSpacing:".03em" }}>Our approach</h3>
              {[{t:"Learned example retrieval",d:"FAISS-indexed gold optimizations. Every successful rewrite makes the next one better."},{t:"Parallel candidate generation",d:"Multiple rewrites per query, scored independently. No single point of failure."},{t:"Semantic + runtime validation",d:"Equivalence verified before and after. Regressions impossible by design."},{t:"Promotion with context",d:"Optimized queries carry full reasoning forward. Compounding improvement."}].map((x,i)=>(
                <div key={i} style={{ marginBottom:20 }}>
                  <h4 style={{ fontSize:14,fontWeight:600,marginBottom:4 }}>{x.t}</h4>
                  <p style={{ fontSize:13,color:"var(--t3)",lineHeight:1.6 }}>{x.d}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Platforms() {
  const p = ["Snowflake","Databricks","BigQuery","Redshift","PostgreSQL","SQL Server","dbt","Airflow","GitHub"];
  return (
    <section style={{ padding:"60px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div className="reveal" style={{ textAlign:"center",marginBottom:32 }}><span className="label" style={{ marginBottom:0 }}>Works with</span></div>
        <div className="reveal" style={{ display:"flex",flexWrap:"wrap",justifyContent:"center",gap:"12px 24px" }}>
          {p.map(x=><span key={x} className="mono" style={{ fontSize:12,color:"var(--t3)",padding:"6px 0",opacity:.7 }}>{x}</span>)}
        </div>
      </div>
    </section>
  );
}

function Contact() {
  const [form, setForm] = useState({ name:"",email:"",company:"",message:"",interest:"querytorque" });
  const [sent, setSent] = useState(false);
  const submit = (e) => { e.preventDefault(); setSent(true); setTimeout(()=>{setSent(false);setForm({name:"",email:"",company:"",message:"",interest:"querytorque"});},4000); };
  return (
    <section id="contact" style={{ padding:"100px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,340px),1fr))",gap:48,maxWidth:880,margin:"0 auto" }}>
          <div className="reveal">
            <span className="label">Contact</span>
            <h2 style={{ fontSize:"clamp(24px,3vw,32px)",fontWeight:600,letterSpacing:"-.02em",marginBottom:16 }}>Let's talk.</h2>
            <p style={{ fontSize:14,color:"var(--t2)",lineHeight:1.7,marginBottom:32 }}>Interested in QueryTorque, curious about Phantom, or want to discuss the research? We respond within 24 hours.</p>
            <div style={{ display:"flex",alignItems:"center",gap:12 }}>
              <div style={{ width:36,height:36,borderRadius:6,background:"var(--bg3)",border:"1px solid var(--brd)",display:"flex",alignItems:"center",justifyContent:"center",color:"var(--t3)" }}><Mail size={16}/></div>
              <div><span style={{ fontSize:12,color:"var(--t3)",display:"block" }}>Email</span><span style={{ fontSize:14 }}>hello@dialectlabs.io</span></div>
            </div>
          </div>
          <div className="reveal rd1">
            <div className="card">
              {sent ? (
                <div style={{ textAlign:"center",padding:"40px 0" }}>
                  <div style={{ width:40,height:40,borderRadius:8,background:"rgba(34,197,94,.1)",border:"1px solid var(--grnd)",display:"flex",alignItems:"center",justifyContent:"center",margin:"0 auto 16px",color:"var(--grn)" }}><Check size={20}/></div>
                  <h4 style={{ fontSize:16,fontWeight:600,marginBottom:4 }}>Message sent</h4>
                  <p style={{ fontSize:13,color:"var(--t3)" }}>We'll be in touch within 24 hours.</p>
                </div>
              ) : (
                <form onSubmit={submit} style={{ display:"flex",flexDirection:"column",gap:14 }}>
                  <div><label style={{ fontSize:12,color:"var(--t3)",display:"block",marginBottom:4 }}>Name</label><input type="text" value={form.name} onChange={e=>setForm({...form,name:e.target.value})} required className="input-f" placeholder="Your name"/></div>
                  <div><label style={{ fontSize:12,color:"var(--t3)",display:"block",marginBottom:4 }}>Email</label><input type="email" value={form.email} onChange={e=>setForm({...form,email:e.target.value})} required className="input-f" placeholder="you@company.com"/></div>
                  <div><label style={{ fontSize:12,color:"var(--t3)",display:"block",marginBottom:4 }}>Company</label><input type="text" value={form.company} onChange={e=>setForm({...form,company:e.target.value})} className="input-f" placeholder="Company name"/></div>
                  <div>
                    <label style={{ fontSize:12,color:"var(--t3)",display:"block",marginBottom:4 }}>Interest</label>
                    <div style={{ display:"flex",gap:8 }}>
                      {["querytorque","phantom","both"].map(o=>(
                        <button key={o} type="button" onClick={()=>setForm({...form,interest:o})} style={{ flex:1,padding:"8px 12px",fontSize:12,fontFamily:"var(--mono)",background:form.interest===o?"var(--bg3)":"var(--bg)",border:`1px solid ${form.interest===o?"var(--accd)":"var(--brd)"}`,color:form.interest===o?"var(--t1)":"var(--t3)",borderRadius:6,cursor:"pointer",transition:"all .2s",textTransform:"capitalize" }}>{o}</button>
                      ))}
                    </div>
                  </div>
                  <div><label style={{ fontSize:12,color:"var(--t3)",display:"block",marginBottom:4 }}>Message</label><textarea value={form.message} onChange={e=>setForm({...form,message:e.target.value})} required className="input-f" placeholder="Tell us about your data challenges..." rows={4} style={{ resize:"vertical",minHeight:100 }}/></div>
                  <button type="submit" className="btn-p" style={{ width:"100%",justifyContent:"center",marginTop:4 }}><Send size={14}/> Send message</button>
                </form>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function CTA() {
  return (
    <section style={{ padding:"80px 0",borderTop:"1px solid var(--brd)" }}>
      <div className="container">
        <div className="reveal" style={{ background:"var(--bg3)",border:"1px solid var(--brd)",borderRadius:8,padding:"clamp(32px,5vw,64px)",textAlign:"center",position:"relative",overflow:"hidden" }}>
          <div style={{ position:"absolute",top:0,left:0,right:0,height:1,background:"linear-gradient(90deg,transparent,var(--grn),transparent)" }}/>
          <h2 className="mono" style={{ fontSize:"clamp(20px,3vw,32px)",fontWeight:400,letterSpacing:"-.01em",marginBottom:12 }}>Stop tuning. Start shipping.</h2>
          <p style={{ fontSize:15,color:"var(--t2)",marginBottom:32,maxWidth:440,margin:"0 auto 32px" }}>Let your queries optimize themselves.</p>
          <div style={{ display:"flex",flexWrap:"wrap",justifyContent:"center",gap:12 }}>
            <button onClick={()=>scrollTo("contact")} className="btn-p">Request access <ArrowRight size={16}/></button>
            <button onClick={()=>scrollTo("contact")} className="btn-s"><Ghost size={16}/> Phantom waitlist</button>
          </div>
        </div>
      </div>
    </section>
  );
}

function Footer() {
  return (
    <footer style={{ borderTop:"1px solid var(--brd)",padding:"40px 0" }}>
      <div className="container" style={{ display:"flex",flexWrap:"wrap",justifyContent:"space-between",alignItems:"center",gap:16 }}>
        <div><span className="mono" style={{ fontSize:13,fontWeight:500,color:"var(--t2)",letterSpacing:".06em" }}>dialect labs</span><span style={{ fontSize:12,color:"var(--t3)",marginLeft:16 }}>© 2025</span></div>
        <div style={{ display:"flex",gap:24 }}>
          {["Privacy","Terms"].map(l=><button key={l} style={{ background:"none",border:"none",cursor:"pointer",fontSize:12,color:"var(--t3)",fontFamily:"var(--sans)" }}>{l}</button>)}
        </div>
      </div>
    </footer>
  );
}

export default function DialectLabs() {
  useEffect(() => {
    const obs = new IntersectionObserver(es => es.forEach(e => { if(e.isIntersecting){e.target.classList.add("revealed");obs.unobserve(e.target);} }),{ threshold:.08,rootMargin:"0px 0px -40px 0px" });
    document.querySelectorAll(".reveal").forEach(el=>obs.observe(el));
    return ()=>obs.disconnect();
  }, []);
  useEffect(() => {
    const t = setTimeout(()=>{
      const obs = new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting){e.target.classList.add("revealed");obs.unobserve(e.target);}}),{threshold:.08,rootMargin:"0px 0px -40px 0px"});
      document.querySelectorAll(".reveal:not(.revealed)").forEach(el=>obs.observe(el));
    },200);
    return ()=>clearTimeout(t);
  });
  return (
    <>
      <style>{styles}</style>
      <div className="dl-root grain">
        <Navigation/>
        <main>
          <Hero/>
          <Products/>
          <QueryTorqueSection/>
          <Platforms/>
          <Research/>
          <CTA/>
          <Contact/>
        </main>
        <Footer/>
      </div>
    </>
  );
}
