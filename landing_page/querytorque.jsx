import { useState, useEffect, useRef } from "react";

/* ——— icons ——— */
const I=({children,size=20,...p})=><svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" {...p}>{children}</svg>;
const ArrowR=({size=15})=><I size={size}><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></I>;
const MenuIc=({size=20})=><I size={size}><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/></I>;
const XIc=({size=20})=><I size={size}><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></I>;
const Chk=({size=14})=><I size={size}><polyline points="20 6 9 17 4 12"/></I>;
const ChvD=({size=14})=><I size={size}><polyline points="6 9 12 15 18 9"/></I>;
const Zap=({size=16})=><I size={size}><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></I>;
const Shld=({size=16})=><I size={size}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></I>;
const Db=({size=16})=><I size={size}><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></I>;
const GitB=({size=16})=><I size={size}><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></I>;
const ActI=({size=16})=><I size={size}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></I>;
const Term=({size=16})=><I size={size}><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></I>;
const SendI=({size=14})=><I size={size}><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></I>;
const BarI=({size=16})=><I size={size}><line x1="12" y1="20" x2="12" y2="10"/><line x1="18" y1="20" x2="18" y2="4"/><line x1="6" y1="20" x2="6" y2="16"/></I>;
const FileI=({size=16})=><I size={size}><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></I>;
const RefI=({size=16})=><I size={size}><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></I>;
const MailI=({size=14})=><I size={size}><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></I>;
const DollI=({size=16})=><I size={size}><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></I>;
const TargI=({size=16})=><I size={size}><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></I>;
const TrndI=({size=16})=><I size={size}><polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/></I>;
const AlertI=({size=16})=><I size={size}><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></I>;
const LayerI=({size=16})=><I size={size}><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></I>;
const UserI=({size=16})=><I size={size}><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></I>;
const XOct=({size=16})=><I size={size}><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/><circle cx="12" cy="12" r="10"/></I>;
const BeakerI=({size=16})=><I size={size}><path d="M4.5 3h15"/><path d="M6 3v16a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V3"/><path d="M6 14h12"/></I>;
const ClockI=({size=16})=><I size={size}><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></I>;

const go=id=>document.getElementById(id)?.scrollIntoView({behavior:"smooth"});

/* ——— css ——— */
const css=`
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600;9..40,700&display=swap');
:root{
  --bg:#fff;--bg2:#f8fafc;--bg3:#f1f5f9;--brd:#e2e8f0;--brd2:#cbd5e1;
  --t1:#0f172a;--t2:#475569;--t3:#94a3b8;
  --grn:#16a34a;--grn2:#dcfce7;
  --red:#dc2626;--red2:#fef2f2;
  --amb:#d97706;--amb2:#fffbeb;
  --blu:#2563eb;--blu2:#eff6ff;
  --mono:'DM Mono',monospace;--sans:'DM Sans',-apple-system,system-ui,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
.qt{background:var(--bg);color:var(--t1);font-family:var(--sans);line-height:1.6;-webkit-font-smoothing:antialiased}
.qt ::selection{background:#dbeafe}
.cx{max-width:1060px;margin:0 auto;padding:0 24px}
@media(min-width:768px){.cx{padding:0 40px}}
.rv{opacity:0;transform:translateY(11px);transition:opacity .5s cubic-bezier(.16,1,.3,1),transform .5s cubic-bezier(.16,1,.3,1)}
.vis{opacity:1!important;transform:translateY(0)!important}
.d1{transition-delay:.06s}.d2{transition-delay:.12s}.d3{transition-delay:.18s}.d4{transition-delay:.24s}.d5{transition-delay:.3s}.d6{transition-delay:.36s}
.lb{font-size:11px;font-family:var(--mono);color:var(--t3);letter-spacing:.1em;text-transform:uppercase;display:block;margin-bottom:10px}
.cd{background:var(--bg);border:1px solid var(--brd);border-radius:10px;padding:24px;transition:border-color .2s,box-shadow .2s}
.cd:hover{border-color:var(--brd2);box-shadow:0 1px 6px rgba(0,0,0,.025)}
.bp{display:inline-flex;align-items:center;gap:8px;padding:11px 22px;font-size:14px;font-weight:500;font-family:var(--sans);color:#fff;background:var(--t1);border:1px solid var(--t1);border-radius:6px;cursor:pointer;transition:all .15s;text-decoration:none}
.bp:hover{background:#1e293b}
.bs{display:inline-flex;align-items:center;gap:8px;padding:11px 22px;font-size:14px;font-weight:500;font-family:var(--sans);color:var(--t2);background:transparent;border:1px solid var(--brd);border-radius:6px;cursor:pointer;transition:all .15s;text-decoration:none}
.bs:hover{color:var(--t1);border-color:var(--brd2);background:var(--bg2)}
.tg{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;font-size:10px;font-family:var(--mono);letter-spacing:.06em;text-transform:uppercase;border-radius:4px;border:1px solid}
.inf{width:100%;padding:10px 14px;font-size:14px;font-family:var(--sans);color:var(--t1);background:var(--bg);border:1px solid var(--brd);border-radius:6px;outline:none;transition:border-color .15s}
.inf:focus{border-color:var(--brd2);box-shadow:0 0 0 3px rgba(148,163,184,.06)}
.inf::placeholder{color:var(--t3)}
.sec{padding:80px 0}.alt{padding:80px 0;background:var(--bg2)}
.trm{background:#0f172a;border-radius:10px;padding:20px 24px;font-family:var(--mono);font-size:12.5px;line-height:1.8;color:#64748b;overflow-x:auto;border:1px solid #1e293b}
@media(max-width:767px){.dm{display:none!important}}
@media(min-width:768px){.dd{display:none!important}}
`;

/* ——— reveal hook ——— */
function useReveal(){
  useEffect(()=>{
    const init=()=>{
      const o=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting){e.target.classList.add("vis");o.unobserve(e.target)}}),{threshold:.04,rootMargin:"0px 0px -16px 0px"});
      document.querySelectorAll(".rv:not(.vis)").forEach(el=>o.observe(el));return o;
    };
    const o=init();const t=setTimeout(init,180);
    return()=>{o.disconnect();clearTimeout(t)};
  });
}

/* ═══════════════════════════════════════
   NAV — sticky, demo always visible
═══════════════════════════════════════ */
function Nav(){
  const[s,setS]=useState(false);
  const[o,setO]=useState(false);
  useEffect(()=>{const f=()=>setS(window.scrollY>8);window.addEventListener("scroll",f);return()=>window.removeEventListener("scroll",f)},[]);
  const lnk=[{l:"How it works",id:"how"},{l:"Comparison",id:"compare"},{l:"Results",id:"results"},{l:"Pricing",id:"pricing"}];
  return(
    <nav style={{position:"fixed",top:0,left:0,right:0,zIndex:100,background:s?"rgba(255,255,255,.92)":"rgba(255,255,255,.4)",backdropFilter:"blur(12px)",borderBottom:s?"1px solid var(--brd)":"1px solid transparent",transition:"all .2s"}}>
      <div className="cx" style={{display:"flex",alignItems:"center",justifyContent:"space-between",height:56}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <button onClick={()=>go("top")} style={{background:"none",border:"none",cursor:"pointer",display:"flex",alignItems:"center",gap:8}}>
            <span style={{fontSize:15,fontWeight:600,color:"var(--t1)",letterSpacing:"-.01em"}}>QueryTorque</span>
          </button>
          <span style={{width:1,height:14,background:"var(--brd)"}}/>
          <span style={{fontSize:11,fontFamily:"var(--mono)",color:"var(--t3)",letterSpacing:".04em"}}>by Dialect Labs</span>
        </div>
        <div className="dm" style={{display:"flex",alignItems:"center",gap:24}}>
          {lnk.map(l=><button key={l.id} onClick={()=>go(l.id)} style={{background:"none",border:"none",cursor:"pointer",fontSize:13,color:"var(--t3)",fontFamily:"var(--sans)"}}>{l.l}</button>)}
          <button onClick={()=>go("contact")} className="bp" style={{padding:"7px 16px",fontSize:13}}>Book a demo</button>
        </div>
        <button className="dd" onClick={()=>setO(!o)} style={{background:"none",border:"none",cursor:"pointer",color:"var(--t1)"}}>{o?<XIc/>:<MenuIc/>}</button>
      </div>
      {o&&<div style={{background:"var(--bg)",borderBottom:"1px solid var(--brd)",padding:"8px 24px"}}>
        {lnk.map(l=><button key={l.id} onClick={()=>{go(l.id);setO(false)}} style={{display:"block",width:"100%",textAlign:"left",background:"none",border:"none",cursor:"pointer",padding:"10px 0",fontSize:14,color:"var(--t2)",fontFamily:"var(--sans)",borderBottom:"1px solid var(--bg2)"}}>{l.l}</button>)}
        <button onClick={()=>{go("contact");setO(false)}} style={{display:"block",width:"100%",textAlign:"left",background:"none",border:"none",cursor:"pointer",padding:"10px 0",fontSize:14,color:"var(--t1)",fontWeight:600,fontFamily:"var(--sans)"}}>Book a demo</button>
      </div>}
    </nav>
  );
}

/* ═══════════════════════════════════════
   1. HERO — credibility-first, measured claims
═══════════════════════════════════════ */
function Hero(){
  return(
    <section id="top" style={{paddingTop:112,paddingBottom:0}}>
      <div className="cx">
        <div className="rv" style={{display:"flex",flexWrap:"wrap",alignItems:"center",gap:12,marginBottom:20}}>
          <span className="tg" style={{color:"var(--grn)",borderColor:"var(--grn2)",background:"rgba(22,163,74,.04)"}}>
            <span style={{width:5,height:5,borderRadius:"50%",background:"var(--grn)",display:"inline-block"}}/> TPC-DS benchmarked
          </span>
          <span className="tg" style={{color:"var(--t2)",borderColor:"var(--brd)",background:"var(--bg2)"}}>
            <BeakerI size={10}/> From Dialect Labs
          </span>
        </div>

        <h1 className="rv d1" style={{fontSize:"clamp(32px,5.5vw,54px)",fontWeight:700,lineHeight:1.05,letterSpacing:"-.04em",color:"var(--t1)",maxWidth:700,marginBottom:20}}>
          Automated SQL optimization<br/>that proves itself before deploy.
        </h1>

        <p className="rv d2" style={{fontSize:17,lineHeight:1.7,color:"var(--t2)",maxWidth:560,marginBottom:14}}>
          QueryTorque rewrites your costliest queries, validates every change for semantic equivalence, and only promotes what's measurably faster. No regressions. No guesswork.
        </p>

        <div className="rv d3" style={{display:"flex",flexWrap:"wrap",gap:10,marginBottom:56}}>
          <button onClick={()=>go("contact")} className="bp">Book a demo <ArrowR/></button>
          <button onClick={()=>go("how")} className="bs">See how it works</button>
        </div>

        {/* benchmark strip */}
        <div className="rv d4" style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(148px,1fr))",gap:1,background:"var(--brd)",borderRadius:10,overflow:"hidden"}}>
          {[
            {v:"33%",l:"median speedup",n:"TPC-DS benchmark"},
            {v:"6–10×",l:"on hardest queries",n:"Where most cost concentrates"},
            {v:"30–40%",l:"compute reduction",n:"Early production pilots"},
            {v:"4-stage",l:"validation pipeline",n:"Syntax → semantic → runtime"},
          ].map((m,i)=>(
            <div key={i} style={{background:"var(--bg)",padding:"20px 22px"}}>
              <span style={{fontFamily:"var(--mono)",fontSize:26,fontWeight:500,color:"var(--t1)",lineHeight:1,display:"block"}}>{m.v}</span>
              <span style={{fontSize:13,fontWeight:500,color:"var(--t1)",display:"block",marginTop:4}}>{m.l}</span>
              <span style={{fontSize:11,color:"var(--t3)",display:"block",marginTop:2}}>{m.n}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   2. PAIN — consolidated problem + gap
═══════════════════════════════════════ */
function Pain(){
  return(
    <section id="pain" className="sec">
      <div className="cx">
        <div className="rv" style={{maxWidth:620,marginBottom:40}}>
          <span className="lb">The problem</span>
          <h2 style={{fontSize:"clamp(22px,3vw,32px)",fontWeight:600,letterSpacing:"-.025em",lineHeight:1.15,marginBottom:12}}>
            You can see the waste. You just can't fix it fast enough.
          </h2>
          <p style={{fontSize:15,lineHeight:1.75,color:"var(--t2)"}}>
            FinOps dashboards flag expensive queries. Your warehouse UI reports per-cluster cost. But actually rewriting the SQL — restructuring joins, eliminating correlated subqueries, fixing anti-patterns — is manual, backlogged, and depends on tribal knowledge that walks out the door.
          </p>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,290px),1fr))",gap:14,marginBottom:40}}>
          {[
            {icon:<DollI size={18}/>,v:"20–40%",l:"of warehouse compute",d:"Industry estimates suggest a significant share of cloud data warehouse spend goes to queries that could be written more efficiently.",color:"var(--amb)",bg:"var(--amb2)",src:"Xomnia, FinOps Foundation"},
            {icon:<ClockI size={18}/>,v:"70%+",l:"of flagged queries",d:"Most slow queries identified by monitoring tools are never actually optimized — the detection exists, the remediation doesn't.",color:"var(--blu)",bg:"var(--blu2)",src:"FinOps practitioner surveys"},
            {icon:<UserI size={18}/>,v:"~20/week",l:"per senior DBA",d:"Even experienced engineers can only hand-tune a fraction of the queries that need attention. At $150K+ fully loaded, it doesn't scale.",color:"var(--t1)",bg:"var(--bg2)",src:"Industry benchmarks"},
          ].map((c,i)=>(
            <div key={i} className={`rv cd d${i+1}`}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12}}>
                <div style={{width:30,height:30,borderRadius:7,background:c.bg,display:"flex",alignItems:"center",justifyContent:"center",color:c.color,flexShrink:0}}>{c.icon}</div>
                <div>
                  <span style={{fontFamily:"var(--mono)",fontSize:20,fontWeight:500,lineHeight:1}}>{c.v}</span>
                  <span style={{display:"block",fontSize:11,color:"var(--t3)",lineHeight:1.2}}>{c.l}</span>
                </div>
              </div>
              <p style={{fontSize:12.5,color:"var(--t3)",lineHeight:1.6,marginBottom:8}}>{c.d}</p>
              <span style={{fontSize:10,fontFamily:"var(--mono)",color:"var(--t3)",opacity:.7}}>{c.src}</span>
            </div>
          ))}
        </div>

        {/* gap visual */}
        <div className="rv d2" style={{display:"flex",flexWrap:"wrap",alignItems:"center",justifyContent:"center",gap:8}}>
          {[
            {l:"FinOps Dashboard",s:"detects cost"},
            {l:"Query Monitor",s:"flags slowness"},
          ].map((t,i)=>(
            <div key={i} style={{display:"flex",alignItems:"center",gap:6}}>
              <div style={{padding:"6px 12px",background:"var(--bg2)",border:"1px solid var(--brd)",borderRadius:6,fontSize:12,fontFamily:"var(--mono)"}}>
                <span style={{color:"var(--t1)",fontWeight:500}}>{t.l}</span>
                <span style={{color:"var(--t3)",marginLeft:6}}>{t.s}</span>
              </div>
              <span style={{color:"var(--t3)",fontSize:12}}>→</span>
            </div>
          ))}
          <div style={{padding:"4px 10px",borderRadius:6,border:"2px dashed var(--red)",background:"var(--red2)"}}>
            <span style={{fontSize:12,fontFamily:"var(--mono)",color:"var(--red)",fontWeight:500}}>manual rewrite backlog</span>
          </div>
          <span style={{color:"var(--t3)",fontSize:12}}>→</span>
          <div style={{padding:"6px 12px",background:"var(--t1)",border:"1px solid var(--t1)",borderRadius:6,fontSize:12,fontFamily:"var(--mono)",color:"#fff",fontWeight:500}}>
            QueryTorque <span style={{color:"var(--grn)",marginLeft:4}}>automates this step</span>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   3. HOW IT WORKS — pipeline + terminal
═══════════════════════════════════════ */
function How(){
  return(
    <section id="how" className="alt">
      <div className="cx">
        <div className="rv" style={{maxWidth:540,marginBottom:40}}>
          <span className="lb">How it works</span>
          <h2 style={{fontSize:"clamp(22px,3vw,32px)",fontWeight:600,letterSpacing:"-.025em",lineHeight:1.15,marginBottom:8}}>
            Four stages. Human review at every gate.
          </h2>
          <p style={{fontSize:15,color:"var(--t2)",lineHeight:1.65}}>Start with full review on every change. Go autonomous on queries you trust, at thresholds you set. There's no cliff — you control the dial.</p>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,230px),1fr))",gap:1,background:"var(--brd)",borderRadius:10,overflow:"hidden",marginBottom:40}}>
          {[
            {s:"01",t:"Parse & profile",d:"Your SQL is parsed into a directed acyclic graph. CostAnalyzer profiles every node and flags anti-patterns — correlated subqueries, redundant joins, type mismatches.",icon:<Term size={18}/>},
            {s:"02",t:"Retrieve context",d:"FAISS similarity search finds relevant optimization patterns from our index — filtered by your engine dialect and query shape. Not generic advice; engine-specific precedent.",icon:<Db size={18}/>},
            {s:"03",t:"Generate candidates",d:"Multiple candidate rewrites generated in parallel, each grounded in DAG context, engine constraints, and retrieved examples. Not one suggestion — a ranked set.",icon:<Zap size={18}/>},
            {s:"04",t:"Validate & promote",d:"Syntax check → semantic equivalence → runtime benchmark against your baseline. Only changes that are measurably faster and semantically identical get promoted.",icon:<Shld size={18}/>},
          ].map((p,i)=>(
            <div key={i} className={`rv d${i+1}`} style={{background:"var(--bg)",padding:22}}>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:12}}>
                <span style={{fontFamily:"var(--mono)",fontSize:11,color:"var(--t3)"}}>{p.s}</span>
                <span style={{color:"var(--t3)"}}>{p.icon}</span>
              </div>
              <h4 style={{fontFamily:"var(--mono)",fontSize:14,fontWeight:500,marginBottom:5}}>{p.t}</h4>
              <p style={{fontSize:12.5,color:"var(--t3)",lineHeight:1.6}}>{p.d}</p>
            </div>
          ))}
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,300px),1fr))",gap:24,alignItems:"start"}}>
          <div className="rv">
            <div className="trm">
              <div style={{display:"flex",gap:5,marginBottom:12}}>
                <span style={{width:7,height:7,borderRadius:"50%",background:"#ef4444",opacity:.4}}/>
                <span style={{width:7,height:7,borderRadius:"50%",background:"#f59e0b",opacity:.4}}/>
                <span style={{width:7,height:7,borderRadius:"50%",background:"#22c55e",opacity:.4}}/>
              </div>
              <span style={{color:"#22c55e"}}>$</span>{" querytorque run --engine snowflake --warehouse analytics"}<br/>
              <span style={{opacity:.5}}>{"▸ 2,847 queries parsed → DAG"}</span><br/>
              <span style={{opacity:.5}}>{"▸ FAISS retrieval (k=5, engine=snowflake)"}</span><br/>
              <span style={{opacity:.5}}>{"▸ 4 candidates/query, parallel"}</span><br/>
              <span style={{opacity:.5}}>{"▸ validation: syntax ✓  semantic ✓  runtime ✓"}</span><br/><br/>
              <span style={{color:"#22c55e"}}>{"✓ complete"}</span><br/>
              <span style={{opacity:.5}}>{"  optimized:  "}</span><span style={{color:"#e2e8f0"}}>1,923 of 2,847</span><br/>
              <span style={{opacity:.5}}>{"  median:     "}</span><span style={{color:"#e2e8f0"}}>33% faster</span><br/>
              <span style={{opacity:.5}}>{"  p90:        "}</span><span style={{color:"#e2e8f0"}}>33% faster</span><br/>
              <span style={{opacity:.5}}>{"  est. save:  "}</span><span style={{color:"#22c55e"}}>$14,200/mo</span><br/>
              <span style={{opacity:.5}}>{"  regr:       "}</span><span style={{color:"#22c55e"}}>0</span>
            </div>
          </div>

          <div className="rv d1" style={{display:"flex",flexDirection:"column",gap:12}}>
            <span className="lb" style={{marginBottom:0}}>Deployment modes</span>
            {[
              {t:"Monitor only",d:"Scan and report. No changes applied. See what QueryTorque would do before you commit.",tag:"Start here",tagColor:"var(--blu)"},
              {t:"Review & approve",d:"Optimizations queued as PRs or change requests. Your team reviews, approves, merges.",tag:"Most teams",tagColor:"var(--grn)"},
              {t:"Autonomous",d:"Confidence-threshold auto-deploy on approved query classes. Instant rollback on any regression.",tag:"When ready",tagColor:"var(--amb)"},
            ].map((m,i)=>(
              <div key={i} style={{padding:"14px 16px",background:"var(--bg)",border:"1px solid var(--brd)",borderRadius:8}}>
                <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}>
                  <h4 style={{fontSize:14,fontWeight:600}}>{m.t}</h4>
                  <span style={{fontSize:9,fontFamily:"var(--mono)",color:m.tagColor,letterSpacing:".06em",textTransform:"uppercase"}}>{m.tag}</span>
                </div>
                <p style={{fontSize:12.5,color:"var(--t3)",lineHeight:1.55}}>{m.d}</p>
              </div>
            ))}
          </div>
        </div>

        {/* platform support */}
        <div className="rv" style={{display:"flex",flexWrap:"wrap",justifyContent:"center",gap:"4px 22px",marginTop:40,paddingTop:24,borderTop:"1px solid var(--brd)"}}>
          {["Snowflake","Databricks","BigQuery","Redshift","PostgreSQL","SQL Server","DuckDB","dbt","Airflow"].map(p=>(
            <span key={p} style={{fontFamily:"var(--mono)",fontSize:11,color:"var(--t3)",padding:"4px 0"}}>{p}</span>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   4. COMPARISON — honest, not defensive
═══════════════════════════════════════ */
function Compare(){
  return(
    <section id="compare" className="sec">
      <div className="cx">
        <div className="rv" style={{maxWidth:560,marginBottom:40}}>
          <span className="lb">How we compare</span>
          <h2 style={{fontSize:"clamp(22px,3vw,30px)",fontWeight:600,letterSpacing:"-.025em",lineHeight:1.15,marginBottom:8}}>
            Different tools solve different parts of this.
          </h2>
          <p style={{fontSize:15,color:"var(--t2)",lineHeight:1.65}}>We're not replacing your engine optimizer or your DBAs. We're filling the gap between "we know this query is slow" and "someone rewrote it."</p>
        </div>

        {/* comparison table */}
        <div className="rv" style={{overflowX:"auto",marginBottom:32}}>
          <div style={{minWidth:640}}>
            {/* header */}
            <div style={{display:"grid",gridTemplateColumns:"180px 1fr 1fr",gap:0,borderBottom:"2px solid var(--brd)",paddingBottom:8,marginBottom:4}}>
              <span style={{fontSize:11,fontFamily:"var(--mono)",color:"var(--t3)",textTransform:"uppercase",letterSpacing:".08em"}}></span>
              <span style={{fontSize:11,fontFamily:"var(--mono)",color:"var(--t3)",textTransform:"uppercase",letterSpacing:".08em"}}>What it does well</span>
              <span style={{fontSize:11,fontFamily:"var(--mono)",color:"var(--t3)",textTransform:"uppercase",letterSpacing:".08em"}}>Where it stops</span>
            </div>
            {[
              {who:"Engine optimizers",sub:"Snowflake, Databricks, etc.",good:"Chooses the best execution plan for the SQL you wrote.",stops:"Can't rewrite your SQL. A correlated subquery stays a correlated subquery."},
              {who:"AI code assistants",sub:"ChatGPT, Copilot",good:"Quick suggestions for one query at a time.",stops:"No validation pipeline. No regression testing. No production deployment path."},
              {who:"Paste-and-optimize tools",sub:"EverSQL, PawSQL",good:"Useful for ad-hoc debugging of individual queries.",stops:"No DAG context, no continuous learning, no CI/CD, no scale."},
              {who:"Your DBA team",sub:"Deep expertise",good:"Understands your business logic and edge cases.",stops:"~20 queries/week capacity. 70% of flagged queries never get touched."},
            ].map((r,i)=>(
              <div key={i} style={{display:"grid",gridTemplateColumns:"180px 1fr 1fr",gap:0,borderBottom:"1px solid var(--brd)",padding:"14px 0"}}>
                <div>
                  <span style={{fontSize:14,fontWeight:600,display:"block",lineHeight:1.2}}>{r.who}</span>
                  <span style={{fontSize:11,color:"var(--t3)"}}>{r.sub}</span>
                </div>
                <p style={{fontSize:13,color:"var(--t2)",lineHeight:1.55,paddingRight:16}}>{r.good}</p>
                <p style={{fontSize:13,color:"var(--t3)",lineHeight:1.55}}>{r.stops}</p>
              </div>
            ))}
          </div>
        </div>

        <div className="rv d1" style={{background:"var(--bg2)",borderRadius:10,border:"1px solid var(--brd)",padding:"20px 24px",maxWidth:640}}>
          <div style={{display:"flex",alignItems:"flex-start",gap:12}}>
            <div style={{width:32,height:32,borderRadius:7,background:"var(--t1)",display:"flex",alignItems:"center",justifyContent:"center",color:"#fff",flexShrink:0,marginTop:2}}><Zap size={16}/></div>
            <div>
              <h4 style={{fontSize:14,fontWeight:600,marginBottom:4}}>QueryTorque fills the remediation gap</h4>
              <p style={{fontSize:13,color:"var(--t2)",lineHeight:1.6}}>
                We don't replace any of the above — we connect to what you already have. Your monitoring finds the expensive queries. Your engine optimizes the plan. QueryTorque rewrites the SQL itself, validates the change, and gives you a deployment path. Think of it as the missing step between detection and resolution.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   5. RESULTS — honest about benchmark vs production
═══════════════════════════════════════ */
function Results(){
  return(
    <section id="results" className="alt">
      <div className="cx">
        <div className="rv" style={{marginBottom:40}}>
          <span className="lb">Results</span>
          <h2 style={{fontSize:"clamp(22px,3vw,30px)",fontWeight:600,letterSpacing:"-.025em",lineHeight:1.15,marginBottom:8}}>
            Benchmark results. Early production data.
          </h2>
          <p style={{fontSize:15,color:"var(--t2)",lineHeight:1.65,maxWidth:540}}>
            We're transparent about what's been validated where. TPC-DS is a reproducible standard benchmark — it's where we proved the approach. Production pilots are where we're proving the economics.
          </p>
        </div>

        {/* benchmark section */}
        <div className="rv" style={{marginBottom:32}}>
          <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:16}}>
            <span className="tg" style={{color:"var(--grn)",borderColor:"var(--grn2)",background:"rgba(22,163,74,.04)"}}>
              <span style={{width:5,height:5,borderRadius:"50%",background:"var(--grn)",display:"inline-block"}}/> TPC-DS benchmark
            </span>
            <span style={{fontSize:12,color:"var(--t3)"}}>99 queries · DuckDB + Snowflake</span>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,200px),1fr))",gap:14}}>
            {[
              {v:"33%",l:"median speedup",d:"Consistent across simple and complex query shapes."},
              {v:"33%",l:"p90 speedup",d:"Same gains at the tail — not just easy wins."},
              {v:"6–10×",l:"top 10 hardest queries",d:"Where nested subqueries and complex joins dominate."},
              {v:"0",l:"regressions",d:"Every rewrite validated for semantic equivalence and runtime."},
            ].map((m,i)=>(
              <div key={i} className="cd" style={{padding:18}}>
                <span style={{fontFamily:"var(--mono)",fontSize:24,fontWeight:500,color:"var(--t1)",lineHeight:1,display:"block"}}>{m.v}</span>
                <span style={{fontSize:13,fontWeight:500,color:"var(--t1)",display:"block",marginTop:4}}>{m.l}</span>
                <p style={{fontSize:12,color:"var(--t3)",lineHeight:1.5,marginTop:4}}>{m.d}</p>
              </div>
            ))}
          </div>
        </div>

        {/* pilot results */}
        <div className="rv d2">
          <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:16}}>
            <span className="tg" style={{color:"var(--blu)",borderColor:"var(--blu2)",background:"rgba(37,99,235,.04)"}}>
              <span style={{width:5,height:5,borderRadius:"50%",background:"var(--blu)",display:"inline-block"}}/> Early pilot results
            </span>
            <span style={{fontSize:12,color:"var(--t3)"}}>Names shared under NDA</span>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,300px),1fr))",gap:14}}>
            {[
              {tag:"Retail · Snowflake",q:"QueryTorque found optimizations on queries our senior engineers had already reviewed. The consistency across 2,400 queries is what convinced us to expand the pilot.",who:"CTO, mid-market retail",metrics:[{v:"85%",l:"avg speedup"},{v:"$1.2M",l:"projected annual savings"}]},
              {tag:"SaaS · BigQuery",q:"We saw a 40% cost reduction in the first week. After one review cycle with zero regressions, we moved to autonomous mode on approved query classes.",who:"VP Data Engineering, B2B SaaS",metrics:[{v:"40%",l:"cost reduction"},{v:"4 mo",l:"zero regressions"}]},
            ].map((c,i)=>(
              <div key={i} className="cd" style={{display:"flex",flexDirection:"column"}}>
                <span className="tg" style={{color:"var(--t2)",borderColor:"var(--brd)",background:"var(--bg2)",marginBottom:10,alignSelf:"flex-start"}}>{c.tag}</span>
                <p style={{fontSize:14,color:"var(--t2)",lineHeight:1.65,flex:1,marginBottom:16}}>"{c.q}"</p>
                <div style={{borderTop:"1px solid var(--brd)",paddingTop:12,marginBottom:10,display:"flex",gap:20}}>
                  {c.metrics.map((m,j)=><div key={j}><span style={{fontFamily:"var(--mono)",fontSize:15,fontWeight:500}}>{m.v}</span><span style={{display:"block",fontSize:11,color:"var(--t3)"}}>{m.l}</span></div>)}
                </div>
                <span style={{fontSize:13,color:"var(--t3)",fontStyle:"italic"}}>{c.who}</span>
              </div>
            ))}
          </div>
        </div>

        {/* capabilities compact */}
        <div className="rv d3" style={{marginTop:40,paddingTop:32,borderTop:"1px solid var(--brd)"}}>
          <span className="lb">Full capability set</span>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,280px),1fr))",gap:12,marginTop:12}}>
            {[
              {icon:<ActI/>,t:"SQL optimization",d:"Snowflake · Databricks · BigQuery · Redshift · PostgreSQL · SQL Server · DuckDB"},
              {icon:<RefI/>,t:"Continuous learning",d:"Every validated optimization feeds the FAISS index. The system improves with use."},
              {icon:<GitB/>,t:"CI/CD integration",d:"Review optimizations like PRs. Approve, auto-merge, or gate on confidence score."},
              {icon:<FileI/>,t:"Audit trail",d:"What changed, why, measured impact, rollback path. Built for FinOps and compliance."},
              {icon:<Shld/>,t:"Quality gates",d:"Configurable promotion thresholds per query, team, or environment. Instant rollback."},
            ].map((c,i)=>(
              <div key={i} style={{display:"flex",alignItems:"flex-start",gap:10,padding:"8px 0"}}>
                <span style={{color:"var(--t3)",marginTop:1,flexShrink:0}}>{c.icon}</span>
                <div>
                  <h4 style={{fontSize:13,fontWeight:600,marginBottom:2}}>{c.t}</h4>
                  <p style={{fontSize:12,color:"var(--t3)",lineHeight:1.5}}>{c.d}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   6. PRICING
═══════════════════════════════════════ */
function Pricing(){
  return(
    <section id="pricing" className="sec">
      <div className="cx">
        <div className="rv" style={{textAlign:"center",maxWidth:440,margin:"0 auto 40px"}}>
          <span className="lb" style={{textAlign:"center"}}>Pricing</span>
          <h2 style={{fontSize:"clamp(20px,2.5vw,28px)",fontWeight:600,letterSpacing:"-.02em",marginBottom:4}}>Start with a flat rate. Scale on proven savings.</h2>
          <p style={{fontSize:14,color:"var(--t2)"}}>14-day trial · no credit card · prove ROI before you commit</p>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,250px),1fr))",gap:14,maxWidth:820,margin:"0 auto"}}>
          {[
            {name:"Starter",price:"$499",per:"/mo",desc:"For teams evaluating the approach.",feat:["10K queries/day","SQL optimization","Reporting dashboard","Email support"],hl:false},
            {name:"Pro",price:"$1,999",per:"/mo",desc:"Full pipeline with autonomous mode.",feat:["100K queries/day","SQL optimization","CI/CD integration","Priority support","Configurable quality gates"],hl:true},
            {name:"Enterprise",price:"Custom",per:"",desc:"Priced on validated savings.",feat:["Unlimited queries","On-prem / VPC deployment","SSO & RBAC","Dedicated CSM","SOC 2 & HIPAA ready"],hl:false},
          ].map((p,i)=>(
            <div key={i} className="rv cd" style={{border:p.hl?"2px solid var(--t1)":"1px solid var(--brd)",display:"flex",flexDirection:"column"}}>
              {p.hl&&<span className="tg" style={{color:"var(--t1)",borderColor:"var(--brd)",background:"var(--bg2)",alignSelf:"flex-start",marginBottom:6}}>Most teams start here</span>}
              <h3 style={{fontSize:16,fontWeight:600,marginBottom:2}}>{p.name}</h3>
              <div style={{marginBottom:4}}><span style={{fontSize:30,fontWeight:700}}>{p.price}</span><span style={{fontSize:13,color:"var(--t3)"}}>{p.per}</span></div>
              <p style={{fontSize:13,color:"var(--t3)",marginBottom:16}}>{p.desc}</p>
              <div style={{display:"flex",flexDirection:"column",gap:7,marginBottom:18,flex:1}}>
                {p.feat.map((f,j)=><div key={j} style={{display:"flex",alignItems:"center",gap:6}}><Chk/><span style={{fontSize:13,color:"var(--t2)"}}>{f}</span></div>)}
              </div>
              <button onClick={()=>go("contact")} className={p.hl?"bp":"bs"} style={{width:"100%",justifyContent:"center"}}>{p.price==="Custom"?"Talk to sales":"Start 14-day trial"}</button>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   7. FAQ — trimmed, honest
═══════════════════════════════════════ */
function FAQ(){
  const[oi,setOi]=useState(null);
  const items=[
    {q:"How does this relate to my engine's built-in optimizer?",a:"They're complementary. Your engine optimizer (Snowflake's query planner, BigQuery's execution engine, etc.) chooses the best execution plan for the SQL you wrote. But it can't restructure your SQL — it can't rewrite a correlated subquery into a window function or reorganize your join logic. QueryTorque rewrites the SQL itself, then your engine optimizes the plan for the rewritten version. Better input SQL → better execution plans."},
    {q:"What's the risk of breaking something?",a:"This is the question we obsess over. Every rewrite goes through four validation stages: syntax checking, semantic equivalence verification (same results on same data), runtime benchmarking against your baseline, and promotion thresholds you configure. If any stage fails, the rewrite is discarded. You can start in monitor-only mode where nothing is applied — just see what QueryTorque would do. Most teams run that way for a week or two before enabling review-and-approve."},
    {q:"TPC-DS is a synthetic benchmark. How does that translate to production?",a:"Fair question. TPC-DS gave us a reproducible standard to validate the approach across 99 query shapes — from simple aggregations through deeply nested subqueries. Real workloads are different: they have your schemas, your data distributions, your access patterns. That's why we run validation against your actual data during pilots. The 30–40% compute reductions we're seeing in production pilots are encouraging, and we're building that evidence base with every new deployment."},
    {q:"How long does setup take?",a:"Most teams run their first scan within a day. Connect your warehouse (read-only access to query history and metadata), configure your thresholds, and review the first batch of recommendations. There's no migration, no schema changes, no code deployment on your side. The typical path: monitor-only for a week, review-and-approve for 2–3 weeks, then selectively go autonomous where you're comfortable."},
    {q:"What's the typical ROI timeline?",a:"In pilots so far, teams have seen measurable compute cost reductions in the first week — typically 30–40% on the queries QueryTorque touches. Whether that translates to five-figure or six-figure annual savings depends on your current spend and how many queries are candidates for optimization. The 14-day trial is specifically designed so you can measure this on your own workload before committing."},
    {q:"Can I review every change before it's deployed?",a:"Yes, and most teams start that way. You can run in monitor-only (no changes applied), review-and-approve (every rewrite goes through your team), or autonomous (confidence-threshold auto-deploy on query classes you've approved). These modes are configurable per query, per team, per environment. We built it this way because we think trust is earned, not assumed."},
  ];
  return(
    <section className="alt">
      <div className="cx" style={{maxWidth:660}}>
        <div className="rv" style={{marginBottom:28}}>
          <span className="lb">FAQ</span>
          <h2 style={{fontSize:"clamp(20px,2.5vw,26px)",fontWeight:600,letterSpacing:"-.02em"}}>Common questions.</h2>
        </div>
        {items.map((item,i)=>(
          <div key={i} className="rv" style={{borderBottom:"1px solid var(--brd)"}}>
            <button onClick={()=>setOi(oi===i?null:i)} style={{width:"100%",display:"flex",alignItems:"center",justifyContent:"space-between",padding:"14px 0",background:"none",border:"none",cursor:"pointer",textAlign:"left",fontFamily:"var(--sans)"}}>
              <span style={{fontSize:14,fontWeight:500,color:"var(--t1)"}}>{item.q}</span>
              <span style={{color:"var(--t3)",transform:oi===i?"rotate(180deg)":"rotate(0)",transition:"transform .15s",flexShrink:0,marginLeft:16}}><ChvD/></span>
            </button>
            {oi===i&&<div style={{paddingBottom:14}}><p style={{fontSize:14,color:"var(--t2)",lineHeight:1.65}}>{item.a}</p></div>}
          </div>
        ))}
        <div className="rv" style={{marginTop:20,textAlign:"center"}}>
          <p style={{fontSize:13,color:"var(--t3)"}}>Have a question we didn't cover? <button onClick={()=>go("contact")} style={{background:"none",border:"none",color:"var(--t1)",fontWeight:500,cursor:"pointer",fontFamily:"var(--sans)",fontSize:13,textDecoration:"underline",textUnderlineOffset:2}}>Reach out directly.</button></p>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   8. CTA + CONTACT — combined final section
═══════════════════════════════════════ */
function Contact(){
  const[f,setF]=useState({name:"",email:"",co:"",msg:""});
  const[sent,setSent]=useState(false);
  const sub=e=>{e.preventDefault();setSent(true);setTimeout(()=>{setSent(false);setF({name:"",email:"",co:"",msg:""})},4000)};
  return(
    <section id="contact" className="sec">
      <div className="cx">
        {/* CTA banner */}
        <div className="rv" style={{background:"var(--t1)",borderRadius:12,padding:"clamp(32px,5vw,48px)",textAlign:"center",marginBottom:56}}>
          <h2 style={{fontSize:"clamp(20px,3vw,28px)",fontWeight:600,color:"#fff",letterSpacing:"-.02em",marginBottom:8}}>See what QueryTorque finds in your warehouse.</h2>
          <p style={{fontSize:15,color:"#94a3b8",maxWidth:460,margin:"0 auto 20px"}}>14-day trial. Read-only access. No credit card. You'll know within a week whether this is worth it.</p>
          <div style={{display:"flex",flexWrap:"wrap",justifyContent:"center",gap:10}}>
            <button onClick={()=>document.getElementById("contact-form")?.scrollIntoView({behavior:"smooth"})} style={{display:"inline-flex",alignItems:"center",gap:8,padding:"10px 20px",fontSize:14,fontWeight:500,color:"var(--t1)",background:"#fff",border:"none",borderRadius:6,cursor:"pointer"}}>Start free trial <ArrowR/></button>
            <button onClick={()=>document.getElementById("contact-form")?.scrollIntoView({behavior:"smooth"})} style={{display:"inline-flex",alignItems:"center",gap:8,padding:"10px 20px",fontSize:14,fontWeight:500,color:"#94a3b8",background:"transparent",border:"1px solid #334155",borderRadius:6,cursor:"pointer"}}>Book a demo</button>
          </div>
        </div>

        {/* contact form */}
        <div id="contact-form" style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,280px),1fr))",gap:40,maxWidth:720,margin:"0 auto"}}>
          <div className="rv">
            <span className="lb">Get started</span>
            <h2 style={{fontSize:"clamp(20px,2.5vw,26px)",fontWeight:600,letterSpacing:"-.02em",marginBottom:8}}>Let's see what you're working with.</h2>
            <p style={{fontSize:14,color:"var(--t2)",lineHeight:1.65,marginBottom:20}}>Demo, pilot, or just questions — we'll get back to you within a day.</p>
            <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12}}>
              <div style={{width:28,height:28,borderRadius:6,background:"var(--bg2)",border:"1px solid var(--brd)",display:"flex",alignItems:"center",justifyContent:"center",color:"var(--t3)"}}><MailI/></div>
              <span style={{fontSize:13}}>hello@dialectlabs.io</span>
            </div>
            <div style={{marginTop:20,padding:"14px 16px",background:"var(--bg2)",borderRadius:8,border:"1px solid var(--brd)"}}>
              <span style={{fontSize:12,fontWeight:600,color:"var(--t1)",display:"block",marginBottom:4}}>Typical pilot timeline</span>
              <div style={{display:"flex",flexDirection:"column",gap:4}}>
                {["Day 1 — Connect warehouse, run first scan","Week 1 — Review recommendations, measure impact","Week 2–3 — Approve optimizations with review gates","Month 1 — Expand coverage, evaluate autonomous mode"].map((s,i)=>(
                  <span key={i} style={{fontSize:12,color:"var(--t3)",fontFamily:"var(--mono)"}}>{s}</span>
                ))}
              </div>
            </div>
          </div>
          <div className="rv d1">
            <div className="cd">
              {sent?(
                <div style={{textAlign:"center",padding:"24px 0"}}>
                  <div style={{width:32,height:32,borderRadius:8,background:"var(--grn2)",display:"flex",alignItems:"center",justifyContent:"center",margin:"0 auto 10px",color:"var(--grn)"}}><Chk size={18}/></div>
                  <h4 style={{fontSize:15,fontWeight:600,marginBottom:2}}>Sent!</h4>
                  <p style={{fontSize:13,color:"var(--t3)"}}>We'll be in touch within 24 hours.</p>
                </div>
              ):(
                <form onSubmit={sub} style={{display:"flex",flexDirection:"column",gap:10}}>
                  <input value={f.name} onChange={e=>setF({...f,name:e.target.value})} required className="inf" placeholder="Name"/>
                  <input type="email" value={f.email} onChange={e=>setF({...f,email:e.target.value})} required className="inf" placeholder="Work email"/>
                  <input value={f.co} onChange={e=>setF({...f,co:e.target.value})} className="inf" placeholder="Company"/>
                  <textarea value={f.msg} onChange={e=>setF({...f,msg:e.target.value})} className="inf" placeholder="Tell us about your data stack and what you're looking to improve..." rows={3} style={{resize:"vertical",minHeight:70}}/>
                  <button type="submit" className="bp" style={{width:"100%",justifyContent:"center",marginTop:2}}><SendI/> Send</button>
                </form>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════
   FOOTER
═══════════════════════════════════════ */
function Footer(){
  return(
    <footer style={{borderTop:"1px solid var(--brd)",padding:"28px 0"}}>
      <div className="cx" style={{display:"flex",flexWrap:"wrap",justifyContent:"space-between",alignItems:"center",gap:10}}>
        <div style={{display:"flex",alignItems:"center",gap:6}}>
          <span style={{fontSize:13,fontWeight:600}}>QueryTorque</span>
          <span style={{fontSize:12,color:"var(--t3)"}}>· built by <span style={{fontFamily:"var(--mono)",letterSpacing:".04em"}}>Dialect Labs</span></span>
        </div>
        <div style={{display:"flex",gap:16}}>
          {["Privacy","Terms"].map(l=><span key={l} style={{fontSize:12,color:"var(--t3)",cursor:"pointer"}}>{l}</span>)}
          <span style={{fontSize:12,color:"var(--t3)"}}>© 2025</span>
        </div>
      </div>
    </footer>
  );
}

/* ═══════════════════════════════════════
   APP — 8 sections
═══════════════════════════════════════ */
export default function App(){
  useReveal();
  return(
    <>
      <style>{css}</style>
      <div className="qt">
        <Nav/>
        <main>
          <Hero/>
          <Pain/>
          <How/>
          <Compare/>
          <Results/>
          <Pricing/>
          <FAQ/>
          <Contact/>
        </main>
        <Footer/>
      </div>
    </>
  );
}
