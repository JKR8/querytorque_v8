import { test, expect } from "@playwright/test";

// This test runs against the LIVE fleet server on port 8765
// Launch first: qt run duckdb_tpcds -q query_1 --mode fleet --live

test.use({ baseURL: "http://127.0.0.1:8765" });
test.setTimeout(120_000); // 2 min — real LLM calls can take 60s+

test("debug editor strike end-to-end", async ({ page }) => {
  // Capture all console messages
  const consoleLogs: string[] = [];
  page.on("console", (msg) => {
    consoleLogs.push(`[${msg.type()}] ${msg.text()}`);
  });

  // Capture WebSocket frames
  const wsFrames: { direction: string; data: string }[] = [];
  page.on("websocket", (ws) => {
    console.log(`WS opened: ${ws.url()}`);
    ws.on("framesent", (frame) => {
      wsFrames.push({ direction: "SENT", data: frame.payload.toString() });
    });
    ws.on("framereceived", (frame) => {
      wsFrames.push({ direction: "RECV", data: frame.payload.toString() });
    });
    ws.on("close", () => {
      console.log("WS closed");
    });
  });

  // 1. Load page
  await page.goto("/");
  await page.waitForTimeout(2000);

  // Check IS_DEMO and wsConnected
  const isDemoVal = await page.evaluate("IS_DEMO");
  const wsConnectedVal = await page.evaluate("wsConnected");
  const wsReadyState = await page.evaluate(
    "ws ? ws.readyState : 'ws is null'"
  );
  const wsReadyFn = await page.evaluate(
    "typeof isWsReady === 'function' ? isWsReady() : 'no isWsReady fn'"
  );

  console.log("\n=== STATE CHECK ===");
  console.log(`IS_DEMO: ${isDemoVal}`);
  console.log(`wsConnected: ${wsConnectedVal}`);
  console.log(`ws.readyState: ${wsReadyState} (1=OPEN)`);
  console.log(`isWsReady(): ${wsReadyFn}`);

  // Check QUERIES data
  const queriesCount = await page.evaluate("QUERIES.length");
  const firstQueryId = await page.evaluate("QUERIES[0] ? QUERIES[0].id : null");
  console.log(`QUERIES.length: ${queriesCount}`);
  console.log(`QUERIES[0].id: ${firstQueryId}`);

  // 2. Switch to Editor tab
  await page.click('[data-tab="editor"]');
  await page.waitForTimeout(500);

  // 3. Select query from dropdown
  const options = await page.evaluate(() => {
    const sel = document.getElementById(
      "editor-query-select"
    ) as HTMLSelectElement;
    return Array.from(sel.options).map((o) => ({
      value: o.value,
      text: o.textContent,
    }));
  });
  console.log(`\n=== EDITOR DROPDOWN ===`);
  console.log(`Options: ${JSON.stringify(options)}`);

  // Select first real query (skip the placeholder)
  const queryOption = options.find((o) => o.value && o.value !== "");
  if (!queryOption) {
    console.log("ERROR: No query options available in dropdown!");
    console.log("\n=== CONSOLE LOGS ===");
    consoleLogs.forEach((l) => console.log(l));
    expect(queryOption).toBeTruthy();
    return;
  }

  await page.selectOption("#editor-query-select", queryOption.value);
  await page.waitForTimeout(500);

  // Check editor state
  const editorQueryId = await page.evaluate("editorQueryId");
  const btnStrikeDisabled = await page.evaluate(
    'document.getElementById("btn-strike").disabled'
  );
  const btnBeamDisabled = await page.evaluate(
    'document.getElementById("btn-beam").disabled'
  );
  const originalSql = await page.evaluate(
    'document.getElementById("editor-original").value.substring(0, 80)'
  );

  console.log(`\n=== EDITOR STATE ===`);
  console.log(`editorQueryId: ${editorQueryId}`);
  console.log(`btn-strike disabled: ${btnStrikeDisabled}`);
  console.log(`btn-beam disabled: ${btnBeamDisabled}`);
  console.log(`original SQL (first 80): ${originalSql}`);

  // Re-check WS state right before clicking
  const wsReadyBefore = await page.evaluate(
    "typeof isWsReady === 'function' ? isWsReady() : (wsConnected && ws)"
  );
  console.log(`isWsReady() before click: ${wsReadyBefore}`);

  // 4. Click Fire Strike
  console.log(`\n=== CLICKING FIRE STRIKE ===`);
  wsFrames.length = 0; // reset

  await page.click("#btn-strike");

  // Wait for response (real LLM call could take 30-60s, but error would be fast)
  await page.waitForTimeout(5000);

  // 5. Capture state after click
  const editorFiring = await page.evaluate("editorFiring");
  const spinnerActive = await page.evaluate(
    'document.getElementById("editor-spinner").classList.contains("active")'
  );
  const spinnerText = await page.evaluate(
    'document.getElementById("editor-spinner-text").textContent'
  );
  const editorResultsCount = await page.evaluate("editorResults.length");

  console.log(`\n=== POST-CLICK STATE ===`);
  console.log(`editorFiring: ${editorFiring}`);
  console.log(`spinner active: ${spinnerActive}`);
  console.log(`spinner text: ${spinnerText}`);
  console.log(`editorResults.length: ${editorResultsCount}`);

  // 6. Dump WS frames
  console.log(`\n=== WEBSOCKET FRAMES ===`);
  if (wsFrames.length === 0) {
    console.log("NO WS FRAMES — strike did not send via WebSocket!");
  }
  for (const f of wsFrames) {
    const preview =
      f.data.length > 200 ? f.data.substring(0, 200) + "..." : f.data;
    console.log(`${f.direction}: ${preview}`);
  }

  // 7. Dump relevant console logs
  console.log(`\n=== CONSOLE LOGS ===`);
  const relevant = consoleLogs.filter(
    (l) =>
      l.includes("Fleet") ||
      l.includes("WebSocket") ||
      l.includes("editor") ||
      l.includes("strike") ||
      l.includes("Error") ||
      l.includes("error")
  );
  for (const l of relevant) {
    console.log(l);
  }

  // If there were WS frames sent, wait longer for the LLM response
  const sentStrike = wsFrames.some(
    (f) => f.direction === "SENT" && f.data.includes("editor_strike")
  );
  if (sentStrike) {
    console.log("\n=== STRIKE SENT — waiting 60s for LLM response... ===");
    // Wait up to 60s, checking every 5s
    for (let i = 0; i < 12; i++) {
      await page.waitForTimeout(5000);
      const firing = await page.evaluate("editorFiring");
      const results = await page.evaluate("editorResults.length");
      const rewrite = await page.evaluate(
        'document.getElementById("editor-rewrite").value.substring(0, 80)'
      );
      console.log(
        `  +${(i + 1) * 5}s: firing=${firing}, results=${results}, rewrite="${rewrite}"`
      );
      if (!firing) {
        console.log("  Strike completed!");
        break;
      }
    }

    // Final WS frames
    console.log(`\n=== ALL WS FRAMES (final) ===`);
    for (const f of wsFrames) {
      const preview =
        f.data.length > 300 ? f.data.substring(0, 300) + "..." : f.data;
      console.log(`${f.direction}: ${preview}`);
    }
  }

  // Assert something was sent
  expect(sentStrike).toBe(true);
});
