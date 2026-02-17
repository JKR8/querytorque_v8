import { test, expect, type Page } from "@playwright/test";
import { execSync, spawn, type ChildProcess } from "child_process";

let server: ChildProcess;

// Start the test server before all tests, stop after
test.beforeAll(async () => {
  server = spawn("python3", ["e2e/serve.py"], {
    cwd: process.cwd(),
    stdio: ["ignore", "pipe", "pipe"],
  });

  // Wait for server to be ready
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Server start timeout")),
      15_000
    );

    server.stdout?.on("data", (chunk: Buffer) => {
      if (chunk.toString().includes("E2E test server")) {
        // Give uvicorn a moment to bind
        setTimeout(() => {
          clearTimeout(timeout);
          resolve();
        }, 500);
      }
    });

    server.stderr?.on("data", (chunk: Buffer) => {
      const msg = chunk.toString();
      // Uvicorn logs to stderr; only reject on real errors
      if (msg.includes("Error") && !msg.includes("DeprecationWarning")) {
        clearTimeout(timeout);
        reject(new Error(`Server error: ${msg}`));
      }
    });

    server.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
});

test.afterAll(async () => {
  if (server && !server.killed) {
    server.kill("SIGTERM");
    // Wait for exit
    await new Promise<void>((resolve) => {
      server.on("exit", resolve);
      setTimeout(resolve, 3_000);
    });
  }
});

// ─── Dashboard Tests ─────────────────────────────────────────

test.describe("Dashboard", () => {
  test("loads and shows header", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".header")).toBeVisible();
    await expect(page.locator(".logo-text")).toHaveText("QueryTorque");
    await expect(page.locator(".header-title")).toHaveText(
      "Fleet Command Centre"
    );
  });

  test("shows fixture queries in triage table", async ({ page }) => {
    await page.goto("/");
    // Wait for JS to render
    await page.waitForTimeout(500);
    // Should see query_1 and query_42 from fixture data
    const body = await page.textContent("body");
    expect(body).toContain("query_1");
  });

  test("has help link in header", async ({ page }) => {
    await page.goto("/");
    const helpLink = page.locator('a[href="/help"]');
    await expect(helpLink).toBeVisible();
    await expect(helpLink).toHaveText("?");
    await expect(helpLink).toHaveAttribute("target", "_blank");
  });

  test("has four tabs", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".tab-btn")).toHaveCount(4);
    await expect(page.locator('.tab-btn[data-tab="done"]')).toContainText("Results");
  });

  test("keeps estimated savings stable when dispatch starts", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(500);

    const triageSavings = (await page.locator("#triage-savings").textContent())?.trim();
    await page.locator("#btn-approve").click();

    await expect(page.locator("#view-execution")).toHaveClass(/active/);
    await expect(page.locator("#exec-savings")).toHaveText(triageSavings || "");
  });

  test("execution view shows backend console activity", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(500);
    await page.locator("#btn-approve").click();

    await expect(page.locator("#view-execution")).toHaveClass(/active/);
    await expect(page.locator("#backend-console")).toBeVisible();
    await expect(page.locator("#backend-console")).toContainText(/Run start requested|Sending approve/i);
    await expect(page.locator("#backend-console-badge")).toHaveText(/live|connecting/i);
  });

  test("config modal exposes blackboard history toggle", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(250);

    await page.locator(".header-config-btn").click();
    const toggle = page.locator("#cfg-use-blackboard");
    await expect(toggle).toBeVisible();
    await expect(toggle).toBeChecked();

    await toggle.uncheck();
    await page.locator('button:has-text("Save Settings")').click();
    await expect(page.locator("#config-summary")).toContainText("history:off");
  });

  test("editor shows live step feedback when optimization is started", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(500);

    await page.locator('.tab-btn[data-tab="editor"]').click();
    const firstQuery = await page.locator("#editor-query-select option").nth(1).getAttribute("value");
    expect(firstQuery).toBeTruthy();
    await page.selectOption("#editor-query-select", firstQuery!);

    await page.locator("#btn-beam").click();
    await expect(page.locator("#editor-spinner")).toHaveClass(/active/);
    await expect(page.locator("#editor-results-content")).toContainText(/Beam requested|request dispatched|Request accepted/i);

    // The e2e fixture server has no benchmark_dir; backend reports an error, which
    // still validates that step-level feedback and completion state are surfaced.
    await expect(page.locator("#editor-results-content")).toContainText(/Optimization failed|Error:/i, {
      timeout: 20_000,
    });
    await expect(page.locator("#editor-spinner")).not.toHaveClass(/active/, { timeout: 20_000 });
  });
});

// ─── Help Page Tests ─────────────────────────────────────────

test.describe("Help Page", () => {
  test("loads at /help", async ({ page }) => {
    await page.goto("/help");
    await expect(page).toHaveTitle("QueryTorque — Fleet C2 Help");
  });

  test("has header with logo and back link", async ({ page }) => {
    await page.goto("/help");
    await expect(page.locator(".logo-text")).toHaveText("QueryTorque");
    await expect(page.locator(".header-title")).toHaveText("Help & Glossary");

    const backLink = page.locator(".back-link");
    await expect(backLink).toBeVisible();
    await expect(backLink).toHaveAttribute("href", "/");
  });

  test("has table of contents with all sections", async ({ page }) => {
    await page.goto("/help");
    const toc = page.locator(".toc");
    await expect(toc).toBeVisible();

    const links = toc.locator("a");
    await expect(links).toHaveCount(6);
    await expect(links.nth(0)).toContainText("Getting Started");
    await expect(links.nth(1)).toContainText("Four Tabs");
    await expect(links.nth(2)).toContainText("Glossary");
    await expect(links.nth(3)).toContainText("Configuration");
    await expect(links.nth(4)).toContainText("Folder Structure");
    await expect(links.nth(5)).toContainText("Output");
  });

  test("getting started section has launch command", async ({ page }) => {
    await page.goto("/help");
    const section = page.locator("#getting-started");
    await expect(section).toBeVisible();

    const body = await page.textContent("body");
    expect(body).toContain("qt run");
    expect(body).toContain("--mode fleet --live");
  });

  test("four tabs section describes all tabs", async ({ page }) => {
    await page.goto("/help");
    const badges = page.locator(".tab-badge");
    // TRIAGE, EXECUTION, DONE, EDITOR
    await expect(badges).toHaveCount(4);
  });

  test("glossary has tables with key terms", async ({ page }) => {
    await page.goto("/help");
    const tables = page.locator(".glossary-table");
    // Buckets, Verdicts, Triage Metrics, Transforms, Execution
    await expect(tables).toHaveCount(5);

    // Check key terms are present
    const body = await page.textContent("body");
    const terms = [
      "Speedup",
      "Bucket",
      "Overlap",
      "Q-error",
      "Transform",
      "Race",
      "Beam",
      "Strike",
      "Snipe",
      "Semantic Validation",
      "Equivalence Check",
    ];
    for (const term of terms) {
      expect(body).toContain(term);
    }
  });

  test("status badges render correctly", async ({ page }) => {
    await page.goto("/help");
    await expect(page.locator(".status-badge.win")).toBeVisible();
    await expect(page.locator(".status-badge.improved")).toBeVisible();
    await expect(page.locator(".status-badge.neutral")).toBeVisible();
    await expect(page.locator(".status-badge.regression")).toBeVisible();
    await expect(page.locator(".status-badge.error")).toBeVisible();
  });

  test("config reference has tables", async ({ page }) => {
    await page.goto("/help");
    const tables = page.locator(".config-table");
    // Database, Benchmark, Validation, Optimization
    await expect(tables).toHaveCount(4);

    const body = await page.textContent("body");
    expect(body).toContain("semantic_validation_enabled");
    expect(body).toContain("race_min_runtime_ms");
    expect(body).toContain("timeout_seconds");
  });

  test("folder structure section has code block", async ({ page }) => {
    await page.goto("/help");
    const section = page.locator("#folder-structure");
    await expect(section).toBeVisible();

    const body = await page.textContent("body");
    expect(body).toContain("config.json");
    expect(body).toContain("beam_sessions/");
    expect(body).toContain("queries/");
  });

  test("TOC links scroll to sections", async ({ page }) => {
    await page.goto("/help");
    // Click glossary link
    await page.locator('.toc a[href="#glossary"]').click();
    // The glossary heading should be near the top of the viewport
    const heading = page.locator("#glossary");
    await expect(heading).toBeInViewport();
  });

  test("back link navigates to dashboard", async ({ page }) => {
    await page.goto("/help");
    // Click back link (same tab)
    await page.locator(".back-link").click();
    await expect(page).toHaveURL(/\/$/);
    await expect(page.locator(".header-title")).toHaveText(
      "Fleet Command Centre"
    );
  });
});

// ─── Navigation Integration ──────────────────────────────────

test.describe("Navigation", () => {
  test("dashboard help link opens help page", async ({ page, context }) => {
    await page.goto("/");

    // The help link opens in a new tab, so listen for it
    const [helpPage] = await Promise.all([
      context.waitForEvent("page"),
      page.locator('a[href="/help"]').click(),
    ]);

    await helpPage.waitForLoadState();
    await expect(helpPage).toHaveTitle("QueryTorque — Fleet C2 Help");
    await expect(helpPage.locator("h1")).toHaveText("Fleet C2 Help");
  });
});
