/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

export {}; // Make this file a module to avoid global scope conflicts

let gatewayUrl = "http://localhost:8787";
let activeLibrary = "";

// ── Helpers ──────────────────────────────────────────────────────────────────

function $(id: string): HTMLElement {
  return document.getElementById(id)!;
}

function setStatus(id: string, msg: string, cls: "ok" | "error" | "" = "") {
  const el = $(id);
  el.textContent = msg;
  el.className = cls;
}

async function apiFetch(path: string, opts?: RequestInit): Promise<any> {
  const resp = await fetch(`${gatewayUrl}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text}`);
  }
  return resp.json();
}

function syncConfigToFunctions() {
  // Update the custom functions' config via the shared runtime global
  const setConfig = (globalThis as any).arcticdbSetConfig;
  if (typeof setConfig === "function") {
    setConfig(gatewayUrl, activeLibrary);
  }
}

// ── UI actions ───────────────────────────────────────────────────────────────

async function testConnection() {
  setStatus("status", "Connecting...");
  gatewayUrl = ($(
    "serverUrl"
  ) as HTMLInputElement).value.replace(/\/+$/, "");
  try {
    await apiFetch("/health");
    setStatus("status", "Connected", "ok");
  } catch (e: any) {
    setStatus("status", e.message, "error");
  }
}

async function openLibrary() {
  const name = ($(
    "libName"
  ) as HTMLInputElement).value.trim();
  const path = ($(
    "libPath"
  ) as HTMLInputElement).value.trim();
  if (!name || !path) {
    setStatus("libStatus", "Name and path are required", "error");
    return;
  }

  try {
    await apiFetch("/api/libraries", {
      method: "POST",
      body: JSON.stringify({ name, path }),
    });
    activeLibrary = name;
    syncConfigToFunctions();
    setStatus("libStatus", `Library "${name}" opened`, "ok");
    ($("btnClose") as HTMLButtonElement).disabled = false;
    ($("btnRefresh") as HTMLButtonElement).disabled = false;
    ($("btnWriteTest") as HTMLButtonElement).disabled = false;
    await refreshSymbols();
  } catch (e: any) {
    setStatus("libStatus", e.message, "error");
  }
}

async function closeLibrary() {
  if (!activeLibrary) return;
  try {
    await apiFetch(`/api/libraries/${encodeURIComponent(activeLibrary)}`, {
      method: "DELETE",
    });
    setStatus("libStatus", `Library "${activeLibrary}" closed`, "");
    activeLibrary = "";
    syncConfigToFunctions();
    ($("btnClose") as HTMLButtonElement).disabled = true;
    ($("btnRefresh") as HTMLButtonElement).disabled = true;
    ($("btnWriteTest") as HTMLButtonElement).disabled = true;
    $("symbolList").innerHTML = '<li class="empty">No library open</li>';
  } catch (e: any) {
    setStatus("libStatus", e.message, "error");
  }
}

async function refreshSymbols() {
  if (!activeLibrary) return;
  const list = $("symbolList");
  list.innerHTML = '<li class="empty">Loading...</li>';

  try {
    const symbols: string[] = await apiFetch(
      `/api/libraries/${encodeURIComponent(activeLibrary)}/symbols`
    );

    if (symbols.length === 0) {
      list.innerHTML = '<li class="empty">No symbols</li>';
      return;
    }

    list.innerHTML = "";
    for (const sym of symbols) {
      const li = document.createElement("li");
      const span = document.createElement("span");
      span.textContent = sym;
      const btn = document.createElement("button");
      btn.textContent = "Load";
      btn.addEventListener("click", () => loadSymbol(sym));
      li.appendChild(span);
      li.appendChild(btn);
      list.appendChild(li);
    }
  } catch (e: any) {
    list.innerHTML = `<li class="empty" style="color:#d13438">${e.message}</li>`;
  }
}

async function loadSymbol(symbol: string) {
  if (!activeLibrary) return;

  try {
    const df = await apiFetch(
      `/api/libraries/${encodeURIComponent(activeLibrary)}/read/${encodeURIComponent(symbol)}`
    );

    // Build 2D array: header row + data rows
    const values: (string | number)[][] = [df.column_names];
    for (const row of df.data) {
      values.push(row);
    }

    // Write to Excel at the current cursor position
    await Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getActiveWorksheet();
      const startCell = context.workbook.getSelectedRange();
      startCell.load("address");
      await context.sync();

      const range = sheet.getRangeByIndexes(
        0, 0,
        values.length,
        values[0].length
      );

      // Recompute range from the selected cell
      const selected = context.workbook.getSelectedRange();
      selected.load(["rowIndex", "columnIndex"]);
      await context.sync();

      const target = sheet.getRangeByIndexes(
        selected.rowIndex,
        selected.columnIndex,
        values.length,
        values[0].length
      );
      target.values = values;
      await context.sync();
    });

    setStatus("libStatus", `Loaded "${symbol}" (${df.num_rows} rows)`, "ok");
  } catch (e: any) {
    setStatus("libStatus", `Error loading "${symbol}": ${e.message}`, "error");
  }
}

async function writeTestData() {
  if (!activeLibrary) return;
  const symbol = ($(
    "testSymbol"
  ) as HTMLInputElement).value.trim();
  const rows = parseInt(
    ($(
      "testRows"
    ) as HTMLInputElement).value,
    10
  );
  const cols = parseInt(
    ($(
      "testCols"
    ) as HTMLInputElement).value,
    10
  );

  if (!symbol) {
    setStatus("libStatus", "Symbol name is required", "error");
    return;
  }

  try {
    await apiFetch(
      `/api/libraries/${encodeURIComponent(activeLibrary)}/write-test`,
      {
        method: "POST",
        body: JSON.stringify({ symbol, rows, cols }),
      }
    );
    setStatus("libStatus", `Wrote test data: "${symbol}" (${rows}x${cols})`, "ok");
    await refreshSymbols();
  } catch (e: any) {
    setStatus("libStatus", e.message, "error");
  }
}

// ── Initialization ───────────────────────────────────────────────────────────

Office.onReady(() => {
  $("btnHealth").addEventListener("click", testConnection);
  $("btnOpen").addEventListener("click", openLibrary);
  $("btnClose").addEventListener("click", closeLibrary);
  $("btnRefresh").addEventListener("click", refreshSymbols);
  $("btnWriteTest").addEventListener("click", writeTestData);

  // Restore config from custom functions runtime if available
  const getConfig = (globalThis as any).arcticdbGetConfig;
  if (typeof getConfig === "function") {
    const cfg = getConfig();
    if (cfg.url) {
      gatewayUrl = cfg.url;
      ($(
        "serverUrl"
      ) as HTMLInputElement).value = cfg.url;
    }
    if (cfg.library) {
      activeLibrary = cfg.library;
      ($(
        "libName"
      ) as HTMLInputElement).value = cfg.library;
    }
  }
});
