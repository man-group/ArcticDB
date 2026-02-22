/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/**
 * ArcticDB custom functions for Excel.
 *
 * These functions call the ArcticDB gateway server and return data as
 * dynamic arrays that spill into adjacent cells.
 */

export {}; // Make this file a module to avoid global scope conflicts

// Gateway server URL — updated via the task pane settings
let gatewayUrl = "http://localhost:8787";
let activeLibrary = "";

/** Called by the task pane to configure the gateway connection. */
(globalThis as any).arcticdbSetConfig = (url: string, library: string) => {
  gatewayUrl = url.replace(/\/+$/, "");
  activeLibrary = library;
};

/** Called by the task pane to get current config. */
(globalThis as any).arcticdbGetConfig = (): { url: string; library: string } => {
  return { url: gatewayUrl, library: activeLibrary };
};

interface DataFrameResponse {
  column_names: string[];
  column_types: string[];
  data: (number | string | null)[][];
  num_rows: number;
}

/**
 * Reads a symbol from ArcticDB and returns it as a spilling 2D array.
 * @customfunction
 * @param symbol The symbol name to read
 * @param [version] Version number (-1 or omit for latest)
 * @returns 2D array with headers in the first row
 */
async function read(
  symbol: string,
  version?: number
): Promise<(string | number | null)[][]> {
  if (!activeLibrary) {
    return [["Error: no library selected. Use the ArcticDB task pane to connect."]];
  }

  const v = version !== undefined && version !== null ? version : -1;
  const url = `${gatewayUrl}/api/libraries/${encodeURIComponent(activeLibrary)}/read/${encodeURIComponent(symbol)}?version=${v}`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const body = await resp.text();
      return [[`Error ${resp.status}: ${body}`]];
    }

    const df: DataFrameResponse = await resp.json();

    // Build 2D array: header row + data rows
    const result: (string | number | null)[][] = [df.column_names];
    for (const row of df.data) {
      result.push(row);
    }
    return result;
  } catch (e: any) {
    return [[`Error: ${e.message}`]];
  }
}

/**
 * Lists all symbols in the active ArcticDB library.
 * @customfunction
 * @returns Spilling list of symbol names
 */
async function list(): Promise<string[][]> {
  if (!activeLibrary) {
    return [["Error: no library selected. Use the ArcticDB task pane to connect."]];
  }

  const url = `${gatewayUrl}/api/libraries/${encodeURIComponent(activeLibrary)}/symbols`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const body = await resp.text();
      return [[`Error ${resp.status}: ${body}`]];
    }

    const symbols: string[] = await resp.json();
    return symbols.map((s) => [s]);
  } catch (e: any) {
    return [[`Error: ${e.message}`]];
  }
}

// Register custom functions with the Office.js runtime
CustomFunctions.associate("READ", read);
CustomFunctions.associate("LIST", list);
