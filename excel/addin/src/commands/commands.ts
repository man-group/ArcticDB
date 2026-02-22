/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/**
 * Ribbon command: Refresh data.
 * Re-calculates all ARCTICDB custom functions in the workbook.
 */
async function refreshData(event: Office.AddinCommands.Event) {
  try {
    await Excel.run(async (context) => {
      // Force recalculation of the active workbook, which re-triggers custom functions
      context.workbook.application.calculate(Excel.CalculationType.full);
      await context.sync();
    });
  } catch (_e) {
    // Silently ignore — the user can retry
  }
  event.completed();
}

// Register ribbon command handlers
Office.actions.associate("refreshData", refreshData);
