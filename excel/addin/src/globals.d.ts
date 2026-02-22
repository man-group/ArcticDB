// Type declarations for Office.js globals loaded via the CDN script tag.
// The @microsoft/office-js package provides types at runtime via the hosted script.

declare namespace CustomFunctions {
  function associate(name: string, fn: Function): void;
}

declare namespace Office {
  function onReady(callback: () => void): void;
  namespace actions {
    function associate(name: string, fn: Function): void;
  }
  namespace AddinCommands {
    interface Event {
      completed(): void;
    }
  }
}

declare namespace Excel {
  enum CalculationType {
    recalculate = "Recalculate",
    full = "Full",
    fullRebuild = "FullRebuild",
  }

  function run(
    callback: (context: RequestContext) => Promise<void>
  ): Promise<void>;

  interface RequestContext {
    workbook: Workbook;
    sync(): Promise<void>;
  }

  interface Workbook {
    worksheets: WorksheetCollection;
    application: Application;
    getSelectedRange(): Range;
  }

  interface Application {
    calculate(type: CalculationType): void;
  }

  interface WorksheetCollection {
    getActiveWorksheet(): Worksheet;
  }

  interface Worksheet {
    getRangeByIndexes(
      row: number,
      col: number,
      rowCount: number,
      colCount: number
    ): Range;
    getRange(address?: string): Range;
  }

  interface Range {
    values: any[][];
    address: string;
    rowIndex: number;
    columnIndex: number;
    load(properties: string | string[]): void;
  }
}
