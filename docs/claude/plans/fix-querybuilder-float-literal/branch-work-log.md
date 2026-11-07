# fix-querybuilder-float-literal work log

## 2026-11-07

- Found by the DataFusion SQL PoC's QueryBuilder benchmarks: `q[q.d == 0.05]` on a float64 column returned
  0 rows, and native TPC-H Q6 under-counted (8.58M against 11.80M).
- Root cause: Python floats reached the overloaded `_Value` binding, where `construct_value<float>`
  preceded `<double>`, so literals were stored as float32.
- Fix: Python floats become FLOAT64 values flagged as weakly typed (NumPy NEP 50). They narrow to FLOAT32
  only against FLOAT32 operands, in comparators, arithmetic, ternary, column-stats pruning and output-schema
  inference.
- Tests: Python filtering and projection tests compared against pandas; C++ unit tests for dispatch, stats
  and output schema.
