"""Logger expert review: series, QC, removal rules, validation and SQL for the /logger-review tool.

Vendored from the empa-logger-review prototype and adapted to the checker image
(pandas 1.5, SQLAlchemy 1.4, no PyYAML). qc_harness.py is the calibration harness from
database-admin/db/empa/logger-threshold-calibration/harness, with one change: it reads
thresholds_main.json (the same content as thresholds_main.yaml) so PyYAML is not needed.
Keep static/logger_review/qc_tests.js in step with qc_harness.py.
"""
