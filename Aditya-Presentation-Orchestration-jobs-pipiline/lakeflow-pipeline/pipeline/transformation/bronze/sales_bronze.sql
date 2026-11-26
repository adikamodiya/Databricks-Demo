-- ==============================================================
-- BRONZE LAYER - Sales ingestion (Auto Loader via read_files)
-- Source : CSV files in UC volume /Volumes/demo2/bronze/data_files1
-- Target : demo2.bronze.sales (streaming table)
--
-- Raw, as-is ingestion:
--   * Schema inference + schema evolution (addNewColumns)
--   * Malformed / unexpected data is RESCUED into _rescued_data,
--     never silently discarded (warn-only quality check on order_id)
--   * Ingestion metadata: ingestion_timestamp, source_file_name,
--     source_file_path
-- ==============================================================

CREATE OR REFRESH STREAMING TABLE demo2.bronze.sales (
  CONSTRAINT expect_order_id EXPECT (order_id IS NOT NULL)
)
COMMENT 'Bronze: raw sales CSVs ingested with Auto Loader from /Volumes/demo2/bronze/data_files1 (schema evolution + rescued data)'
AS SELECT
  *,
  current_timestamp()        AS ingestion_timestamp,
  _metadata.file_name       AS source_file_name,
  _metadata.file_path       AS source_file_path
FROM STREAM(read_files(
  '/Volumes/demo2/bronze/data_files1',
  format               => 'csv',
  header               => 'true',
  schemaEvolutionMode  => 'addNewColumns',
  rescuedDataColumn    => '_rescued_data',
  pathGlobFilter       => '*.csv'
));
