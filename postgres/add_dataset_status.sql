-- Add status column to datasets table to track materialization job status
ALTER TABLE app.datasets 
ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'NOT_STARTED';

-- Add index for performance
CREATE INDEX IF NOT EXISTS idx_datasets_status ON app.datasets(status);

-- Initialize all existing records to NOT_STARTED
-- The actual status will be updated when records are loaded based on run_id
UPDATE app.datasets 
SET status = 'NOT_STARTED'
WHERE status IS NULL;

-- Add comment for documentation
COMMENT ON COLUMN app.datasets.status IS 'Status of materialization job: NOT_STARTED, PENDING, RUNNING, SUCCESS, FAILED, TERMINATED, SKIPPED';