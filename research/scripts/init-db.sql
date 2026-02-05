-- QueryTorque Database Initialization Script
-- This script runs when the PostgreSQL container is first created

-- Ensure UUID extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create additional databases for testing (optional)
-- CREATE DATABASE querytorque_test;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE querytorque TO querytorque;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'QueryTorque database initialized successfully';
END $$;
