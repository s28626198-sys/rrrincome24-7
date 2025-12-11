-- Migration: Add number_count column to user_sessions table
-- Run this SQL in Supabase SQL Editor if you already have the user_sessions table

-- Add number_count column with default value of 2
ALTER TABLE user_sessions 
ADD COLUMN IF NOT EXISTS number_count INTEGER DEFAULT 2;

-- Update existing rows to have number_count = 2 if NULL
UPDATE user_sessions 
SET number_count = 2 
WHERE number_count IS NULL;

