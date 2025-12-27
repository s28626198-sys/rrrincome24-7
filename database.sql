-- Supabase Database Setup for Telegram Bot
-- Run this SQL in Supabase SQL Editor

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP WITH TIME ZONE
);

-- User sessions table (for active OTP monitoring and preferences)
CREATE TABLE IF NOT EXISTS user_sessions (
    user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    selected_service TEXT,
    selected_country TEXT,
    range_id TEXT,
    number TEXT,
    monitoring INTEGER DEFAULT 0,
    number_count INTEGER DEFAULT 2,
    otp_count INTEGER DEFAULT 0,
    otp_date TEXT,
    last_check TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Used numbers table (to prevent reuse within 24 hours)
CREATE TABLE IF NOT EXISTS used_numbers (
    number TEXT PRIMARY KEY,
    used_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
CREATE INDEX IF NOT EXISTS idx_user_sessions_monitoring ON user_sessions(monitoring);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_used_numbers_used_at ON used_numbers(used_at);
