#!/bin/bash
set -e

echo "🔧 Starting backend entrypoint..."

# Set default environment variables (these will be overridden by Cloud Run)
export AWS_REGION=${AWS_REGION:-"us-west-2"}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-""}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-""}
export AWS_BUCKET_NAME=${AWS_BUCKET_NAME:-"skoopin-mercato-prd-us-west-2"}
export AWS_AI_BUCKET_NAME=${AWS_AI_BUCKET_NAME:-"metafoodx-ai-bucket"}
export AWS_ENV=${AWS_ENV:-"prd"}

export DYNAMODB_AUDIT_SESSION_TABLE=${DYNAMODB_AUDIT_SESSION_TABLE:-"AuditSession"}
export DYNAMODB_SCAN_AUDIT_TABLE=${DYNAMODB_SCAN_AUDIT_TABLE:-"ScanAuditTable"}
export DYNAMODB_USERS_TABLE=${DYNAMODB_USERS_TABLE:-"Users"}

export SKOOPIN_SERVER_ADDRESS=${SKOOPIN_SERVER_ADDRESS:-"https://mercato.skoopin.net/api/v1"}
export SKOOPIN_CLIENT_ID=${SKOOPIN_CLIENT_ID:-"6q7je53tsgpvcm154gjd3bh04o"}
export SKOOPIN_REFRESH_TOKEN=${SKOOPIN_REFRESH_TOKEN:-""}

export AUDIT_SCAN_ZERO_WEIGHT=${AUDIT_SCAN_ZERO_WEIGHT:-"/tmp/pan_per_scans"}
export AUDIT_CSV_FILE_DIR=${AUDIT_CSV_FILE_DIR:-"/tmp/_auditsCompleted"}
export AUDIT_DIRECTORY=${AUDIT_DIRECTORY:-"/tmp/_audits"}
export GOOGLE_API_KEY=${GOOGLE_API_KEY:-""}
export OPENAI_API_KEY=${OPENAI_API_KEY:-""}

export DB_PORT=${DB_PORT:-"3310"}
export DB_USER=${DB_USER:-"readonly"}
export DB_PASS=${DB_PASS:-""}
export DB_SCHEMA=${DB_SCHEMA:-"skoopin_production"}
export DB_HOST=${DB_HOST:-"127.0.0.1"}
export SSH_HOST=${SSH_HOST:-"production-bastion.skoopin.net"}
export SSH_USERNAME=${SSH_USERNAME:-"ubuntu"}
export SSH_PKEY=${SSH_PKEY:-"~/.ssh/id_rsa_prod_cloud"}
export REMOTE_HOST=${REMOTE_HOST:-"production-database.cp6ohzeoevfi.us-west-2.rds.amazonaws.com"}
export REMOTE_PORT=${REMOTE_PORT:-"3306"}

# Create necessary directories
mkdir -p /tmp/pan_per_scans
mkdir -p /tmp/_auditsCompleted
mkdir -p /tmp/_audits

# Verify AI/ML dependencies are available
echo "🔍 Checking AI/ML dependencies..."
if [ -d "/app/system/model_weights" ]; then
    echo "✅ Model weights directory found"
    ls -la /app/system/model_weights/
else
    echo "⚠️  Model weights directory not found"
fi

if [ -d "/app/audit_automation" ]; then
    echo "✅ Audit automation scripts found"
    ls -la /app/audit_automation/ | head -10
else
    echo "⚠️  Audit automation scripts not found"
fi

# If running in Google Cloud with mounted secrets
if [ -f "/secrets/config.yaml" ]; then
    echo "📋 Using mounted secret config from Google Cloud..."
    cp /secrets/config.yaml /app/backend/config.yaml
elif [ -f "/app/backend/config.yaml" ]; then
    echo "📋 Using existing config.yaml..."
    # File already exists, use it as-is
else
    echo "🔄 Rendering config from environment variables..."
    # Use envsubst to replace environment variables in template
    if [ -f "/app/backend/config.template.yaml" ]; then
        envsubst < /app/backend/config.template.yaml > /app/backend/config.yaml
    else
        echo "⚠️  No config.template.yaml found, using default config"
    fi
fi

echo "✅ Configuration ready, starting uvicorn..."

# Start the application with optimal settings for Cloud Run
exec uvicorn main:app \
    --host 0.0.0.0 \
    --port ${PORT:-8080} \
    --workers 1 \
    --loop asyncio \
    --timeout-keep-alive 120 \
    --access-log \
    --log-level info
