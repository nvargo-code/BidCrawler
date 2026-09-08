#!/bin/bash
# Daily bid crawler refresh — crawls every enabled source, reconciles closed
# bids, and pushes the result to Supabase. Runs via cron on the DigitalOcean
# droplet (root's crontab: `0 6 * * * /opt/bid-crawler/daily_refresh.sh`).
source /etc/profile.d/bid-crawler.sh
cd /opt/bid-crawler
echo "===== $(date) =====" >> data/scheduled_run.log
.venv/bin/bid-crawler sync --run-first >> data/scheduled_run.log 2>&1
