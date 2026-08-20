#!/usr/bin/env bash
# deploy_dashboard.sh — Run as root on the DigitalOcean droplet
# Sets up Streamlit dashboard as a systemd service behind nginx
set -euo pipefail

APP_DIR="/opt/bid-crawler"
VENV="$APP_DIR/.venv"
APP_FILE="$APP_DIR/bid_crawler/app.py"
SERVICE_NAME="bid-dashboard"
PORT=8501

echo "==> Verifying streamlit is installed..."
"$VENV/bin/streamlit" --version

# ---------------------------------------------------------------------------
# 1. systemd service
# ---------------------------------------------------------------------------
echo "==> Writing systemd service..."
cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Bid Crawler Streamlit Dashboard
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP_DIR
EnvironmentFile=/etc/profile.d/bid-crawler.sh
ExecStart=$VENV/bin/streamlit run $APP_FILE \
    --server.port $PORT \
    --server.address 127.0.0.1 \
    --server.headless true \
    --browser.gatherUsageStats false
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl restart ${SERVICE_NAME}
echo "==> Streamlit service started."

# ---------------------------------------------------------------------------
# 2. nginx
# ---------------------------------------------------------------------------
echo "==> Installing nginx..."
apt-get update -qq && apt-get install -y nginx

echo "==> Writing nginx site config..."
cat > /etc/nginx/sites-available/bid-dashboard <<EOF
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:$PORT;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_read_timeout 86400;
    }
}
EOF

# Enable site, remove default
ln -sf /etc/nginx/sites-available/bid-dashboard /etc/nginx/sites-enabled/bid-dashboard
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl enable nginx
systemctl restart nginx
echo "==> nginx configured."

# ---------------------------------------------------------------------------
# 3. Firewall
# ---------------------------------------------------------------------------
echo "==> Opening firewall ports..."
ufw allow OpenSSH
ufw allow 'Nginx HTTP'
ufw --force enable
echo "==> Firewall updated."

# ---------------------------------------------------------------------------
# 4. Smoke test
# ---------------------------------------------------------------------------
echo "==> Waiting for Streamlit to start..."
sleep 5
if curl -sf http://127.0.0.1:$PORT/healthz > /dev/null 2>&1; then
    echo "==> Streamlit is responding on port $PORT."
else
    echo "==> Streamlit not yet responding on /healthz — checking service status..."
    systemctl status ${SERVICE_NAME} --no-pager | tail -20
fi

echo ""
echo "Done! Dashboard should be live at: http://134.209.172.184"
