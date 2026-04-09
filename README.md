# Solar Monitor

This app turns the existing Powerpal BLE polling idea into a browser-based monitor with:

- background BLE polling for grid usage
- background HTTP polling for local network solar data
- SQLite storage for each datapoint
- live status cards that show reconnect issues and HTTP 404 failures
- an interactive Plotly chart available in the browser

## Configure

Copy `.env.example` into your environment and set:

- `BLE_MAC` and `BLE_PAIRING_CODE` for the Powerpal monitor
- `LOCAL_SITE_URL` for the local device or inverter page
- either JSON paths or regexes for usage and solar extraction:
  - `LOCAL_USAGE_JSON_PATH`
  - `LOCAL_SOLAR_JSON_PATH`
  - `LOCAL_USAGE_REGEX`
  - `LOCAL_SOLAR_REGEX`
  - or fixed line extraction with optional scaling:
  - `LOCAL_USAGE_LINE_INDEX`
  - `LOCAL_SOLAR_LINE_INDEX`
  - `LOCAL_USAGE_DIVISOR`
  - `LOCAL_SOLAR_DIVISOR`
  - `LOCAL_USAGE_MULTIPLIER`
  - `LOCAL_SOLAR_MULTIPLIER`

Examples:

- JSON: `LOCAL_SOLAR_JSON_PATH=data.production.watts`
- HTML/JS text: `LOCAL_SOLAR_REGEX=solarGeneration\\D+([0-9.]+)`
- Line-based page like `/home.cgi`: `LOCAL_SOLAR_LINE_INDEX=10` and `LOCAL_SOLAR_DIVISOR=60`
- Script-style fallback on fetch/parse failure now uses the average of the previous three values by default:
  - `LOCAL_SITE_ZERO_ON_ERROR=true`
  - `FAILURE_AVERAGE_WINDOW=3`

## Run locally

```bash
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m uvicorn app.main:app --reload --port 8001
```

Open [http://localhost:8001](http://localhost:8001).

## Run as a Raspberry Pi service

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip bluetooth bluez libcap2-bin
sudo mkdir -p /opt/solar-monitor
sudo rsync -av ./ /opt/solar-monitor/
sudo cp /opt/solar-monitor/deploy/solar-monitor.service /etc/systemd/system/solar-monitor.service
```

Then edit your environment file:

```bash
cd /opt/solar-monitor
cp .env.example .env
nano .env
```

To run a Raspberry Pi as a collector-only node that forwards data to another server, set:

```env
POLLER_ONLY=true
REMOTE_INGEST_URL=http://your-server:8001
REMOTE_INGEST_TOKEN=your-shared-token
BLE_ENABLED=true
LOCAL_SITE_ENABLED=false
BYD_ENABLED=false
```

In that mode, `poll_solar.sh` runs only the pollers and does not start `uvicorn`.

To run the Raspberry Pi as a tiny BLE-only site that exposes the latest reading as simple text lines, set:

```env
BLE_SITE_ONLY=true
BLE_ENABLED=true
NETWORK_BLE_ENABLED=false
LOCAL_SITE_ENABLED=false
BYD_ENABLED=false
BLE_SITE_PORT=8002
```

That starts a minimal app on `http://<pi-ip>:8002/` with these lines:

1. latest BLE grid usage watts
2. battery percent
3. observed timestamp
4. BLE state

There is also a simple human-readable page at `http://<pi-ip>:8002/html`.

To have the main webapp server read BLE from that page, set on the server:

```env
BLE_ENABLED=false
NETWORK_BLE_ENABLED=true
NETWORK_BLE_URL=http://<pi-ip>:8002/
LOCAL_SITE_ENABLED=true
BYD_ENABLED=true
```

In that mode, the webapp fetches the BLE text page just like the solar/local site fetch path, stores the reading as `ble`, and shows a `network_ble` collector status card.

If you want port 80 instead of port 8001, either put nginx in front of the app or grant the venv Python permission to bind low ports:

```bash
sudo setcap 'cap_net_bind_service=+ep' /opt/solar-monitor/.venv/bin/python3
```

`poll_solar.sh` creates the venv with `--system-site-packages`, so the service can import globally installed `python3` packages like `bleak` while still keeping the app's own dependencies in `.venv`.

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable solar-monitor
sudo systemctl start solar-monitor
sudo systemctl status solar-monitor
```

View logs:

```bash
journalctl -u solar-monitor -f
```

The browser UI will then be available at [http://raspberrypi.local:8001](http://raspberrypi.local:8001) or `http://<pi-ip>:8001`.

If `pip install -r requirements.txt` hangs or spends a long time building `uvloop` on the Raspberry Pi, use the plain `uvicorn` dependency in this repo. The app does not need `uvicorn[standard]` for the Pi service setup.

## Enable HTTPS with Caddy

If you want Chrome on Android to offer app install, serve the dashboard over HTTPS with a real domain name.

1. Point a DNS name at your server, for example `solar.example.com`.
2. Keep the app running locally on `127.0.0.1:8001` via the included `solar-monitor.service`.
3. Install Caddy:

```bash
sudo apt update
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install -y caddy
```

4. Copy the example reverse-proxy config and replace the hostname:

```bash
sudo cp /opt/solar-monitor/deploy/Caddyfile /etc/caddy/Caddyfile
sudo nano /etc/caddy/Caddyfile
```

Set it to:

```caddyfile
solar.example.com {
  encode gzip zstd
  reverse_proxy 127.0.0.1:8001
}
```

5. Open the firewall for web traffic if needed:

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
```

6. Start Caddy:

```bash
sudo systemctl restart caddy
sudo systemctl enable caddy
sudo systemctl status caddy
```

Once DNS is correct, Caddy will obtain and renew the TLS certificate automatically. Your dashboard should then be available at `https://solar.example.com`.

## Service notes

- The included unit file is [deploy/solar-monitor.service](/Users/sekkevin/LocalR/solar/deploy/solar-monitor.service).
- An example HTTPS reverse-proxy config is [deploy/Caddyfile](/Users/sekkevin/LocalR/solar/solar/deploy/Caddyfile).
- It assumes the app is deployed to `/opt/solar-monitor` and runs as user `sek0002` with group `root`.
- BLE works much more naturally on bare metal Linux than in Docker, which is why this deployment mode is the better fit for a Raspberry Pi.
- If your Pi uses a different account mapping, update `User=` and `Group=` in the service file before copying it into `/etc/systemd/system/`.

## Notes about the original scripts

- `test2.sh` contains Python BLE logic, not shell.
- `poll_solar.sh` points to `test2.py`, which is not present in the repo.
- The new app keeps the original Powerpal pairing and notification flow, but wraps it with persistent storage and browser visualization.
