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

Examples:

- JSON: `LOCAL_SOLAR_JSON_PATH=data.production.watts`
- HTML/JS text: `LOCAL_SOLAR_REGEX=solarGeneration\\D+([0-9.]+)`

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000).

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

If you want port 80 instead of port 8000, either put nginx in front of the app or grant the venv Python permission to bind low ports:

```bash
sudo setcap 'cap_net_bind_service=+ep' /opt/solar-monitor/.venv/bin/python3
```

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

The browser UI will then be available at [http://raspberrypi.local:8000](http://raspberrypi.local:8000) or `http://<pi-ip>:8000`.

If `pip install -r requirements.txt` hangs or spends a long time building `uvloop` on the Raspberry Pi, use the plain `uvicorn` dependency in this repo. The app does not need `uvicorn[standard]` for the Pi service setup.

## Service notes

- The included unit file is [deploy/solar-monitor.service](/Users/sekkevin/LocalR/solar/deploy/solar-monitor.service).
- It assumes the app is deployed to `/opt/solar-monitor` and runs as user `sek0002` with group `root`.
- BLE works much more naturally on bare metal Linux than in Docker, which is why this deployment mode is the better fit for a Raspberry Pi.
- If your Pi uses a different account mapping, update `User=` and `Group=` in the service file before copying it into `/etc/systemd/system/`.

## Notes about the original scripts

- `test2.sh` contains Python BLE logic, not shell.
- `poll_solar.sh` points to `test2.py`, which is not present in the repo.
- The new app keeps the original Powerpal pairing and notification flow, but wraps it with persistent storage and browser visualization.
