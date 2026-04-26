# Home Assistant Integration

This app exposes two Home Assistant-friendly read APIs:

- `/api/home-assistant/summary`
- `/api/home-assistant/history?hours=24&target_points=288`

Both endpoints require a bearer token from `HOME_ASSISTANT_API_TOKEN`.

Example request:

```bash
curl \
  -H "Authorization: Bearer YOUR_HOME_ASSISTANT_API_TOKEN" \
  https://solar.example.com/api/home-assistant/summary
```

## What the endpoints return

`/api/home-assistant/summary`

- current BLE grid, site solar, and BYD EV power in `kW`
- charger on/off, current, manual override, and automation state
- BLE battery and EV SoC
- daily / weekly / monthly totals
- latest cumulative totals

`/api/home-assistant/history`

- window metadata
- downsampled time-series for:
  - `grid`
  - `solar`
  - `ev`
- filtered cumulative series for:
  - `grid`
  - `solar`
  - `ev`

## Home Assistant setup

1. Set `HOME_ASSISTANT_API_TOKEN` in the Solar app environment.
2. Copy [rest_package.yaml](/Users/sekkevin/LocalR/solar/solar/home_assistant/rest_package.yaml) into your Home Assistant `packages/` directory.
3. Replace:
   - `https://solar.example.com`
   - `YOUR_HOME_ASSISTANT_API_TOKEN`
4. Restart Home Assistant.
5. Add cards from [lovelace_example.yaml](/Users/sekkevin/LocalR/solar/solar/home_assistant/lovelace_example.yaml).

## Notes

- The example history cards use the custom [ApexCharts Card](https://github.com/RomRider/apexcharts-card).
- If you only want built-in Home Assistant graphs, use the template sensors from the package and add `history-graph` cards for them.
