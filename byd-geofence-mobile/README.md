# BYD Geofence Mobile

A separate Expo / React Native companion app for background geofencing around:

- the user's current phone location
- the vehicle's latest location from the BYD web console
- manually entered vehicle coordinates

## What it does

- requests foreground and background location permissions
- creates custom geofences with adjustable radius
- monitors geofence enter / exit events in the background
- shows local notifications when a geofence is entered or exited
- stores a local event history on-device
- can fetch the current vehicle coordinates from the BYD web console endpoint:
  - `GET /api/vehicle-location`

## Important limitation

This is designed for a development build or standalone build.

`Expo Go` is not sufficient for reliable background geofencing. Use:

```bash
npm install
npx expo run:android
```

or

```bash
npx expo run:ios
```

## Install

```bash
cd byd-geofence-mobile
npm install
```

## Start development

```bash
npm start
```

## Build notes

The app uses:

- `expo-location`
- `expo-task-manager`
- `expo-notifications`
- `@react-native-async-storage/async-storage`

Background geofence events are handled in [src/geofencing.ts](/Users/sekkevin/LocalR/solar/byd-geofence-mobile/src/geofencing.ts).

## Vehicle location feed

If your BYD web console is running, enter its vehicle location endpoint in the mobile app:

```text
https://your-host.example.com/api/vehicle-location
```

The mobile app will use that endpoint to pull the latest car coordinates, then let you create a geofence around that position.
