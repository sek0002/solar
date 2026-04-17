import AsyncStorage from "@react-native-async-storage/async-storage";

import type { GeofenceLogEntry, GeofencePreset, VehicleLocationFeed } from "./types";

const GEOFENCES_KEY = "byd_geofences";
const LOGS_KEY = "byd_geofence_logs";
const VEHICLE_FEED_KEY = "byd_vehicle_feed";

async function loadJson<T>(key: string, fallback: T): Promise<T> {
  const raw = await AsyncStorage.getItem(key);
  if (!raw) {
    return fallback;
  }
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export function loadGeofences(): Promise<GeofencePreset[]> {
  return loadJson<GeofencePreset[]>(GEOFENCES_KEY, []);
}

export function saveGeofences(geofences: GeofencePreset[]): Promise<void> {
  return AsyncStorage.setItem(GEOFENCES_KEY, JSON.stringify(geofences));
}

export async function appendGeofenceLog(entry: GeofenceLogEntry): Promise<void> {
  const existing = await loadJson<GeofenceLogEntry[]>(LOGS_KEY, []);
  const next = [entry, ...existing].slice(0, 100);
  await AsyncStorage.setItem(LOGS_KEY, JSON.stringify(next));
}

export function loadGeofenceLogs(): Promise<GeofenceLogEntry[]> {
  return loadJson<GeofenceLogEntry[]>(LOGS_KEY, []);
}

export function clearGeofenceLogs(): Promise<void> {
  return AsyncStorage.removeItem(LOGS_KEY);
}

export function loadVehicleFeed(): Promise<VehicleLocationFeed | null> {
  return loadJson<VehicleLocationFeed | null>(VEHICLE_FEED_KEY, null);
}

export function saveVehicleFeed(feed: VehicleLocationFeed): Promise<void> {
  return AsyncStorage.setItem(VEHICLE_FEED_KEY, JSON.stringify(feed));
}
