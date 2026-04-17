import * as Location from "expo-location";
import * as Notifications from "expo-notifications";
import * as TaskManager from "expo-task-manager";

import { appendGeofenceLog, loadGeofences } from "./storage";
import type { GeofencePreset } from "./types";

export const GEOFENCE_TASK_NAME = "byd-geofence-task";

TaskManager.defineTask(GEOFENCE_TASK_NAME, async ({ data, error }) => {
  if (error) {
    return;
  }

  const event = data as
    | {
        eventType?: Location.GeofencingEventType;
        region?: { identifier?: string };
      }
    | undefined;

  const regionId = event?.region?.identifier;
  if (!regionId || event?.eventType == null) {
    return;
  }

  const geofences = await loadGeofences();
  const geofence = geofences.find((item) => item.id === regionId);
  const eventType = event.eventType === Location.GeofencingEventType.Enter ? "enter" : "exit";
  const regionName = geofence?.name ?? regionId;

  await appendGeofenceLog({
    id: `${regionId}-${Date.now()}`,
    regionId,
    regionName,
    eventType,
    occurredAt: new Date().toISOString(),
  });

  await Notifications.scheduleNotificationAsync({
    content: {
      title: `Geofence ${eventType === "enter" ? "entered" : "exited"}`,
      body: `${regionName} was ${eventType === "enter" ? "entered" : "left"}.`,
      sound: "default",
    },
    trigger: null,
  });
});

function toLocationRegion(geofence: GeofencePreset): Location.LocationRegion {
  return {
    identifier: geofence.id,
    latitude: geofence.latitude,
    longitude: geofence.longitude,
    radius: geofence.radiusMeters,
    notifyOnEnter: true,
    notifyOnExit: true,
  };
}

export async function syncBackgroundGeofences(geofences: GeofencePreset[]): Promise<void> {
  if (geofences.length === 0) {
    const started = await Location.hasStartedGeofencingAsync(GEOFENCE_TASK_NAME);
    if (started) {
      await Location.stopGeofencingAsync(GEOFENCE_TASK_NAME);
    }
    return;
  }

  await Location.startGeofencingAsync(
    GEOFENCE_TASK_NAME,
    geofences.map(toLocationRegion),
  );
}
