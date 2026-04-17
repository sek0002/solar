import { ReactNode, useEffect, useMemo, useState } from "react";
import {
  Alert,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { StatusBar } from "expo-status-bar";
import * as Location from "expo-location";
import * as Notifications from "expo-notifications";

import { GEOFENCE_TASK_NAME, syncBackgroundGeofences } from "./src/geofencing";
import {
  clearGeofenceLogs,
  loadGeofenceLogs,
  loadGeofences,
  loadVehicleFeed,
  saveGeofences,
  saveVehicleFeed,
} from "./src/storage";
import type { GeofenceLogEntry, GeofencePreset, GeofenceSource } from "./src/types";


Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowBanner: true,
    shouldShowList: true,
    shouldPlaySound: true,
    shouldSetBadge: false,
  }),
});

type Coordinate = {
  latitude: number;
  longitude: number;
};

const DEFAULT_RADIUS = "150";

function newId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function ensurePermissions() {
  const foreground = await Location.requestForegroundPermissionsAsync();
  const background = await Location.requestBackgroundPermissionsAsync();
  await Notifications.requestPermissionsAsync();
  return {
    foreground: foreground.status,
    background: background.status,
  };
}

export default function App() {
  const [permissionSummary, setPermissionSummary] = useState("Permissions not requested yet.");
  const [currentLocation, setCurrentLocation] = useState<Coordinate | null>(null);
  const [vehicleLocation, setVehicleLocation] = useState<Coordinate | null>(null);
  const [vehicleFeedUrl, setVehicleFeedUrl] = useState("");
  const [geofences, setGeofences] = useState<GeofencePreset[]>([]);
  const [logs, setLogs] = useState<GeofenceLogEntry[]>([]);
  const [radiusMeters, setRadiusMeters] = useState(DEFAULT_RADIUS);
  const [customName, setCustomName] = useState("");
  const [manualVehicleLat, setManualVehicleLat] = useState("");
  const [manualVehicleLng, setManualVehicleLng] = useState("");
  const [status, setStatus] = useState("Ready.");

  const geofencingCountText = useMemo(() => `${geofences.length} active zone${geofences.length === 1 ? "" : "s"}`, [geofences.length]);

  useEffect(() => {
    void (async () => {
      const [savedGeofences, savedLogs, savedFeed] = await Promise.all([
        loadGeofences(),
        loadGeofenceLogs(),
        loadGeofences().then(async () => {
          const feed = await loadVehicleFeed();
          return feed;
        }),
      ]);
      setGeofences(savedGeofences);
      setLogs(savedLogs);
      if (savedFeed?.url) {
        setVehicleFeedUrl(savedFeed.url);
      }
    })();
  }, []);

  async function refreshCurrentLocation() {
    setStatus("Reading current phone location...");
    const location = await Location.getCurrentPositionAsync({
      accuracy: Location.Accuracy.Balanced,
    });
    const coords = {
      latitude: location.coords.latitude,
      longitude: location.coords.longitude,
    };
    setCurrentLocation(coords);
    setStatus(`Phone location updated: ${coords.latitude.toFixed(5)}, ${coords.longitude.toFixed(5)}`);
    return coords;
  }

  async function setupPermissions() {
    const permissions = await ensurePermissions();
    setPermissionSummary(
      `Foreground: ${permissions.foreground} | Background: ${permissions.background}`,
    );
    setStatus("Permission check complete.");
  }

  async function fetchVehicleLocation() {
    if (!vehicleFeedUrl.trim()) {
      Alert.alert("Vehicle feed URL required", "Enter the BYD web console /api/vehicle-location endpoint first.");
      return;
    }
    setStatus("Fetching vehicle location from BYD web console...");
    const response = await fetch(vehicleFeedUrl.trim());
    if (!response.ok) {
      throw new Error(`Vehicle feed returned ${response.status}`);
    }
    const payload = (await response.json()) as {
      latitude?: number | null;
      longitude?: number | null;
    };
    if (payload.latitude == null || payload.longitude == null) {
      throw new Error("Vehicle feed did not include coordinates.");
    }
    const coords = {
      latitude: Number(payload.latitude),
      longitude: Number(payload.longitude),
    };
    setVehicleLocation(coords);
    await saveVehicleFeed({ url: vehicleFeedUrl.trim(), lastFetchedAt: new Date().toISOString() });
    setStatus(`Vehicle location updated: ${coords.latitude.toFixed(5)}, ${coords.longitude.toFixed(5)}`);
  }

  function saveManualVehicleLocation() {
    const latitude = Number(manualVehicleLat);
    const longitude = Number(manualVehicleLng);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      Alert.alert("Invalid coordinates", "Enter numeric latitude and longitude for the vehicle.");
      return;
    }
    setVehicleLocation({ latitude, longitude });
    setStatus(`Manual vehicle location saved: ${latitude.toFixed(5)}, ${longitude.toFixed(5)}`);
  }

  async function addGeofence(source: GeofenceSource) {
    const radius = Number(radiusMeters);
    if (!Number.isFinite(radius) || radius <= 0) {
      Alert.alert("Invalid radius", "Enter a radius in meters greater than zero.");
      return;
    }

    const center =
      source === "user"
        ? currentLocation ?? (await refreshCurrentLocation())
        : vehicleLocation;

    if (!center) {
      Alert.alert(
        "Missing center point",
        source === "user"
          ? "Unable to determine the current phone location yet."
          : "Set or fetch the vehicle location first.",
      );
      return;
    }

    const geofence: GeofencePreset = {
      id: newId(),
      name: customName.trim() || (source === "user" ? "My location zone" : "Vehicle zone"),
      source,
      latitude: center.latitude,
      longitude: center.longitude,
      radiusMeters: radius,
      createdAt: new Date().toISOString(),
    };

    const next = [geofence, ...geofences];
    setGeofences(next);
    await saveGeofences(next);
    await syncBackgroundGeofences(next);
    setCustomName("");
    setStatus(`Geofence "${geofence.name}" saved and background monitoring started.`);
  }

  async function removeGeofence(id: string) {
    const next = geofences.filter((item) => item.id !== id);
    setGeofences(next);
    await saveGeofences(next);
    await syncBackgroundGeofences(next);
    setStatus("Geofence removed.");
  }

  async function clearLogs() {
    await clearGeofenceLogs();
    setLogs([]);
    setStatus("Geofence event history cleared.");
  }

  async function refreshLogs() {
    setLogs(await loadGeofenceLogs());
  }

  useEffect(() => {
    void refreshLogs();
  }, [geofences.length]);

  return (
    <SafeAreaView style={styles.safeArea}>
      <StatusBar style="light" />
      <ScrollView contentContainerStyle={styles.container}>
        <Text style={styles.eyebrow}>BYD Geofence Mobile</Text>
        <Text style={styles.title}>Background geofences for you and your vehicle</Text>
        <Text style={styles.subtitle}>
          Create custom geofences around your current phone location or the latest vehicle location.
          Background region monitoring is powered by Expo Location and Task Manager.
        </Text>

        <Card title="Permissions">
          <Text style={styles.body}>{permissionSummary}</Text>
          <ActionButton label="Request location + notification permissions" onPress={setupPermissions} />
          <Text style={styles.helper}>
            Background geofencing requires a development build or standalone app. Expo Go is not enough.
          </Text>
        </Card>

        <Card title="Current phone location">
          <Text style={styles.body}>
            {currentLocation
              ? `${currentLocation.latitude.toFixed(5)}, ${currentLocation.longitude.toFixed(5)}`
              : "No phone location captured yet."}
          </Text>
          <ActionButton label="Refresh current phone location" onPress={refreshCurrentLocation} />
        </Card>

        <Card title="Vehicle location">
          <TextInput
            style={styles.input}
            placeholder="https://your-byd-console/api/vehicle-location"
            placeholderTextColor="#6b7280"
            value={vehicleFeedUrl}
            autoCapitalize="none"
            onChangeText={setVehicleFeedUrl}
          />
          <ActionButton label="Fetch from BYD web console" onPress={fetchVehicleLocation} />
          <Text style={styles.helper}>Or enter coordinates manually:</Text>
          <View style={styles.row}>
            <TextInput
              style={[styles.input, styles.rowInput]}
              placeholder="Latitude"
              placeholderTextColor="#6b7280"
              keyboardType="numeric"
              value={manualVehicleLat}
              onChangeText={setManualVehicleLat}
            />
            <TextInput
              style={[styles.input, styles.rowInput]}
              placeholder="Longitude"
              placeholderTextColor="#6b7280"
              keyboardType="numeric"
              value={manualVehicleLng}
              onChangeText={setManualVehicleLng}
            />
          </View>
          <ActionButton label="Save manual vehicle location" onPress={saveManualVehicleLocation} tone="secondary" />
          <Text style={styles.body}>
            {vehicleLocation
              ? `Vehicle: ${vehicleLocation.latitude.toFixed(5)}, ${vehicleLocation.longitude.toFixed(5)}`
              : "No vehicle location available yet."}
          </Text>
        </Card>

        <Card title="Create geofence">
          <TextInput
            style={styles.input}
            placeholder="Optional geofence name"
            placeholderTextColor="#6b7280"
            value={customName}
            onChangeText={setCustomName}
          />
          <TextInput
            style={styles.input}
            placeholder="Radius in meters"
            placeholderTextColor="#6b7280"
            keyboardType="numeric"
            value={radiusMeters}
            onChangeText={setRadiusMeters}
          />
          <View style={styles.row}>
            <ActionButton label="Around my location" onPress={() => addGeofence("user")} />
            <ActionButton label="Around vehicle" onPress={() => addGeofence("vehicle")} tone="secondary" />
          </View>
          <Text style={styles.helper}>
            Each geofence monitors enter and exit transitions and triggers a local notification.
          </Text>
        </Card>

        <Card title={`Active geofences · ${geofencingCountText}`}>
          {geofences.length === 0 ? (
            <Text style={styles.body}>No active geofences yet.</Text>
          ) : (
            geofences.map((item) => (
              <View key={item.id} style={styles.listItem}>
                <View style={styles.listItemCopy}>
                  <Text style={styles.listTitle}>{item.name}</Text>
                  <Text style={styles.body}>
                    {item.source === "user" ? "User" : "Vehicle"} · {item.radiusMeters}m
                  </Text>
                  <Text style={styles.helper}>
                    {item.latitude.toFixed(5)}, {item.longitude.toFixed(5)}
                  </Text>
                </View>
                <Pressable onPress={() => void removeGeofence(item.id)} style={styles.removeButton}>
                  <Text style={styles.removeButtonText}>Remove</Text>
                </Pressable>
              </View>
            ))
          )}
        </Card>

        <Card title="Event history">
          <View style={styles.row}>
            <ActionButton label="Refresh log" onPress={refreshLogs} tone="secondary" />
            <ActionButton label="Clear log" onPress={clearLogs} tone="secondary" />
          </View>
          {logs.length === 0 ? (
            <Text style={styles.body}>No enter/exit events recorded yet.</Text>
          ) : (
            logs.map((log) => (
              <View key={log.id} style={styles.logItem}>
                <Text style={styles.listTitle}>
                  {log.eventType === "enter" ? "Entered" : "Exited"} · {log.regionName}
                </Text>
                <Text style={styles.helper}>{new Date(log.occurredAt).toLocaleString()}</Text>
              </View>
            ))
          )}
        </Card>

        <Card title="Task status">
          <Text style={styles.body}>Task name: {GEOFENCE_TASK_NAME}</Text>
          <Text style={styles.helper}>{status}</Text>
        </Card>
      </ScrollView>
    </SafeAreaView>
  );
}

function Card(props: { title: string; children: ReactNode }) {
  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>{props.title}</Text>
      <View style={styles.cardBody}>{props.children}</View>
    </View>
  );
}

function ActionButton(props: { label: string; onPress: () => void | Promise<void>; tone?: "primary" | "secondary" }) {
  return (
    <Pressable
      onPress={() => void props.onPress()}
      style={[styles.button, props.tone === "secondary" ? styles.buttonSecondary : null]}
    >
      <Text style={styles.buttonText}>{props.label}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: "#08111d",
  },
  container: {
    padding: 20,
    gap: 16,
  },
  eyebrow: {
    color: "#38bdf8",
    textTransform: "uppercase",
    letterSpacing: 2,
    fontSize: 12,
    fontWeight: "700",
  },
  title: {
    color: "#edf4ff",
    fontSize: 30,
    fontWeight: "800",
  },
  subtitle: {
    color: "#9aa9bd",
    fontSize: 15,
    lineHeight: 22,
  },
  card: {
    backgroundColor: "#0f172a",
    borderRadius: 22,
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.18)",
    padding: 18,
    gap: 12,
  },
  cardTitle: {
    color: "#edf4ff",
    fontSize: 18,
    fontWeight: "700",
  },
  cardBody: {
    gap: 10,
  },
  body: {
    color: "#dbe7f5",
    fontSize: 14,
    lineHeight: 20,
  },
  helper: {
    color: "#94a3b8",
    fontSize: 13,
    lineHeight: 18,
  },
  input: {
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.18)",
    backgroundColor: "#111827",
    color: "#edf4ff",
    paddingHorizontal: 14,
    paddingVertical: 12,
    borderRadius: 14,
  },
  row: {
    flexDirection: "row",
    gap: 10,
  },
  rowInput: {
    flex: 1,
  },
  button: {
    flex: 1,
    backgroundColor: "#0284c7",
    paddingVertical: 13,
    paddingHorizontal: 16,
    borderRadius: 14,
    alignItems: "center",
  },
  buttonSecondary: {
    backgroundColor: "#334155",
  },
  buttonText: {
    color: "#f8fafc",
    fontWeight: "700",
  },
  listItem: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
    alignItems: "center",
    backgroundColor: "#111827",
    padding: 14,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.18)",
  },
  listItemCopy: {
    flex: 1,
    gap: 4,
  },
  listTitle: {
    color: "#edf4ff",
    fontWeight: "700",
  },
  removeButton: {
    backgroundColor: "rgba(248,113,113,0.16)",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderRadius: 12,
  },
  removeButtonText: {
    color: "#fecaca",
    fontWeight: "700",
  },
  logItem: {
    backgroundColor: "#111827",
    padding: 14,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.18)",
    gap: 4,
  },
});
