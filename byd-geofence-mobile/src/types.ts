export type GeofenceSource = "user" | "vehicle";

export type GeofencePreset = {
  id: string;
  name: string;
  source: GeofenceSource;
  latitude: number;
  longitude: number;
  radiusMeters: number;
  createdAt: string;
};

export type VehicleLocationFeed = {
  url: string;
  lastFetchedAt?: string;
};

export type GeofenceLogEntry = {
  id: string;
  regionId: string;
  regionName: string;
  eventType: "enter" | "exit";
  occurredAt: string;
};
