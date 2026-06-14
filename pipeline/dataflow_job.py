import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json, math, logging, csv, io

PROJECT = "ais-ports-pipeline-2"
REGION  = "us-central1"
BUCKET  = "gs://ais-ports-pipeline-bucket"
SUB     = f"projects/{PROJECT}/subscriptions/ais-vessel-sub"
BQ_RAW  = f"{PROJECT}:raw.vessel_pings"
PORTS_GCS = f"{BUCKET}/raw-data/ports_for_worker.csv"

def haversine(lat1, lon1, lat2, lon2):
    R  = 3440.065
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a  = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.asin(math.sqrt(a))

class GeofenceJoin(beam.DoFn):
    def setup(self):
        from apache_beam.io.gcp.gcsio import GcsIO
        gcs  = GcsIO()
        data = gcs.open(PORTS_GCS).read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(data))
        self.ports = []
        for row in reader:
            try:
                self.ports.append({
                    "port_id":          row["port_id"],
                    "port_name":        row["port_name"],
                    "country_code":     row["country_code"],
                    "region":           row["region"],
                    "water_body":       row["water_body"],
                    "harbor_size":      row["harbor_size"],
                    "harbor_type":      row["harbor_type"],
                    "channel_depth_m":  float(row["channel_depth_m"] or 0),
                    "latitude":         float(row["latitude"]),
                    "longitude":        float(row["longitude"]),
                })
            except:
                continue
        logging.info(f"Loaded {len(self.ports)} ports from GCS")

    def process(self, vessel):
        try:
            v_lat = float(vessel.get("latitude", 0))
            v_lon = float(vessel.get("longitude", 0))
            if v_lat == 0 and v_lon == 0:
                return

            nearest  = None
            min_dist = float("inf")
            for p in self.ports:
                try:
                    d = haversine(v_lat, v_lon,
                                  p["latitude"], p["longitude"])
                    if d <= 50 and d < min_dist:
                        min_dist = d
                        nearest  = p
                except:
                    continue

            yield {
                "mmsi":            str(vessel.get("mmsi", "")),
                "vessel_name":     str(vessel.get("vessel_name", "Unknown")),
                "vessel_type":     str(vessel.get("vessel_type", "Unknown")),
                "latitude":        v_lat,
                "longitude":       v_lon,
                "speed_knots":     float(vessel.get("speed_knots", 0)),
                "heading_deg":     int(vessel.get("heading_deg", 0)),
                "nav_status":      str(vessel.get("nav_status", "Unknown")),
                "nav_status_code": int(vessel.get("nav_status_code", 15)),
                "draught_m":       float(vessel.get("draught_m", 0)),
                "destination":     str(vessel.get("destination", "Unknown")),
                "imo":             str(vessel.get("imo", "")),
                "ais_timestamp":   str(vessel.get("ais_timestamp", "")),
                "port_id":         nearest["port_id"] if nearest else None,
                "port_name":       nearest["port_name"] if nearest else None,
                "country_code":    nearest["country_code"] if nearest else None,
                "region":          nearest["region"] if nearest else None,
                "water_body":      nearest["water_body"] if nearest else None,
                "harbor_size":     nearest["harbor_size"] if nearest else None,
                "harbor_type":     nearest["harbor_type"] if nearest else None,
                "channel_depth_m": nearest["channel_depth_m"] if nearest else None,
                "distance_nmi":    round(min_dist, 2) if nearest else None,
                "is_congestion":   (
                    vessel.get("nav_status") == "At Anchor"
                    and nearest is not None
                    and min_dist > 5
                ),
            }
        except Exception as e:
            logging.error(f"Error: {e}")

BQ_SCHEMA = ",".join([
    "mmsi:STRING","vessel_name:STRING","vessel_type:STRING",
    "latitude:FLOAT","longitude:FLOAT","speed_knots:FLOAT",
    "heading_deg:INTEGER","nav_status:STRING","nav_status_code:INTEGER",
    "draught_m:FLOAT","destination:STRING","imo:STRING",
    "ais_timestamp:STRING","port_id:STRING","port_name:STRING",
    "country_code:STRING","region:STRING","water_body:STRING",
    "harbor_size:STRING","harbor_type:STRING","channel_depth_m:FLOAT",
    "distance_nmi:FLOAT","is_congestion:BOOLEAN",
])

def run():
    options = PipelineOptions([
        f"--project={PROJECT}",
        f"--region={REGION}",
        f"--temp_location={BUCKET}/dataflow/temp",
        f"--staging_location={BUCKET}/dataflow/staging",
        "--runner=DataflowRunner",
        "--streaming",
        "--job_name=ais-vessel-geofence",
        "--machine_type=e2-small",
        "--num_workers=1",
        "--max_num_workers=1",
        f"--service_account_email=dataflow-sa@{PROJECT}.iam.gserviceaccount.com",
    ])
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=SUB)
            | "ParseJSON"  >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
            | "GeoJoin"    >> beam.ParDo(GeofenceJoin())
            | "WriteBQ"    >> beam.io.WriteToBigQuery(
                BQ_RAW,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    print("Submitting Dataflow job...")
    run()
    print("✓ Job submitted!")