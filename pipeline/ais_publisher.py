import asyncio
import json
import websockets
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID    = "ais-ports-pipeline-2"
TOPIC_ID      = "ais-vessel-stream"
AISSTREAM_KEY = "YOUR_API_KEY"

VESSEL_TYPE_MAP = {
    0:"Unknown",30:"Fishing",31:"Towing",35:"Military",
    36:"Sailing",37:"Pleasure Craft",60:"Passenger",
    61:"Passenger",70:"Cargo",71:"Cargo",72:"Cargo",
    80:"Tanker",81:"Tanker",82:"Tanker",90:"Other"
}

NAVSTAT_MAP = {
    0:"Underway Using Engine",1:"At Anchor",2:"Not Under Command",
    3:"Restricted Manoeuvrability",5:"Moored",6:"Aground",
    7:"Engaged In Fishing",8:"Underway Sailing",15:"Undefined"
}

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
count      = 0

async def connect():
    global count
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe = {
        "APIKey": AISSTREAM_KEY,
        "BoundingBoxes": [
            [[30,-10],[65,40]],
            [[-5,100],[40,135]],
            [[20,-90],[50,-60]]
        ],
        "FilterMessageTypes": ["PositionReport","ShipStaticData"]
    }

    print("=" * 50)
    print("  AIS Vessel Stream Publisher")
    print("  Regions: North Sea | Asia | US East Coast")
    print("=" * 50)
    print("Connecting to AISStream...\n")

    async with websockets.connect(url, ping_interval=20) as ws:
        await ws.send(json.dumps(subscribe))
        print("✓ Connected — live vessel data flowing\n")

        async for raw in ws:
            try:
                msg      = json.loads(raw)
                msg_type = msg.get("MessageType","")
                meta     = msg.get("MetaData",{})

                clean_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                if msg_type == "PositionReport":
                    nav  = msg.get("Message",{}).get("PositionReport",{})
                    data = {
                        "mmsi":            str(meta.get("MMSI","")),
                        "vessel_name":     meta.get("ShipName","Unknown").strip() or "Unknown",
                        "latitude":        float(nav.get("Latitude",0)),
                        "longitude":       float(nav.get("Longitude",0)),
                        "speed_knots":     float(nav.get("Sog",0)),
                        "heading_deg":     int(nav.get("TrueHeading",0)),
                        "course_deg":      float(nav.get("Cog",0)),
                        "nav_status":      NAVSTAT_MAP.get(nav.get("NavigationalStatus",15),"Undefined"),
                        "nav_status_code": int(nav.get("NavigationalStatus",15)),
                        "rot":             float(nav.get("RateOfTurn",0)),
                        "vessel_type":     "Unknown",
                        "draught_m":       0.0,
                        "destination":     "Unknown",
                        "imo":             "",
                        "ais_timestamp":   clean_timestamp,
                    }

                elif msg_type == "ShipStaticData":
                    static = msg.get("Message",{}).get("ShipStaticData",{})
                    data = {
                        "mmsi":            str(meta.get("MMSI","")),
                        "vessel_name":     static.get("Name","Unknown").strip() or "Unknown",
                        "latitude":        float(meta.get("latitude",0)),
                        "longitude":       float(meta.get("longitude",0)),
                        "speed_knots":     0.0,
                        "heading_deg":     0,
                        "course_deg":      0.0,
                        "nav_status":      "Unknown",
                        "nav_status_code": 15,
                        "rot":             0.0,
                        "vessel_type":     VESSEL_TYPE_MAP.get(static.get("Type",0),"Unknown"),
                        "draught_m":       float(static.get("MaximumStaticDraught",0)),
                        "destination":     static.get("Destination","Unknown").strip() or "Unknown",
                        "imo":             str(static.get("ImoNumber","")),
                        "ais_timestamp":   clean_timestamp,
                    }
                else:
                    continue

                if data["latitude"] == 0 and data["longitude"] == 0:
                    continue

                payload = json.dumps(data).encode("utf-8")
                publisher.publish(topic_path, payload, mmsi=data["mmsi"])
                count += 1

                if count % 50 == 0:
                    print(f"[{count:>5} msgs] {data['vessel_name']:<25} | "
                          f"{data['vessel_type']:<15} | "
                          f"{data['nav_status']:<25} | "
                          f"lat:{data['latitude']:>8.3f} lon:{data['longitude']:>9.3f}")

            except Exception as e:
                print(f"Error: {e}")
                continue

asyncio.run(connect())