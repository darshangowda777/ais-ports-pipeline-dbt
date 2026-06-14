from google.cloud import pubsub_v1, storage
import json
import time
from datetime import datetime

PROJECT_ID    = "ais-ports-pipeline-2"
SUBSCRIPTION  = "ais-vessel-sub"
BUCKET        = "ais-ports-pipeline-bucket"
MAX_MESSAGES  = 10000
BATCH_SIZE    = 1000

subscriber   = pubsub_v1.SubscriberClient()
sub_path     = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)
storage_client = storage.Client(project=PROJECT_ID)
bucket       = storage_client.bucket(BUCKET)

messages = []
print(f"Pulling messages from Pub/Sub...")

while len(messages) < MAX_MESSAGES:
    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": BATCH_SIZE},
        timeout=30
    )
    if not response.received_messages:
        print(f"No more messages. Total pulled: {len(messages)}")
        break

    ack_ids = []
    for msg in response.received_messages:
        try:
            data = json.loads(msg.message.data.decode("utf-8"))
            messages.append(data)
            ack_ids.append(msg.ack_id)
        except:
            continue

    subscriber.acknowledge(
        request={"subscription": sub_path, "ack_ids": ack_ids}
    )
    print(f"Pulled {len(messages)} messages so far...")

# Save as JSON lines to GCS
timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
filename   = f"raw-data/vessel_pings/pings_{timestamp}.json"
json_lines = "\n".join(json.dumps(m) for m in messages)

blob = bucket.blob(filename)
blob.upload_from_string(json_lines, content_type="application/json")

print(f"✓ Saved {len(messages)} messages to gs://{BUCKET}/{filename}")
