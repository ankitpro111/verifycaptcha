import firebase_admin
from firebase_admin import credentials, firestore
import json
import time

# Initialize Firebase (skip if already initialized)
if not firebase_admin._apps:
    cred = credentials.Certificate("../firebase/nonuserdata-firebase-adminsdk-fbsvc-577bb1155f.json")
    firebase_admin.initialize_app(cred)

city_name = "bangalore"

INPUT_FILE = f"./{city_name}/units_{city_name}_data.json"

db = firestore.client()

# Load units
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    units = json.load(f)

BATCH_SIZE = 500
total_units = len(units)
print(f"📦 Total units to upload: {total_units}")

batch_number = 1

# EXCLUDE_KEYS = {"onFirebase", "id", "unique_unit"}
EXCLUDE_KEYS = {"id"}
for i in range(0, total_units, BATCH_SIZE):
    batch = db.batch()
    chunk = units[i:i + BATCH_SIZE]
    indexes_to_mark = []
    doc_refs = []
    # Prepare batch
    for index, unit in enumerate(chunk):
        try:
            if not unit.get("onFirebase"):
                doc_ref = db.collection("sy_units").document()  # generate unique doc ID
                  
                # Prepare data for Firestore without local-only keys
                firebase_unit = {k: v for k, v in unit.items() if k not in EXCLUDE_KEYS}
                
                batch.set(doc_ref, firebase_unit)

                batch.set(doc_ref, unit)
                indexes_to_mark.append(i + index)
                doc_refs.append(doc_ref)  # store for later
        except Exception as e:
            print(f"❌ Error preparing unit at index {i + index}: {e}")

    # Commit batch
    try:
        if indexes_to_mark:
            batch.commit()
            print(f"✅ Batch {batch_number} committed ({i + len(chunk)}/{total_units})")
            # Update units with Firebase info
            for idx, doc_ref in zip(indexes_to_mark, doc_refs):
                units[idx]["onFirebase"] = True
                units[idx]["id"] = doc_ref.id  # store the actual doc ID
            batch_number += 1
            time.sleep(1)
    except Exception as e:
        print(f"❌ Batch commit failed at offset {i}: {e}")

# Save updated units back to the file
with open(INPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(units, f, ensure_ascii=False, indent=2)

print("✅ All batches processed.")
