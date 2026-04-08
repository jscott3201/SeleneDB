#!/usr/bin/env python3
"""Create edges for the campus dataset using the REST API."""

import json
import random
import urllib.request
import sys

URL = "http://localhost:8080"
HEADERS = {"Content-Type": "application/json"}
random.seed(42)

edge_count = 0

def gql(query, params=None):
    body = {"query": query}
    if params:
        body["parameters"] = params
    data = json.dumps(body).encode()
    req = urllib.request.Request(f"{URL}/gql", data=data, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

def create_edge(src, tgt, label):
    global edge_count
    body = json.dumps({"source": src, "target": tgt, "label": label}).encode()
    req = urllib.request.Request(f"{URL}/edges", data=body, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            edge_count += 1
    except Exception as e:
        pass  # skip failures silently

def get_ids(label, limit=10000):
    r = gql(f"MATCH (n:{label}) RETURN id(n) AS id LIMIT {limit}")
    return [row["ID"] for row in r.get("data", [])]

def progress(msg):
    print(f"  [{edge_count:,} edges] {msg}")

print("=== Creating edges for campus dataset ===\n")

# Fetch all node IDs by label
campus_ids = get_ids("Campus")
building_ids = get_ids("Building")
floor_ids = get_ids("Floor")
room_ids = get_ids("Room")
sensor_ids = get_ids("Sensor")
equip_ids = get_ids("Equipment")
person_ids = get_ids("Person")
dept_ids = get_ids("Department")
course_ids = get_ids("Course")
network_ids = get_ids("NetworkDevice")
workorder_ids = get_ids("WorkOrder")
alert_ids = get_ids("Alert")

print(f"Fetched IDs: {len(campus_ids)} campus, {len(building_ids)} buildings, "
      f"{len(floor_ids)} floors, {len(room_ids)} rooms, {len(sensor_ids)} sensors, "
      f"{len(equip_ids)} equip, {len(person_ids)} people, {len(dept_ids)} depts, "
      f"{len(course_ids)} courses, {len(network_ids)} network, "
      f"{len(workorder_ids)} workorders, {len(alert_ids)} alerts\n")

# Filter to only new IDs (>5685 from previous data)
MIN_NEW = 5685
new_building_ids = [i for i in building_ids if i > MIN_NEW]
new_floor_ids = [i for i in floor_ids if i > MIN_NEW]
new_room_ids = [i for i in room_ids if i > MIN_NEW]
new_sensor_ids = [i for i in sensor_ids if i > MIN_NEW]
new_equip_ids = [i for i in equip_ids if i > MIN_NEW]
new_person_ids = [i for i in person_ids if i > MIN_NEW]
new_dept_ids = [i for i in dept_ids if i > MIN_NEW]
new_course_ids = [i for i in course_ids if i > MIN_NEW]
new_network_ids = [i for i in network_ids if i > MIN_NEW]
new_workorder_ids = [i for i in workorder_ids if i > MIN_NEW]
new_alert_ids = [i for i in alert_ids if i > MIN_NEW]
new_campus_ids = [i for i in campus_ids if i > MIN_NEW]

# Campus -> Buildings
for bid in new_building_ids:
    if new_campus_ids:
        create_edge(new_campus_ids[0], bid, "contains")
progress(f"campus->buildings ({len(new_building_ids)})")

# Buildings -> Floors (distribute evenly)
floors_per_building = len(new_floor_ids) // max(len(new_building_ids), 1)
for i, fid in enumerate(new_floor_ids):
    bi = min(i // max(floors_per_building, 1), len(new_building_ids) - 1)
    create_edge(new_building_ids[bi], fid, "contains")
progress(f"buildings->floors ({len(new_floor_ids)})")

# Floors -> Rooms (distribute evenly)
rooms_per_floor = len(new_room_ids) // max(len(new_floor_ids), 1)
for i, rid in enumerate(new_room_ids):
    fi = min(i // max(rooms_per_floor, 1), len(new_floor_ids) - 1)
    create_edge(new_floor_ids[fi], rid, "contains")
    if (i + 1) % 200 == 0:
        progress(f"floors->rooms {i+1}/{len(new_room_ids)}")
progress(f"floors->rooms ({len(new_room_ids)})")

# Rooms -> Sensors (distribute ~5 per room)
sensors_per_room = max(len(new_sensor_ids) // max(len(new_room_ids), 1), 1)
for i, sid in enumerate(new_sensor_ids):
    ri = min(i // sensors_per_room, len(new_room_ids) - 1)
    create_edge(new_room_ids[ri], sid, "monitors")
    if (i + 1) % 500 == 0:
        progress(f"rooms->sensors {i+1}/{len(new_sensor_ids)}")
progress(f"rooms->sensors ({len(new_sensor_ids)})")

# Rooms -> Equipment (distribute ~3 per room)
equip_per_room = max(len(new_equip_ids) // max(len(new_room_ids), 1), 1)
for i, eid in enumerate(new_equip_ids):
    ri = min(i // equip_per_room, len(new_room_ids) - 1)
    create_edge(new_room_ids[ri], eid, "equipped_with")
    if (i + 1) % 500 == 0:
        progress(f"rooms->equipment {i+1}/{len(new_equip_ids)}")
progress(f"rooms->equipment ({len(new_equip_ids)})")

# Buildings -> Departments
for did in new_dept_ids:
    create_edge(random.choice(new_building_ids), did, "houses")
progress(f"buildings->departments ({len(new_dept_ids)})")

# Departments -> People
for pid in new_person_ids:
    create_edge(random.choice(new_dept_ids), pid, "employs")
    if (len(new_room_ids) > 0 and random.random() < 0.5):
        create_edge(pid, random.choice(new_room_ids), "occupies")
progress(f"depts->people + occupies ({len(new_person_ids)})")

# People -> Courses (teaches)
for cid in new_course_ids:
    if new_person_ids:
        create_edge(random.choice(new_person_ids), cid, "teaches")
    if new_room_ids:
        create_edge(cid, random.choice(new_room_ids), "held_in")
progress(f"people->courses ({len(new_course_ids)})")

# Floors -> Network devices
for nid in new_network_ids:
    create_edge(random.choice(new_floor_ids), nid, "has_network")
progress(f"floors->network ({len(new_network_ids)})")

# Work orders -> targets (equipment or rooms)
for wid in new_workorder_ids:
    if random.random() < 0.7 and new_equip_ids:
        create_edge(wid, random.choice(new_equip_ids), "targets")
    elif new_room_ids:
        create_edge(wid, random.choice(new_room_ids), "targets")
    if new_person_ids and random.random() < 0.8:
        create_edge(random.choice(new_person_ids), wid, "assigned_to")
progress(f"workorders ({len(new_workorder_ids)})")

# Alerts -> sensors/equipment
for aid in new_alert_ids:
    if random.random() < 0.6 and new_sensor_ids:
        create_edge(random.choice(new_sensor_ids), aid, "triggered")
    elif new_equip_ids:
        create_edge(random.choice(new_equip_ids), aid, "triggered")
progress(f"alerts ({len(new_alert_ids)})")

# Cross-links: sensor reads_from equipment
for sid in random.sample(new_sensor_ids, min(len(new_sensor_ids), 1500)):
    if new_equip_ids:
        create_edge(sid, random.choice(new_equip_ids), "reads_from")
progress("sensor-equipment cross-links")

# Collaboration edges
for _ in range(min(800, len(new_person_ids))):
    if len(new_person_ids) >= 2:
        a, b = random.sample(new_person_ids, 2)
        create_edge(a, b, "collaborates_with")
progress("collaboration edges")

print(f"\n=== DONE: {edge_count:,} edges created ===")
