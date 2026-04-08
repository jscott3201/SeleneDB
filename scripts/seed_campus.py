#!/usr/bin/env python3
"""Seed a 14K+ node university campus dataset into a running Selene instance."""

import json
import random
import urllib.request
import time
import sys

URL = "http://localhost:8080/gql"
HEADERS = {"Content-Type": "application/json"}

created = 0
edge_count = 0

def gql(query, params=None):
    body = {"query": query}
    if params:
        body["parameters"] = params
    data = json.dumps(body).encode()
    req = urllib.request.Request(URL, data=data, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        return None

def insert_node(label, props):
    global created
    prop_str = ", ".join(f"{k}: ${k}" for k in props)
    q = f"INSERT (n:{label} {{{prop_str}}}) RETURN id(n) AS id"
    r = gql(q, props)
    if r and r.get("row_count", 0) > 0:
        created += 1
        return r["data"][0]["ID"]
    return None

def insert_edge(src_id, tgt_id, label, props=None):
    global edge_count
    q = f"MATCH (a) WHERE id(a) = $src MATCH (b) WHERE id(b) = $tgt INSERT (a)-[:{label}]->(b)"
    p = {"src": src_id, "tgt": tgt_id}
    r = gql(q, p)
    if r and r.get("status") == "00000":
        edge_count += 1

def progress(msg):
    print(f"  [{created:,} nodes / {edge_count:,} edges] {msg}")

# ── Domain data ──────────────────────────────────────────────
BUILDING_NAMES = [
    "Newton Hall", "Tesla Center", "Curie Science Building",
    "Turing Computing Center", "Da Vinci Arts Building",
    "Fleming Medical School", "Borges Library", "Hawking Research Lab",
    "Lovelace Data Center", "Olympus Sports Complex"
]

DEPARTMENTS = [
    "Computer Science", "Electrical Engineering", "Mechanical Engineering",
    "Physics", "Chemistry", "Biology", "Mathematics", "Philosophy",
    "History", "Economics", "Psychology", "Sociology", "English Literature",
    "Art & Design", "Music", "Architecture", "Civil Engineering",
    "Biomedical Engineering", "Environmental Science", "Data Science",
    "Neuroscience", "Astronomy", "Political Science", "Linguistics",
    "Materials Science", "Chemical Engineering", "Nursing",
    "Public Health", "Law", "Business Administration",
    "Information Systems", "Robotics", "Quantum Computing",
    "Marine Biology", "Geology", "Statistics", "Education",
    "Theater", "Communications", "Anthropology"
]

ROOM_TYPES = ["classroom", "lab", "office", "conference", "storage", "server_room", "workshop", "lounge"]
SENSOR_TYPES = ["temperature", "humidity", "co2", "occupancy", "light_level", "power_consumption", "noise_level"]
EQUIP_TYPES = ["hvac_unit", "lighting_system", "power_distribution", "fire_alarm", "security_camera", "ups_battery", "air_handler"]
FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack",
               "Karen", "Leo", "Mia", "Nate", "Olivia", "Pete", "Quinn", "Rosa", "Sam", "Tina",
               "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zane", "Ava", "Ben", "Clara", "Dan",
               "Elena", "Felix", "Gina", "Hugo", "Isla", "Jay", "Kira", "Liam", "Maya", "Noah",
               "Omar", "Pia", "Raj", "Sara", "Tom", "Ursula", "Vera", "Will", "Xena", "Yuri"]
LAST_NAMES = ["Smith", "Chen", "Patel", "Kim", "Garcia", "Muller", "Tanaka", "Silva", "Cohen", "Okafor",
              "Johnson", "Lee", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris",
              "Martin", "Thompson", "Moore", "Clark", "Lewis", "Walker", "Hall", "Allen", "Young", "King",
              "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts"]
ROLES = ["professor", "associate_professor", "assistant_professor", "lecturer",
         "phd_student", "masters_student", "undergrad", "postdoc",
         "admin_staff", "maintenance", "security", "it_support", "librarian"]
ALERT_TYPES = ["high_temperature", "low_battery", "motion_detected", "door_open",
               "power_outage", "water_leak", "air_quality_warning", "occupancy_exceeded",
               "equipment_fault", "network_offline", "fire_alarm", "hvac_failure"]
WORK_ORDER_TYPES = ["repair", "inspection", "replacement", "calibration", "cleaning", "upgrade", "installation"]
NETWORK_TYPES = ["wifi_ap", "switch", "router", "firewall", "server", "ups"]
COURSE_LEVELS = ["100", "200", "300", "400", "500", "600"]

random.seed(42)  # reproducible

print("=== Seeding 20K-node university campus dataset ===\n")

# ── Campus ───────────────────────────────────────────────────
campus_id = insert_node("Campus", {"name": "Meridian University", "city": "Austin", "state": "TX", "founded": 1965, "motto": "Knowledge through discovery"})
progress("Campus created")

# ── Buildings ────────────────────────────────────────────────
building_ids = []
for i, name in enumerate(BUILDING_NAMES):
    bid = insert_node("Building", {
        "name": name,
        "floors": random.randint(3, 8),
        "year_built": random.randint(1960, 2024),
        "area_sqft": random.randint(20000, 120000),
        "status": random.choice(["operational", "operational", "operational", "renovation", "planned"]),
    })
    building_ids.append(bid)
    insert_edge(campus_id, bid, "contains")
progress(f"{len(BUILDING_NAMES)} buildings")

# ── Departments ──────────────────────────────────────────────
dept_ids = []
for name in DEPARTMENTS:
    did = insert_node("Department", {
        "name": name,
        "budget": random.randint(500000, 15000000),
        "faculty_count": random.randint(5, 45),
        "student_count": random.randint(50, 800),
    })
    dept_ids.append(did)
    insert_edge(random.choice(building_ids), did, "houses")
progress(f"{len(DEPARTMENTS)} departments")

# ── Floors ───────────────────────────────────────────────────
floor_ids = []
for bi, bid in enumerate(building_ids):
    num_floors = random.randint(3, 7)
    for f in range(1, num_floors + 1):
        fid = insert_node("Floor", {
            "name": f"Floor {f}",
            "level": f,
            "building": BUILDING_NAMES[bi],
            "area_sqft": random.randint(3000, 18000),
        })
        floor_ids.append((fid, bi))
        insert_edge(bid, fid, "contains")
progress(f"{len(floor_ids)} floors")

# ── Rooms ────────────────────────────────────────────────────
room_ids = []
for fi, (fid, bi) in enumerate(floor_ids):
    num_rooms = random.randint(15, 25)
    for r in range(1, num_rooms + 1):
        rtype = random.choice(ROOM_TYPES)
        room_num = f"{fi+1}{r:02d}"
        rid = insert_node("Room", {
            "name": f"Room {room_num}",
            "room_number": room_num,
            "room_type": rtype,
            "capacity": random.randint(2, 60) if rtype in ("classroom", "lab", "conference") else random.randint(1, 4),
            "has_projector": random.choice([True, False]),
            "has_whiteboard": True,
            "floor_level": fi + 1,
        })
        room_ids.append((rid, fid, bi))
        insert_edge(fid, rid, "contains")
progress(f"{len(room_ids)} rooms")

# ── Sensors (bulk: 5 per room) ───────────────────────────────
sensor_ids = []
batch_size = len(room_ids)
for ri, (rid, fid, bi) in enumerate(room_ids):
    num_sensors = random.randint(4, 7)
    for s in range(num_sensors):
        stype = SENSOR_TYPES[s % len(SENSOR_TYPES)]
        sid = insert_node("Sensor", {
            "name": f"sensor-{ri+1:04d}-{stype[:4]}",
            "sensor_type": stype,
            "unit": {"temperature": "celsius", "humidity": "percent", "co2": "ppm",
                     "occupancy": "count", "light_level": "lux", "power_consumption": "watts",
                     "noise_level": "dB"}.get(stype, "unit"),
            "value": round(random.uniform(15, 95), 2),
            "status": random.choice(["online", "online", "online", "offline", "maintenance"]),
            "battery_pct": random.randint(10, 100),
            "firmware": f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
        })
        sensor_ids.append(sid)
        insert_edge(rid, sid, "monitors")
    if (ri + 1) % 200 == 0:
        progress(f"sensors through room {ri+1}/{len(room_ids)}")
progress(f"{len(sensor_ids)} sensors total")

# ── Equipment (3 per room) ──────────────────────────────────
equip_ids = []
for ri, (rid, fid, bi) in enumerate(room_ids):
    num_equip = random.randint(2, 4)
    for e in range(num_equip):
        etype = EQUIP_TYPES[e % len(EQUIP_TYPES)]
        eid = insert_node("Equipment", {
            "name": f"equip-{ri+1:04d}-{etype[:4]}",
            "equipment_type": etype,
            "manufacturer": random.choice(["Honeywell", "Siemens", "Johnson Controls", "Schneider", "Trane", "Carrier", "ABB"]),
            "model": f"M{random.randint(100,999)}",
            "install_date": f"20{random.randint(15,25):02d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "status": random.choice(["operational", "operational", "needs_maintenance", "offline"]),
            "power_rating_kw": round(random.uniform(0.5, 50.0), 1),
        })
        equip_ids.append(eid)
        insert_edge(rid, eid, "equipped_with")
    if (ri + 1) % 200 == 0:
        progress(f"equipment through room {ri+1}/{len(room_ids)}")
progress(f"{len(equip_ids)} equipment items")

# ── People ───────────────────────────────────────────────────
people_ids = []
target_people = 2000
for i in range(target_people):
    fname = random.choice(FIRST_NAMES)
    lname = random.choice(LAST_NAMES)
    role = random.choice(ROLES)
    pid = insert_node("Person", {
        "name": f"{fname} {lname}",
        "email": f"{fname.lower()}.{lname.lower()}{random.randint(1,999)}@meridian.edu",
        "role": role,
        "department": random.choice(DEPARTMENTS),
        "office_hours": random.choice(["MWF 10-12", "TTh 2-4", "MW 1-3", "F 9-11", "by appointment"]),
        "active": True,
    })
    people_ids.append(pid)
    # Assign to a department
    insert_edge(random.choice(dept_ids), pid, "employs" if role not in ("phd_student", "masters_student", "undergrad") else "enrolls")
    # Assign to a room (office or lab)
    if random.random() < 0.6:
        room_id = random.choice(room_ids)[0]
        insert_edge(pid, room_id, "occupies")
    if (i + 1) % 500 == 0:
        progress(f"people: {i+1}/{target_people}")
progress(f"{len(people_ids)} people")

# ── Courses ──────────────────────────────────────────────────
course_ids = []
course_names = [
    "Intro to", "Advanced", "Topics in", "Seminar on", "Principles of",
    "Foundations of", "Applied", "Computational", "Experimental", "Theory of"
]
for i in range(300):
    dept = random.choice(DEPARTMENTS)
    prefix = random.choice(course_names)
    level = random.choice(COURSE_LEVELS)
    cid = insert_node("Course", {
        "name": f"{prefix} {dept}",
        "code": f"{dept[:3].upper()}{level}",
        "department": dept,
        "credits": random.choice([3, 3, 3, 4, 4, 1]),
        "enrollment": random.randint(10, 200),
        "semester": random.choice(["Fall 2025", "Spring 2026", "Fall 2026"]),
    })
    course_ids.append(cid)
    # Taught by someone
    if people_ids:
        insert_edge(random.choice(people_ids), cid, "teaches")
    # In a room
    room_id = random.choice(room_ids)[0]
    insert_edge(cid, room_id, "held_in")
progress(f"{len(course_ids)} courses")

# ── Network Infrastructure ──────────────────────────────────
network_ids = []
for fi, (fid, bi) in enumerate(floor_ids):
    for n in range(random.randint(3, 6)):
        ntype = random.choice(NETWORK_TYPES)
        nid = insert_node("NetworkDevice", {
            "name": f"net-{fi+1:02d}-{n+1:02d}",
            "device_type": ntype,
            "ip_address": f"10.{bi+1}.{fi+1}.{n+10}",
            "mac_address": ":".join(f"{random.randint(0,255):02x}" for _ in range(6)),
            "status": random.choice(["online", "online", "online", "degraded"]),
            "bandwidth_mbps": random.choice([100, 1000, 10000]),
        })
        network_ids.append(nid)
        insert_edge(fid, nid, "has_network")
progress(f"{len(network_ids)} network devices")

# ── Work Orders ──────────────────────────────────────────────
wo_ids = []
for i in range(500):
    wtype = random.choice(WORK_ORDER_TYPES)
    priority = random.choice(["low", "medium", "medium", "high", "critical"])
    wid = insert_node("WorkOrder", {
        "title": f"{wtype.title()} request #{i+1}",
        "work_type": wtype,
        "priority": priority,
        "status": random.choice(["open", "in_progress", "completed", "completed", "completed"]),
        "created_date": f"2026-{random.randint(1,4):02d}-{random.randint(1,28):02d}",
        "estimated_hours": round(random.uniform(0.5, 16.0), 1),
    })
    wo_ids.append(wid)
    # Link to equipment or room
    if random.random() < 0.7 and equip_ids:
        insert_edge(wid, random.choice(equip_ids), "targets")
    else:
        insert_edge(wid, random.choice(room_ids)[0], "targets")
    # Assigned to a person
    if people_ids and random.random() < 0.8:
        insert_edge(random.choice(people_ids), wid, "assigned_to")
progress(f"{len(wo_ids)} work orders")

# ── Alerts ───────────────────────────────────────────────────
alert_ids = []
for i in range(600):
    atype = random.choice(ALERT_TYPES)
    aid = insert_node("Alert", {
        "alert_type": atype,
        "severity": random.choice(["info", "warning", "warning", "critical"]),
        "message": f"{atype.replace('_', ' ').title()} detected",
        "timestamp": f"2026-04-{random.randint(1,7):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z",
        "acknowledged": random.choice([True, True, False]),
        "resolved": random.choice([True, False, False]),
    })
    alert_ids.append(aid)
    # Link to sensor or equipment
    if random.random() < 0.6 and sensor_ids:
        insert_edge(random.choice(sensor_ids), aid, "triggered")
    elif equip_ids:
        insert_edge(random.choice(equip_ids), aid, "triggered")
    if (i + 1) % 200 == 0:
        progress(f"alerts: {i+1}/600")
progress(f"{len(alert_ids)} alerts")

# ── Cross-references: sensor-to-equipment links ─────────────
cross_edges = 0
for sid in random.sample(sensor_ids, min(len(sensor_ids), 1500)):
    if equip_ids:
        insert_edge(sid, random.choice(equip_ids), "reads_from")
        cross_edges += 1
progress(f"{cross_edges} sensor-equipment cross-links")

# ── Collaboration edges: person-to-person ────────────────────
collab_edges = 0
for _ in range(800):
    a, b = random.sample(people_ids, 2)
    insert_edge(a, b, "collaborates_with")
    collab_edges += 1
progress(f"{collab_edges} collaboration edges")

print(f"\n=== DONE: {created:,} nodes, {edge_count:,} edges created ===")
