"""Maritime search-and-rescue coordination service."""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "maritime_sar.db"
ACTIVE_INCIDENT = {"reported", "coordinating", "recovering"}
CLOSED_INCIDENT = {"closed", "cancelled", "duplicate", "merged"}
# 待确认的重复报警仍在处置中，允许继续挂区域、线索和资源
WORKABLE_INCIDENT = ACTIVE_INCIDENT | {"duplicate"}
MERGE_ITEM_FAILURE = {"unverified", "invalid"}


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def validate_position(lat: Any, lon: Any) -> tuple[float, float]:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError) as exc:
        raise DomainError("经纬度必须是数值") from exc
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise DomainError("经纬度超出有效范围")
    return lat, lon


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class MaritimeSARService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    vessel_name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    uncertainty_km REAL NOT NULL,
                    drift_direction REAL NOT NULL DEFAULT 0,
                    drift_speed_kn REAL NOT NULL DEFAULT 0,
                    sea_state INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'reported',
                    lead_org TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES incidents(id),
                    merge_batch_id TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    capabilities TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_kn REAL NOT NULL,
                    range_km REAL NOT NULL,
                    max_sea_state INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS search_areas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    origin_incident_id INTEGER,
                    code TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    center_lat REAL NOT NULL,
                    center_lon REAL NOT NULL,
                    radius_km REAL NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 3,
                    status TEXT NOT NULL DEFAULT 'planned',
                    assigned_asset_id INTEGER REFERENCES assets(id),
                    note TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS clues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    origin_incident_id INTEGER,
                    area_id INTEGER REFERENCES search_areas(id),
                    client_event_id TEXT NOT NULL UNIQUE,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unverified',
                    distance_from_incident_km REAL NOT NULL,
                    reporter TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    recorded_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    merged_at TEXT,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merge_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_batch_id TEXT NOT NULL UNIQUE,
                    duplicate_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    main_incident_id INTEGER NOT NULL REFERENCES incidents(id),
                    status TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    confirmed_at TEXT,
                    reverted_at TEXT,
                    reverted_by TEXT,
                    summary TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS merge_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES merge_batches(id),
                    item_type TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT '',
                    merged_at TEXT,
                    reverted_at TEXT,
                    UNIQUE(batch_id, item_type, item_id)
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER REFERENCES incidents(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_clues_incident ON clues(incident_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline(incident_id, id);
                CREATE INDEX IF NOT EXISTS idx_merge_items_batch ON merge_items(batch_id, status);
                """
            )
            # 兼容旧库：补齐合并流程需要的列
            existing = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(incidents)").fetchall()
            }
            if "merge_batch_id" not in existing:
                conn.execute("ALTER TABLE incidents ADD COLUMN merge_batch_id TEXT")
            area_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(search_areas)").fetchall()
            }
            if "origin_incident_id" not in area_cols:
                conn.execute("ALTER TABLE search_areas ADD COLUMN origin_incident_id INTEGER")
            if "merged_at" not in area_cols:
                conn.execute("ALTER TABLE search_areas ADD COLUMN merged_at TEXT")
            clue_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(clues)").fetchall()
            }
            if "origin_incident_id" not in clue_cols:
                conn.execute("ALTER TABLE clues ADD COLUMN origin_incident_id INTEGER")

    def _audit(self, conn: sqlite3.Connection, incident_id: int | None, actor: str, action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(incident_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (incident_id, actor, action, json_dump(details), utcnow()),
        )

    def create_incident(self, actor: str, role: str, code: str, vessel_name: str,
                        latitude: float, longitude: float, uncertainty_km: float,
                        sea_state: int, lead_org: str, drift_direction: float = 0,
                        drift_speed_kn: float = 0, description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "创建遇险事件")
        code, vessel_name, lead_org = code.strip(), vessel_name.strip(), lead_org.strip()
        if not code or not vessel_name or not lead_org:
            raise DomainError("事件编号、船名和负责机构不能为空")
        lat, lon = validate_position(latitude, longitude)
        try:
            uncertainty_km = float(uncertainty_km)
            sea_state = int(sea_state)
            drift_direction = float(drift_direction)
            drift_speed_kn = float(drift_speed_kn)
        except (TypeError, ValueError) as exc:
            raise DomainError("不确定半径、海况和漂移参数必须是数值") from exc
        if uncertainty_km <= 0 or uncertainty_km > 1000:
            raise DomainError("不确定半径应在 0 到 1000 公里之间")
        if not 0 <= sea_state <= 9 or drift_speed_kn < 0:
            raise DomainError("海况或漂移速度无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                "SELECT * FROM incidents WHERE vessel_name=? AND status IN ('reported','coordinating','recovering') ORDER BY id DESC",
                (vessel_name,),
            ).fetchall()
            duplicate_of = None
            for row in duplicate:
                if haversine_km(lat, lon, row["latitude"], row["longitude"]) <= max(20.0, uncertainty_km + row["uncertainty_km"]):
                    duplicate_of = row["id"]
                    break
            status = "duplicate" if duplicate_of else "reported"
            try:
                cur = conn.execute(
                    """INSERT INTO incidents(code,vessel_name,description,latitude,longitude,uncertainty_km,
                       drift_direction,drift_speed_kn,sea_state,status,lead_org,duplicate_of,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, vessel_name, description.strip(), lat, lon, uncertainty_km, drift_direction, drift_speed_kn,
                     sea_state, status, lead_org, duplicate_of, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("事件编号已存在", 409) from exc
            incident_id = int(cur.lastrowid)
            self._audit(conn, incident_id, actor, "incident.reported", {"duplicate_of": duplicate_of})
            if duplicate_of:
                self._audit(conn, duplicate_of, actor, "incident.duplicate_detected", {"duplicate_incident": code})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def list_assets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM assets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def add_asset(self, actor: str, role: str, name: str, kind: str,
                  capabilities: list[str], latitude: float, longitude: float,
                  speed_kn: float, range_km: float, max_sea_state: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "登记搜救资源")
        lat, lon = validate_position(latitude, longitude)
        name, kind = name.strip(), kind.strip()
        caps = sorted({str(item).strip() for item in capabilities if str(item).strip()})
        if not name or not kind or not caps:
            raise DomainError("资源名称、类型和能力不能为空")
        try:
            speed_kn, range_km, max_sea_state = float(speed_kn), float(range_km), int(max_sea_state)
        except (TypeError, ValueError) as exc:
            raise DomainError("速度和航程参数必须是数值") from exc
        if speed_kn <= 0 or range_km <= 0 or not 0 <= max_sea_state <= 9:
            raise DomainError("速度、航程或适用海况无效")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO assets(name,kind,capabilities,latitude,longitude,speed_kn,range_km,max_sea_state,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (name, kind, json_dump(caps), lat, lon, speed_kn, range_km, max_sea_state, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("资源名称已存在", 409) from exc
            self._audit(conn, None, actor, "asset.registered", {"asset_id": cur.lastrowid, "name": name})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_search_area(self, actor: str, role: str, incident_id: int, code: str,
                           kind: str, center_lat: float, center_lon: float,
                           radius_km: float, priority: int = 3, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "创建搜索区域")
        lat, lon = validate_position(center_lat, center_lon)
        kind, code = kind.strip(), code.strip()
        if not kind or not code:
            raise DomainError("区域类型和编号不能为空")
        try:
            radius_km, priority = float(radius_km), int(priority)
        except (TypeError, ValueError) as exc:
            raise DomainError("半径和优先级必须是数值") from exc
        if radius_km <= 0 or not 1 <= priority <= 5:
            raise DomainError("搜索半径或优先级无效")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in WORKABLE_INCIDENT:
                raise DomainError("当前事件不能创建搜索区域", 409)
            try:
                cur = conn.execute(
                    """INSERT INTO search_areas(incident_id,code,kind,center_lat,center_lon,radius_km,priority,note,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (incident_id, code, kind, lat, lon, radius_km, priority, note.strip(), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("搜索区域编号已存在", 409) from exc
            self._audit(conn, incident_id, actor, "area.created", {"area_id": cur.lastrowid, "code": code})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign_area(self, actor: str, role: str, area_id: int, asset_id: int,
                    expected_asset_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "分配搜索任务")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not area or not asset:
                raise DomainError("搜索区域或资源不存在", 404)
            if area["assigned_asset_id"] is not None:
                raise DomainError("搜索区域已经分配", 409)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (area["incident_id"],)).fetchone()
            if not incident or incident["status"] not in WORKABLE_INCIDENT:
                raise DomainError("事件当前不可分配", 409)
            if expected_asset_version is not None and asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] != "available":
                raise DomainError("资源当前不可用", 409)
            if incident["sea_state"] > asset["max_sea_state"]:
                raise DomainError("海况超出资源能力", 409)
            capabilities = json.loads(asset["capabilities"])
            if area["kind"] not in capabilities:
                raise DomainError("资源不具备该搜索区域能力", 409)
            distance = haversine_km(asset["latitude"], asset["longitude"], area["center_lat"], area["center_lon"])
            if distance > asset["range_km"]:
                raise DomainError("搜索区域超出资源航程", 409)
            now = utcnow()
            changed = conn.execute(
                "UPDATE assets SET status='assigned',version=version+1,updated_at=? WHERE id=? AND status='available' AND version=?",
                (now, asset_id, asset["version"]),
            )
            if changed.rowcount != 1:
                raise DomainError("资源已被其他任务占用", 409)
            conn.execute(
                "UPDATE search_areas SET assigned_asset_id=?,status='assigned',version=version+1,updated_at=? WHERE id=?",
                (asset_id, now, area_id),
            )
            self._audit(conn, area["incident_id"], actor, "area.assigned", {"area_id": area_id, "asset_id": asset_id, "distance_km": round(distance, 2)})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def record_clue(self, actor: str, role: str, incident_id: int, client_event_id: str,
                    latitude: float, longitude: float, confidence: float, source: str,
                    area_id: int | None = None, details: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator", "field"}, "记录搜索线索")
        lat, lon = validate_position(latitude, longitude)
        event_id, source = client_event_id.strip(), source.strip()
        if not event_id or not source:
            raise DomainError("事件幂等编号和线索来源不能为空")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError) as exc:
            raise DomainError("线索置信度必须是数值") from exc
        if not 0 <= confidence <= 1:
            raise DomainError("置信度应在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
            if existing:
                return dict(existing)
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] not in WORKABLE_INCIDENT:
                raise DomainError("已结束事件不能新增线索", 409)
            if area_id is not None:
                area = conn.execute("SELECT * FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)).fetchone()
                if not area:
                    raise DomainError("搜索区域不属于该事件", 409)
            distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
            status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
            cur = conn.execute(
                """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                   distance_from_incident_km,reporter,details,recorded_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (incident_id, area_id, event_id, lat, lon, confidence, source, status, distance, actor, details.strip(), utcnow()),
            )
            self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (cur.lastrowid,)).fetchone())

    def verify_clue(self, actor: str, role: str, clue_id: int, status: str,
                    expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "analyst"}, "核验线索")
        if status not in {"verified", "rejected", "unverified"}:
            raise DomainError("线索状态无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            clue = conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone()
            if not clue:
                raise DomainError("线索不存在", 404)
            if status == "verified" and clue["status"] == "invalid" and role != "coordinator":
                raise DomainError("异常位置线索只能由协调员确认", 403)
            conn.execute("UPDATE clues SET status=?,merged_at=? WHERE id=?", (status, utcnow(), clue_id))
            self._audit(conn, clue["incident_id"], actor, "clue.reviewed", {"clue_id": clue_id, "status": status})
            return dict(conn.execute("SELECT * FROM clues WHERE id=?", (clue_id,)).fetchone())

    def withdraw_asset(self, actor: str, role: str, asset_id: int, reason: str,
                       expected_asset_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "撤回资源")
        if not reason.strip():
            raise DomainError("撤回原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            asset = conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
            if not asset:
                raise DomainError("资源不存在", 404)
            if asset["version"] != int(expected_asset_version):
                raise DomainError("资源状态已变化，请刷新后重试", 409)
            if asset["status"] == "available":
                raise DomainError("资源当前未分配", 409)
            now = utcnow()
            areas = conn.execute("SELECT id,incident_id FROM search_areas WHERE assigned_asset_id=? AND status IN ('assigned','active')", (asset_id,)).fetchall()
            for area in areas:
                conn.execute("UPDATE search_areas SET assigned_asset_id=NULL,status='planned',version=version+1,updated_at=? WHERE id=?", (now, area["id"]))
                self._audit(conn, area["incident_id"], actor, "area.unassigned", {"area_id": area["id"], "reason": reason.strip()})
            conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (now, asset_id))
            self._audit(conn, None, actor, "asset.withdrawn", {"asset_id": asset_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone())

    def transfer_incident(self, actor: str, role: str, incident_id: int, new_org: str,
                          expected_version: int, note: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "移交事件")
        new_org = new_org.strip()
        if not new_org:
            raise DomainError("接收机构不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["status"] in CLOSED_INCIDENT:
                raise DomainError("已结束事件不能移交", 409)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            conn.execute(
                "UPDATE incidents SET lead_org=?,version=version+1,updated_at=? WHERE id=? AND version=?",
                (new_org, utcnow(), incident_id, expected_version),
            )
            self._audit(conn, incident_id, actor, "incident.transferred", {"from": incident["lead_org"], "to": new_org, "note": note.strip()})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def complete_area(self, actor: str, role: str, area_id: int, outcome: str,
                      expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束搜索区域")
        if outcome not in {"completed", "abandoned"}:
            raise DomainError("区域结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            area = conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone()
            if not area:
                raise DomainError("搜索区域不存在", 404)
            if area["status"] in {"completed", "abandoned"}:
                raise DomainError("搜索区域已经结束", 409)
            if expected_version is not None and area["version"] != int(expected_version):
                raise DomainError("搜索区域已变化，请刷新后重试", 409)
            if area["assigned_asset_id"] is not None:
                conn.execute("UPDATE assets SET status='available',version=version+1,updated_at=? WHERE id=?", (utcnow(), area["assigned_asset_id"]))
            conn.execute("UPDATE search_areas SET status=?,assigned_asset_id=NULL,version=version+1,updated_at=? WHERE id=?", (outcome, utcnow(), area_id))
            self._audit(conn, area["incident_id"], actor, "area." + outcome, {"area_id": area_id})
            return dict(conn.execute("SELECT * FROM search_areas WHERE id=?", (area_id,)).fetchone())

    def close_incident(self, actor: str, role: str, incident_id: int, outcome: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"coordinator"}, "结束事件")
        if outcome not in {"resolved", "cancelled", "false_alarm"}:
            raise DomainError("结束结论无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
            if not incident:
                raise DomainError("事件不存在", 404)
            if incident["version"] != int(expected_version):
                raise DomainError("事件已变化，请刷新后重试", 409)
            active_area = conn.execute(
                "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=? AND status IN ('planned','assigned','active')",
                (incident_id,),
            ).fetchone()["c"]
            if active_area and outcome != "false_alarm":
                raise DomainError("仍有未结束搜索区域，不能关闭事件", 409)
            status = "closed" if outcome == "resolved" else "cancelled"
            conn.execute("UPDATE incidents SET status=?,version=version+1,updated_at=? WHERE id=?", (status, utcnow(), incident_id))
            self._audit(conn, incident_id, actor, "incident.closed", {"outcome": outcome})
            return dict(conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone())

    def _upsert_merge_item(self, conn: sqlite3.Connection, batch_id: int, item_type: str,
                           item_id: int, status: str, error: str = "", merged_at: str | None = None,
                           reverted_at: str | None = None) -> None:
        conn.execute(
            """INSERT INTO merge_items(batch_id,item_type,item_id,status,error,merged_at,reverted_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(batch_id,item_type,item_id) DO UPDATE SET
                   status=excluded.status,
                   error=excluded.error,
                   merged_at=COALESCE(excluded.merged_at, merge_items.merged_at),
                   reverted_at=excluded.reverted_at""",
            (batch_id, item_type, item_id, status, error, merged_at, reverted_at),
        )

    def confirm_duplicate_merge(self, actor: str, role: str, duplicate_incident_id: int,
                                main_incident_id: int | None = None,
                                client_batch_id: str = "", note: str = "") -> dict[str, Any]:
        """确认重复报警：区域与线索并入主事件。同一 client_batch_id 重试为可恢复续跑。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "确认重复报警合并")
        batch_code = client_batch_id.strip()
        if not batch_code:
            raise DomainError("缺少合并批次编号")
        if not note.strip():
            raise DomainError("确认合并必须填写判断说明")
        try:
            dup_id = int(duplicate_incident_id)
        except (TypeError, ValueError) as exc:
            raise DomainError("重复报警编号无效") from exc
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            batch_row = conn.execute("SELECT * FROM merge_batches WHERE client_batch_id=?", (batch_code,)).fetchone()
            if batch_row:
                if batch_row["duplicate_incident_id"] != dup_id:
                    raise DomainError("批次编号已用于其他报警", 409)
                if batch_row["status"] == "reverted":
                    raise DomainError("批次已撤销，请使用新批次重新确认", 409)
                if batch_row["status"] == "merged":
                    # 完整幂等：同一批次重复提交直接返回既有结果
                    summary = json.loads(batch_row["summary"])
                    return {"batch_id": batch_code, "idempotent": True, "status": batch_row["status"], "summary": summary}
                batch_id = batch_row["id"]
                target_id = batch_row["main_incident_id"]
                dup = conn.execute("SELECT * FROM incidents WHERE id=?", (dup_id,)).fetchone()
                if not dup:
                    raise DomainError("重复报警不存在", 404)
                resume = True
            else:
                dup = conn.execute("SELECT * FROM incidents WHERE id=?", (dup_id,)).fetchone()
                if not dup:
                    raise DomainError("重复报警不存在", 404)
                if dup["status"] != "duplicate":
                    raise DomainError("该事件不是待确认的重复报警", 409)
                if main_incident_id is None:
                    target_id = dup["duplicate_of"]
                    if target_id is None:
                        raise DomainError("该报警没有可并入的主事件，请指定 main_incident_id")
                else:
                    try:
                        target_id = int(main_incident_id)
                    except (TypeError, ValueError) as exc:
                        raise DomainError("主事件编号无效") from exc
                main = conn.execute("SELECT * FROM incidents WHERE id=?", (target_id,)).fetchone()
                if not main:
                    raise DomainError("主事件不存在", 404)
                if target_id == dup_id:
                    raise DomainError("主事件不能是报警自身")
                if main["status"] not in WORKABLE_INCIDENT:
                    raise DomainError("主事件已结束，不能并入", 409)
                if main["vessel_name"] != dup["vessel_name"]:
                    raise DomainError("只能并入同船报警")
                now = utcnow()
                # 抢占：把待确认报警从 duplicate 改成 merged，两名值班员同时提交只有一方能改到
                changed = conn.execute(
                    "UPDATE incidents SET status='merged',merge_batch_id=?,version=version+1,updated_at=? WHERE id=? AND status='duplicate'",
                    (batch_code, now, dup_id),
                )
                if changed.rowcount != 1:
                    raise DomainError("重复报警已被其他值班员确认", 409)
                cur = conn.execute(
                    """INSERT INTO merge_batches(client_batch_id,duplicate_incident_id,main_incident_id,
                       status,actor,note,created_at,confirmed_at,summary)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (batch_code, dup_id, target_id, "partial", actor, note.strip(), now, now, "{}"),
                )
                batch_id = int(cur.lastrowid)
                self._audit(conn, dup_id, actor, "duplicate.merge_confirmed",
                            {"batch_id": batch_code, "main_incident_id": target_id, "note": note.strip()})
                self._audit(conn, target_id, actor, "incident.merge_target",
                            {"duplicate_incident": dup["code"], "batch_id": batch_code})
                resume = False
            return self._process_merge(conn, batch_id, batch_code, dup_id, target_id, actor, note, resume)

    def _process_merge(self, conn: sqlite3.Connection, batch_id: int, batch_code: str,
                       dup_id: int, target_id: int, actor: str, note: str, resume: bool) -> dict[str, Any]:
        main = conn.execute("SELECT * FROM incidents WHERE id=?", (target_id,)).fetchone()
        if not main:
            raise DomainError("主事件不存在", 404)
        now = utcnow()
        handled = {
            (row["item_type"], row["item_id"]): row
            for row in conn.execute("SELECT * FROM merge_items WHERE batch_id=?", (batch_id,)).fetchall()
        }
        merged_ids: list[dict[str, Any]] = []
        areas = conn.execute("SELECT * FROM search_areas WHERE incident_id=? ORDER BY id", (dup_id,)).fetchall()
        for area in areas:
            prior = handled.get(("area", area["id"]))
            if prior and prior["status"] == "merged":
                merged_ids.append({"item_type": "area", "item_id": area["id"], "status": "merged", "idempotent": True})
                continue
            savepoint = "merge_area_%s" % area["id"]
            conn.execute("SAVEPOINT %s" % savepoint)
            try:
                conn.execute(
                    """UPDATE search_areas SET incident_id=?,origin_incident_id=COALESCE(origin_incident_id,?),
                       version=version+1,updated_at=?,merged_at=? WHERE id=?""",
                    (target_id, dup_id, now, now, area["id"]),
                )
            except sqlite3.Error as exc:  # 理论上的约束失败也不能炸掉整批
                conn.execute("ROLLBACK TO SAVEPOINT %s" % savepoint)
                conn.execute("RELEASE SAVEPOINT %s" % savepoint)
                self._upsert_merge_item(conn, batch_id, "area", area["id"], "failed", str(exc))
                merged_ids.append({"item_type": "area", "item_id": area["id"], "status": "failed", "error": str(exc)})
                continue
            conn.execute("RELEASE SAVEPOINT %s" % savepoint)
            self._upsert_merge_item(conn, batch_id, "area", area["id"], "merged", merged_at=now)
            self._audit(conn, target_id, actor, "area.merged",
                        {"area_id": area["id"], "code": area["code"], "from_incident_id": dup_id,
                         "batch_id": batch_code})
            merged_ids.append({"item_type": "area", "item_id": area["id"], "status": "merged"})

        clues = conn.execute("SELECT * FROM clues WHERE incident_id=? ORDER BY id", (dup_id,)).fetchall()
        for clue in clues:
            prior = handled.get(("clue", clue["id"]))
            if prior and prior["status"] == "merged":
                merged_ids.append({"item_type": "clue", "item_id": clue["id"], "status": "merged", "idempotent": True})
                continue
            savepoint = "merge_clue_%s" % clue["id"]
            conn.execute("SAVEPOINT %s" % savepoint)
            try:
                area_id = clue["area_id"]
                if area_id is not None:
                    linked = conn.execute("SELECT incident_id FROM search_areas WHERE id=?", (area_id,)).fetchone()
                    if not linked or linked["incident_id"] not in (target_id, dup_id):
                        raise DomainError("线索关联的搜索区域属于其他事件")
                distance = haversine_km(main["latitude"], main["longitude"], clue["latitude"], clue["longitude"])
                # 人工核验结论保持不变；系统初判随主事件位置重新评估
                if clue["status"] in MERGE_ITEM_FAILURE:
                    new_status = "unverified" if distance <= main["uncertainty_km"] * 3 else "invalid"
                else:
                    new_status = clue["status"]
                conn.execute(
                    """UPDATE clues SET incident_id=?,origin_incident_id=COALESCE(origin_incident_id,?),
                       area_id=?,distance_from_incident_km=?,status=?,merged_at=? WHERE id=?""",
                    (target_id, dup_id, area_id, distance, new_status, now, clue["id"]),
                )
            except (DomainError, sqlite3.Error) as exc:
                conn.execute("ROLLBACK TO SAVEPOINT %s" % savepoint)
                conn.execute("RELEASE SAVEPOINT %s" % savepoint)
                self._upsert_merge_item(conn, batch_id, "clue", clue["id"], "failed", str(exc))
                merged_ids.append({"item_type": "clue", "item_id": clue["id"], "status": "failed", "error": str(exc)})
                continue
            conn.execute("RELEASE SAVEPOINT %s" % savepoint)
            self._upsert_merge_item(conn, batch_id, "clue", clue["id"], "merged", merged_at=now)
            self._audit(conn, target_id, actor, "clue.merged",
                        {"clue_id": clue["id"], "from_incident_id": dup_id, "batch_id": batch_code})
            merged_ids.append({"item_type": "clue", "item_id": clue["id"], "status": "merged"})

        items = conn.execute("SELECT * FROM merge_items WHERE batch_id=?", (batch_id,)).fetchall()
        merged_count = sum(1 for row in items if row["status"] == "merged")
        failed = [dict(item_type=row["item_type"], item_id=row["item_id"], error=row["error"])
                  for row in items if row["status"] == "failed"]
        remaining_areas = conn.execute(
            "SELECT COUNT(*) AS c FROM search_areas WHERE incident_id=?", (dup_id,)
        ).fetchone()["c"]
        remaining_clues = conn.execute(
            "SELECT COUNT(*) AS c FROM clues WHERE incident_id=?", (dup_id,)
        ).fetchone()["c"]
        if failed or remaining_areas or remaining_clues:
            batch_status = "partial" if merged_count else "failed"
        else:
            batch_status = "merged"
            conn.execute("UPDATE merge_batches SET status='merged' WHERE id=?", (batch_id,))
        if not resume and merged_count == 0:
            # 首次尝试一条都没并入：释放抢占，报警回到待确认状态，允许按同批次重试
            conn.execute(
                "UPDATE incidents SET status='duplicate',merge_batch_id=NULL,version=version+1,updated_at=? WHERE id=?",
                (now, dup_id),
            )
            conn.execute("UPDATE merge_batches SET status='failed' WHERE id=?", (batch_id,))
            batch_status = "failed"
            self._audit(conn, dup_id, actor, "duplicate.merge_failed",
                        {"batch_id": batch_code, "failed": failed})
        else:
            self._audit(conn, target_id, actor, "duplicate.merge_progress",
                        {"batch_id": batch_code, "status": batch_status, "merged": merged_count, "failed": len(failed)})
        asset_rows = conn.execute(
            """SELECT DISTINCT a.* FROM assets a JOIN search_areas s ON s.assigned_asset_id=a.id
               WHERE s.incident_id=? AND s.assigned_asset_id IS NOT NULL""",
            (target_id,),
        ).fetchall()
        summary = {
            "duplicate_incident_id": dup_id,
            "main_incident_id": target_id,
            "merged": merged_count,
            "failed": failed,
            "items": [dict(item_type=row["item_type"], item_id=row["item_id"], status=row["status"],
                           error=row["error"]) for row in items],
            "assets_following": [row["id"] for row in asset_rows],
        }
        conn.execute("UPDATE merge_batches SET status=?,summary=? WHERE id=?",
                     (batch_status, json_dump(summary), batch_id))
        return {"batch_id": batch_code, "idempotent": False, "status": batch_status, "summary": summary}

    def revert_duplicate_merge(self, actor: str, role: str, client_batch_id: str,
                               reason: str) -> dict[str, Any]:
        """撤销合并：把仍属于该报警的区域/线索放回；已被拆走或挂到别处的内容不动。"""
        actor = clean_actor(actor)
        require_role(role, {"coordinator", "operator"}, "撤销重复报警合并")
        if not reason.strip():
            raise DomainError("撤销原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute("SELECT * FROM merge_batches WHERE client_batch_id=?", (client_batch_id,)).fetchone()
            if not batch:
                raise DomainError("合并批次不存在", 404)
            if batch["status"] == "reverted":
                summary = json.loads(batch["summary"])
                return {"batch_id": client_batch_id, "idempotent": True, "status": "reverted", "summary": summary}
            dup_id = batch["duplicate_incident_id"]
            target_id = batch["main_incident_id"]
            now = utcnow()
            target = conn.execute("SELECT * FROM incidents WHERE id=?", (target_id,)).fetchone()
            reopened = False
            link_back = True
            if target is None or target["status"] == "merged":
                # 主事件自身也已被并走：不能往它身上挂，报警独立恢复为待处理
                link_back = False
            elif target["status"] in {"closed", "cancelled"}:
                # 主事件已关闭，先转待处理
                conn.execute(
                    "UPDATE incidents SET status='reported',version=version+1,updated_at=? WHERE id=?",
                    (now, target_id),
                )
                reopened = True
                self._audit(conn, target_id, actor, "incident.reopened_for_revert",
                            {"batch_id": client_batch_id, "reason": reason.strip()})
            reverted_items: list[dict[str, Any]] = []
            detached_items: list[dict[str, Any]] = []
            item_rows = conn.execute(
                "SELECT * FROM merge_items WHERE batch_id=? AND status='merged' ORDER BY id", (batch["id"],)
            ).fetchall()
            # 先放回区域，再处理线索，避免线索挂回一个还在别处的区域
            for row in item_rows:
                if row["item_type"] != "area":
                    continue
                area = conn.execute("SELECT * FROM search_areas WHERE id=?", (row["item_id"],)).fetchone()
                if not area or area["incident_id"] != target_id:
                    detached_items.append({"item_type": "area", "item_id": row["item_id"]})
                    self._upsert_merge_item(conn, batch["id"], "area", row["item_id"], "detached",
                                            reverted_at=now)
                    continue
                conn.execute(
                    "UPDATE search_areas SET incident_id=?,version=version+1,updated_at=? WHERE id=?",
                    (dup_id, now, area["id"]),
                )
                self._upsert_merge_item(conn, batch["id"], "area", area["id"], "reverted",
                                        reverted_at=now)
                self._audit(conn, dup_id, actor, "area.reverted",
                            {"area_id": area["id"], "code": area["code"], "from_incident_id": target_id,
                             "batch_id": client_batch_id})
                reverted_items.append({"item_type": "area", "item_id": area["id"]})
            for row in item_rows:
                if row["item_type"] != "clue":
                    continue
                clue = conn.execute("SELECT * FROM clues WHERE id=?", (row["item_id"],)).fetchone()
                if not clue or clue["incident_id"] != target_id:
                    detached_items.append({"item_type": "clue", "item_id": row["item_id"]})
                    self._upsert_merge_item(conn, batch["id"], "clue", row["item_id"], "detached",
                                            reverted_at=now)
                    continue
                area_id = clue["area_id"]
                if area_id is not None:
                    linked = conn.execute("SELECT incident_id FROM search_areas WHERE id=?", (area_id,)).fetchone()
                    if not linked or linked["incident_id"] not in (dup_id, target_id):
                        detached_items.append({"item_type": "clue", "item_id": clue["id"]})
                        self._upsert_merge_item(conn, batch["id"], "clue", clue["id"], "detached",
                                                reverted_at=now)
                        continue
                conn.execute(
                    "UPDATE clues SET incident_id=?,area_id=? WHERE id=?",
                    (dup_id, area_id, clue["id"]),
                )
                self._upsert_merge_item(conn, batch["id"], "clue", clue["id"], "reverted",
                                        reverted_at=now)
                self._audit(conn, dup_id, actor, "clue.reverted",
                            {"clue_id": clue["id"], "from_incident_id": target_id,
                             "batch_id": client_batch_id})
                reverted_items.append({"item_type": "clue", "item_id": clue["id"]})
            if link_back and (reopened or (target is not None and target["status"] in WORKABLE_INCIDENT)):
                new_status, new_dup_of = "duplicate", target_id
            else:
                new_status, new_dup_of = "reported", None
            conn.execute(
                "UPDATE incidents SET status=?,duplicate_of=?,version=version+1,updated_at=? WHERE id=?",
                (new_status, new_dup_of, now, dup_id),
            )
            summary = {
                "duplicate_incident_id": dup_id,
                "main_incident_id": target_id,
                "reverted": reverted_items,
                "detached": detached_items,
                "main_reopened": reopened,
                "restored_status": new_status,
            }
            conn.execute(
                "UPDATE merge_batches SET status='reverted',reverted_at=?,reverted_by=?,summary=? WHERE id=?",
                (now, actor, json_dump(summary), batch["id"]),
            )
            self._audit(conn, dup_id, actor, "duplicate.merge_reverted",
                        {"batch_id": client_batch_id, "reason": reason.strip(),
                         "reverted": len(reverted_items), "detached": len(detached_items),
                         "main_reopened": reopened, "restored_status": new_status})
            return {"batch_id": client_batch_id, "idempotent": False, "status": "reverted", "summary": summary}

    def merge_offline_batch(self, actor: str, role: str, client_batch_id: str,
                            events: list[dict[str, Any]]) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"operator", "field", "coordinator"}, "合并离线记录")
        batch_id = client_batch_id.strip()
        if not batch_id or not isinstance(events, list):
            raise DomainError("批次编号和事件列表不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM offline_batches WHERE client_batch_id=?", (batch_id,)).fetchone()
            if existing:
                return {"batch_id": batch_id, "idempotent": True, "status": existing["status"], "summary": json.loads(existing["summary"])}
            results = []
            for event in events:
                event_id = str(event.get("client_event_id", "")).strip()
                try:
                    if not event_id:
                        raise DomainError("离线事件缺少 client_event_id")
                    if event.get("type") == "clue":
                        existing_clue = conn.execute("SELECT id FROM clues WHERE client_event_id=?", (event_id,)).fetchone()
                        if existing_clue:
                            results.append({"client_event_id": event_id, "status": "merged", "record_id": existing_clue["id"], "idempotent": True})
                            continue
                        incident_id = int(event["incident_id"])
                        lat, lon = validate_position(event["latitude"], event["longitude"])
                        confidence = float(event["confidence"])
                        if not 0 <= confidence <= 1:
                            raise DomainError("置信度应在 0 到 1 之间")
                        incident = conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
                        if not incident:
                            raise DomainError("事件不存在", 404)
                        if incident["status"] not in WORKABLE_INCIDENT:
                            raise DomainError("已结束事件不能新增线索", 409)
                        area_id = event.get("area_id")
                        if area_id is not None and not conn.execute(
                            "SELECT 1 FROM search_areas WHERE id=? AND incident_id=?", (area_id, incident_id)
                        ).fetchone():
                            raise DomainError("搜索区域不属于该事件", 409)
                        distance = haversine_km(incident["latitude"], incident["longitude"], lat, lon)
                        status = "unverified" if distance <= incident["uncertainty_km"] * 3 else "invalid"
                        cur = conn.execute(
                            """INSERT INTO clues(incident_id,area_id,client_event_id,latitude,longitude,confidence,source,status,
                               distance_from_incident_km,reporter,details,recorded_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (incident_id, area_id, event_id, lat, lon, confidence,
                             str(event.get("source", "offline")).strip(), status, distance, actor,
                             str(event.get("details", "")).strip(), utcnow()),
                        )
                        self._audit(conn, incident_id, actor, "clue.recorded", {"clue_id": cur.lastrowid, "status": status, "event_id": event_id})
                        results.append({"client_event_id": event_id, "status": "merged", "record_id": cur.lastrowid})
                    elif event.get("type") == "timeline":
                        incident_id = int(event["incident_id"])
                        if not conn.execute("SELECT 1 FROM incidents WHERE id=?", (incident_id,)).fetchone():
                            raise DomainError("事件不存在", 404)
                        self._audit(conn, incident_id, actor, event.get("action", "offline.note"), event.get("details", {}))
                        results.append({"client_event_id": event_id, "status": "merged", "record_id": None})
                    else:
                        raise DomainError("不支持的离线事件类型")
                except (DomainError, KeyError, TypeError, ValueError) as exc:
                    results.append({"client_event_id": event_id, "status": "rejected", "error": str(exc)})
            summary = {"accepted": sum(1 for item in results if item["status"] == "merged"), "rejected": sum(1 for item in results if item["status"] == "rejected"), "events": results}
            now = utcnow()
            conn.execute(
                "INSERT INTO offline_batches(client_batch_id,actor,status,received_at,merged_at,summary) VALUES(?,?,?,?,?,?)",
                (batch_id, actor, "merged", now, now, json_dump(summary)),
            )
            self._audit(conn, None, actor, "offline.batch_merged", {"batch_id": batch_id, **{k: summary[k] for k in ("accepted", "rejected")}})
            return {"batch_id": batch_id, "idempotent": False, "status": "merged", "summary": summary}

    def state(self, actor: str = "", role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            incidents = [dict(r) for r in conn.execute("SELECT * FROM incidents ORDER BY id DESC").fetchall()]
            areas = [dict(r) for r in conn.execute("SELECT * FROM search_areas ORDER BY priority,id").fetchall()]
            clues = [dict(r) for r in conn.execute("SELECT * FROM clues ORDER BY id DESC LIMIT 200").fetchall()]
            assets = [dict(r) for r in conn.execute("SELECT * FROM assets ORDER BY id").fetchall()]
            merge_batches = [
                {**dict(r), "summary": json.loads(r["summary"] or "{}")}
                for r in conn.execute("SELECT * FROM merge_batches ORDER BY id DESC").fetchall()
            ]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 300").fetchall()]
        return {"incidents": incidents, "assets": assets, "search_areas": areas, "clues": clues,
                "merge_batches": merge_batches, "timeline": timeline}

    def incident_timeline(self, incident_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM timeline WHERE incident_id=? ORDER BY id", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        incident = self.create_incident("coord-demo", "coordinator", "SAR-2026-001", "远星号", 31.2, 122.5, 15.0, 3, "东海搜救中心", description="演示遇险事件")
        self.add_asset("coord-demo", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 22.0, 180.0, 6)
        self.add_asset("coord-demo", "coordinator", "救助直升机", "aircraft", ["air", "night"], 30.8, 122.1, 180.0, 260.0, 5)
        self.create_search_area("coord-demo", "coordinator", incident["id"], "AREA-A", "surface", 31.2, 122.5, 20.0, 1, "首要搜索区")
        return {"seeded": True, "incident_id": incident["id"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: MaritimeSARService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _actor(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "viewer")

    def _json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise DomainError("Content-Length 无效") from exc
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(data, dict):
            raise DomainError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/health":
                self._send(200, {"status": "ok", "service": "maritime-sar"})
                return
            if path == "/api/state":
                self._send(200, self.service.state(*self._actor()))
                return
            if path.startswith("/api/incidents/") and path.endswith("/timeline"):
                incident_id = int(path.split("/")[3])
                self._send(200, {"timeline": self.service.incident_timeline(incident_id)})
                return
            self._send(404, {"error": "接口不存在"})
        except (DomainError, ValueError) as exc:
            self._send(getattr(exc, "status", 400), {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            data, (actor, role) = self._json(), self._actor()
            if path == "/api/incidents":
                result = self.service.create_incident(actor, role, **data)
            elif path == "/api/assets":
                result = self.service.add_asset(actor, role, **data)
            elif path == "/api/areas":
                result = self.service.create_search_area(actor, role, **data)
            elif path == "/api/assignments":
                result = self.service.assign_area(actor, role, **data)
            elif path == "/api/clues":
                result = self.service.record_clue(actor, role, **data)
            elif path == "/api/clues/verify":
                result = self.service.verify_clue(actor, role, **data)
            elif path == "/api/assets/withdraw":
                result = self.service.withdraw_asset(actor, role, **data)
            elif path == "/api/areas/complete":
                result = self.service.complete_area(actor, role, **data)
            elif path == "/api/incidents/transfer":
                result = self.service.transfer_incident(actor, role, **data)
            elif path == "/api/incidents/close":
                result = self.service.close_incident(actor, role, **data)
            elif path == "/api/offline/batch":
                result = self.service.merge_offline_batch(actor, role, **data)
            elif path == "/api/incidents/merge":
                result = self.service.confirm_duplicate_merge(actor, role, **data)
            elif path == "/api/incidents/merge/revert":
                result = self.service.revert_duplicate_merge(actor, role, **data)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: MaritimeSARService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Maritime SAR service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="海上搜救协调服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8206)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = MaritimeSARService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
