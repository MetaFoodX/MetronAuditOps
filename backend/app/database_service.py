import logging
import os
import pymysql
import stat
import tempfile
import threading
from sshtunnel import SSHTunnelForwarder
from typing import Dict, List

from app.utils.config import get_config

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        config = get_config()
        db_config = config.get("DB")
        if not db_config:
            raise ValueError("Database configuration is missing in config.yaml")

        # SSH/bastion configuration
        self.ssh_host = db_config.get("ssh_host")
        self.ssh_username = db_config.get("ssh_username")
        self.ssh_pkey = db_config.get("ssh_pkey")  # can be a path or PEM content
        self.remote_host = db_config.get("remote_host")
        self.remote_port = db_config.get("remote_port")
        # Database configuration
        self.db_user = db_config.get("db_user")
        self.db_pass = db_config.get("db_pass")
        self.db_schema = db_config.get("db_schema")
        self.db_host = db_config.get("db_host") or "127.0.0.1"
        self.db_port = int(db_config.get("db_port") or 3306)

        self.tunnel: SSHTunnelForwarder | None = None
        self.connection: pymysql.connections.Connection | None = None
        self._ssh_key_tempfile: str | None = None
        self._lock = threading.RLock()
        self._local_bind_host: str | None = None
        self._local_bind_port: int | None = None

    def _resolve_ssh_key_path(self) -> str | None:
        """Resolve SSH private key to a filesystem path usable by paramiko.

        Priority:
        - ENV DB_SSH_KEY (raw PEM)
        - ENV DB_SSH_KEY_BASE64 (base64-encoded PEM)
        - self.ssh_pkey (path or raw PEM)
        Returns a path, creating a secure tempfile if needed.
        """
        # ENV raw PEM
        pem = os.environ.get("DB_SSH_KEY")
        if pem:
            try:
                logger.info("DB SSH key: using raw PEM from env DB_SSH_KEY")
            except Exception:
                pass
            return self._write_temp_ssh_key(pem)
        # ENV base64 PEM
        b64 = os.environ.get("DB_SSH_KEY_BASE64")
        if b64:
            try:
                import base64

                pem = base64.b64decode(b64).decode("utf-8")
                try:
                    logger.info(
                        "DB SSH key: using base64 PEM from env DB_SSH_KEY_BASE64"
                    )
                except Exception:
                    pass
                return self._write_temp_ssh_key(pem)
            except Exception as e:
                logger.error(f"Failed to decode DB_SSH_KEY_BASE64: {e}")
        # Config value
        if not self.ssh_pkey:
            return None
        if isinstance(self.ssh_pkey, str) and self.ssh_pkey.strip().startswith(
            "-----BEGIN"
        ):
            try:
                logger.info("DB SSH key: using raw PEM from config DB.ssh_pkey")
            except Exception:
                pass
            return self._write_temp_ssh_key(self.ssh_pkey)
        # Treat as path
        key_path = os.path.expanduser(str(self.ssh_pkey))
        if os.path.exists(key_path):
            try:
                logger.info(
                    f"DB SSH key: using file path from config DB.ssh_pkey at {key_path}"
                )
            except Exception:
                pass
            return key_path
        logger.warning(f"SSH key path not found: {key_path}")
        return None

    def _write_temp_ssh_key(self, pem: str) -> str:
        if self._ssh_key_tempfile and os.path.exists(self._ssh_key_tempfile):
            return self._ssh_key_tempfile
        fd, path = tempfile.mkstemp(prefix="db_ssh_key_", text=True)
        with os.fdopen(fd, "w") as f:
            f.write(pem)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600
        self._ssh_key_tempfile = path
        return path

    def start_tunnel(self) -> bool:
        """Start a persistent SSH tunnel to the production database.
        Safe to call multiple times; it will no-op if already active.
        """
        with self._lock:
            # Already active
            if self.tunnel and getattr(self.tunnel, "is_active", False):
                return True
            try:
                key_path = self._resolve_ssh_key_path()
                if not key_path:
                    raise RuntimeError("SSH private key not provided or not found")
                # Use ephemeral local bind port to avoid collisions
                self.tunnel = SSHTunnelForwarder(
                    (self.ssh_host, 22),
                    ssh_username=self.ssh_username,
                    ssh_pkey=key_path,
                    remote_bind_address=(self.remote_host, int(self.remote_port)),
                    local_bind_address=("127.0.0.1", 0),
                    set_keepalive=30,
                    allow_agent=False,
                )
                self.tunnel.start()
                # Capture the chosen local bind host/port
                # sshtunnel exposes _local_binds as a list of (host, port)
                try:
                    self._local_bind_host = self.tunnel.local_bind_host
                    self._local_bind_port = int(self.tunnel.local_bind_port)
                except Exception:
                    # Fallback for older sshtunnel versions
                    binds = getattr(self.tunnel, "_local_binds", None)
                    if binds and len(binds) > 0:
                        self._local_bind_host, self._local_bind_port = binds[0]
                    else:
                        self._local_bind_host, self._local_bind_port = (
                            "127.0.0.1",
                            self.db_port,
                        )
                logger.info(
                    f"SSH tunnel established to {self.ssh_host} -> {self.remote_host}:{self.remote_port} at {self._local_bind_host}:{self._local_bind_port}"
                )
                return True
            except Exception as e:
                logger.error(f"Failed to establish SSH tunnel: {e}")
                # Ensure we clean up a partially created tunnel
                try:
                    if self.tunnel and getattr(self.tunnel, "is_active", False):
                        self.tunnel.stop()
                except Exception:
                    pass
                self.tunnel = None
                return False

    def connect_db(self) -> bool:
        """Connect to MySQL database through the persistent SSH tunnel."""
        # Fast path under lock: return if already connected
        with self._lock:
            if self.connection:
                try:
                    self.connection.ping(reconnect=True)
                    return True
                except Exception:
                    try:
                        self.connection.close()
                    except Exception:
                        pass
                    self.connection = None

        # Start tunnel WITHOUT holding the lock, with timeout protection
        import threading
        import time

        # Use a thread with timeout to prevent hanging
        tunnel_result = {"success": False, "error": None}

        def start_tunnel_with_timeout():
            try:
                tunnel_result["success"] = self.start_tunnel()
            except Exception as e:
                tunnel_result["error"] = str(e)
                tunnel_result["success"] = False

        tunnel_thread = threading.Thread(target=start_tunnel_with_timeout)
        tunnel_thread.daemon = True
        tunnel_thread.start()

        # Wait for tunnel to start with timeout
        tunnel_thread.join(
            timeout=20.0
        )  # allow a bit more time on Cloud Run cold starts

        if tunnel_thread.is_alive():
            logger.error("SSH tunnel startup timed out after 10 seconds")
            return False

        if not tunnel_result["success"]:
            if tunnel_result["error"]:
                logger.error(f"SSH tunnel failed: {tunnel_result['error']}")
            return False

        host = self._local_bind_host or "127.0.0.1"
        port = int(self._local_bind_port or self.db_port)

        # Quick reachability probe (not under the lock)
        try:
            import socket

            s = socket.create_connection((host, port), timeout=3.0)
            s.close()
        except Exception as e:
            logger.error(f"Local tunnel port not reachable at {host}:{port}: {e}")
            return False

        # Establish DB connection (still not under the lock)
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_schema,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,
                read_timeout=20,
                write_timeout=20,
                autocommit=True,
            )
        except Exception as e:
            logger.error(f"Failed to connect to MySQL at {host}:{port}: {e}")
            return False

        # Cache under lock
        with self._lock:
            self.connection = conn
            logger.info("Connected to MySQL through SSH tunnel")
            return True

    def _new_conn(self):
        """Create a new MySQL connection. Assumes tunnel is already up."""
        host = self._local_bind_host or "127.0.0.1"
        port = int(self._local_bind_port or self.db_port)
        return pymysql.connect(
            host=host,
            port=port,
            user=self.db_user,
            password=self.db_pass,
            database=self.db_schema,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
            read_timeout=10,
            write_timeout=10,
            autocommit=True,
        )

    def get_reference_pans_for_restaurant(
        self,
        restaurant_id: int | str,
        date: str | None = None,
        types: List[int] | None = None,
        days_back: int = 0,
        hard_limit: int = 5000,
    ) -> List[Dict]:
        """
        Return the latest scan row per PanID for a restaurant directly from the DB.
        - If 'types' is provided, restrict to those Type values (e.g., [6] for reference pans).
        - If 'date' is provided, limit the time window around that day (optionally 'days_back' days before).
        """
        try:
            if not self.start_tunnel():
                return []

            conn = None
            cursor = None
            try:
                conn = self._new_conn()
                conn.ping(reconnect=True)
                cursor = conn.cursor()

                where = ["s.RestaurantID = %s", "s.PanID IS NOT NULL", "s.Status <> 0"]
                params: list = [restaurant_id]
                if types and len(types) > 0:
                    placeholders = ",".join(["%s"] * len(types))
                    where.append(f"s.Type IN ({placeholders})")
                    params.extend(types)
                # Time window filter using COALESCE of known timestamp columns
                if date:
                    try:
                        from datetime import datetime, timedelta

                        # Interpret date as UTC midnight
                        base = datetime.strptime(date, "%Y-%m-%d")
                        start = base - timedelta(days=max(0, int(days_back)))
                        end = base + timedelta(days=1)  # up to end of that date
                        where.append(
                            "COALESCE(s.UpdatedAt, s.CapturedAt, s.CreatedAt) >= %s"
                        )
                        where.append(
                            "COALESCE(s.UpdatedAt, s.CapturedAt, s.CreatedAt) < %s"
                        )
                        params.append(start.strftime("%Y-%m-%d %H:%M:%S"))
                        params.append(end.strftime("%Y-%m-%d %H:%M:%S"))
                    except Exception as te:
                        logger.warning(
                            f"Time window parse failed for date={date}: {te}"
                        )
                where_sql = " AND ".join(where)
                query = (
                    "SELECT s.ID, s.Number, s.ShortID, s.PanID, s.MenuItemName, s.DetectedSizeStandard, "
                    "s.Weight, s.DetectedDepth, s.Volume, s.ImageURL, s.DepthImageURL, "
                    "s.Status, s.Type, s.CapturedAt, s.CreatedAt, s.UpdatedAt, "
                    "COALESCE(p.Shape, 'Unknown') as Shape, "
                    "COALESCE(p.SizeStandard, s.DetectedSizeStandard) as SizeStandard, "
                    "p.Data AS PansData, p.Depth AS PanDepth "
                    "FROM Scans s "
                    "LEFT JOIN Pans p ON s.PanID = p.ID "
                    f"WHERE {where_sql} "
                    "ORDER BY s.PanID ASC, s.UpdatedAt DESC, s.CapturedAt DESC, s.CreatedAt DESC "
                    "LIMIT %s"
                )
                params.append(int(hard_limit))

                cursor.execute(query, tuple(params))
                rows = cursor.fetchall() or []

                # Debug: Log the first result to see what fields we're getting
                if rows:
                    logger.info(f"First result fields: {list(rows[0].keys())}")
                    logger.info(f"First result Shape field: {rows[0].get('Shape')}")
                    logger.info(
                        f"First result SizeStandard field: {rows[0].get('SizeStandard')}"
                    )

                seen: set[int] = set()
                latest: List[Dict] = []
                for row in rows:
                    pid = row.get("PanID")
                    if pid is None or pid in seen:
                        continue
                    seen.add(pid)
                    # Parse Pans.Data JSON if present
                    if "PansData" in row and isinstance(row["PansData"], str):
                        try:
                            import json as _json

                            row["Data"] = _json.loads(row["PansData"])
                        except Exception:
                            row["Data"] = {}
                    # Normalize depth field from Pans if available
                    if "PanDepth" in row and row.get("PanDepth") is not None:
                        row["Depth"] = float(row["PanDepth"])

                    # Convert NULL values to appropriate defaults for React
                    cleaned_row = {}
                    for key, value in row.items():
                        if value is None:
                            if key in [
                                "Number",
                                "ShortID",
                                "MenuItemName",
                                "DetectedSizeStandard",
                                "Shape",
                                "SizeStandard",
                            ]:
                                cleaned_row[key] = ""
                            elif key in ["Weight", "DetectedDepth", "Volume"]:
                                cleaned_row[key] = 0.0
                            else:
                                cleaned_row[key] = ""
                        else:
                            cleaned_row[key] = value
                    latest.append(cleaned_row)

                logger.info(
                    f"DB pans: selected {len(latest)} unique pans from {len(rows)} rows (restaurant_id={restaurant_id}, types={types}, date={date}, days_back={days_back})"
                )
                return latest

            finally:
                try:
                    if cursor:
                        cursor.close()
                except Exception:
                    pass
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
        except Exception as e:
            logger.error(
                f"Failed to get reference pans for restaurant {restaurant_id}: {e}"
            )
            return []

    def get_reference_scans_for_pan(self, restaurant_id, pan_id):
        """
        Single pan query (kept for backward compatibility)
        """
        # Use the main method instead of the batch method
        all_pans = self.get_reference_pans_for_restaurant(restaurant_id, types=[6])
        for pan in all_pans:
            if pan.get("PanID") == pan_id:
                return [pan]
        return []

    def close(self):
        """Close database connection and SSH tunnel"""
        try:
            if self.connection:
                self.connection.close()
                logger.info("MySQL connection closed")
        except Exception as e:
            logger.error(f"Failed to close MySQL connection: {e}")

        try:
            if self.tunnel and getattr(self.tunnel, "is_active", False):
                self.tunnel.stop()
                logger.info("SSH tunnel closed")
        except Exception as e:
            logger.error(f"Failed to close SSH tunnel: {e}")

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
