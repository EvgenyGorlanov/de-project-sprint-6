from datetime import datetime
import json
from logging import Logger
from typing import List

from project.dds.dds_settings import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class TimestampStgObj(BaseModel):
    id: int
    object_value: str

class TimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int) -> List[TimestampStgObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampStgObj)) as cur:
            cur.execute(
                """
                 SELECT 
                    id,
                    object_value                    
                    FROM stg.prj_deliveries
                    WHERE
                        id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": timestamp_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class TimestampDestRepository:
    def insert_timestamp(self, conn: Connection, ts: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.prj_timestamps(
	                    ts, year, month, day, time, date)
                    SELECT
                        CAST(%(ts)s as timestamp) as ts,
                        CAST(EXTRACT(YEAR FROM CAST(%(ts)s as timestamp)) as int) as year,
                        CAST(EXTRACT(MONTH FROM CAST(%(ts)s as timestamp)) as int) as month,
                        CAST(EXTRACT(DAY FROM CAST(%(ts)s as timestamp)) as int) as day,
                        CAST(CAST(%(ts)s as timestamp) as time) as time,
                        CAST(CAST(%(ts)s as timestamp) as date) as date
                    ON CONFLICT (ts) DO NOTHING
                """,
                {
                    "ts": ts
                },
            )

class TimestampLoader:
    WF_KEY = "dds.prj_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampOriginRepository(pg_origin)
        self.dds = TimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        
        with self.pg_dest.connection() as conn:            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_timestamps(last_loaded)
        
            if not load_queue:
                self.log.info("Quitting.")
                return

            for timestamp_obj in load_queue:
                timestamp_str_json = json.loads(timestamp_obj.object_value.replace("'", "\""))
                for timestamp in timestamp_str_json:
                    self.dds.insert_timestamp(conn, timestamp["order_ts"])
                    self.dds.insert_timestamp(conn, timestamp["delivery_ts"])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
