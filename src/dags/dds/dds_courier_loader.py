import json
from logging import Logger
from typing import List

from project.dds.dds_settings import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class CourierStgObj(BaseModel):
    id: int
    object_value: str

class CourierOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int) -> List[CourierStgObj]:
        with self._db.client().cursor(row_factory=class_row(CourierStgObj)) as cur:
            cur.execute(
                """
                 SELECT 
                    id,
                    object_value                    
                    FROM stg.prj_couriers
                    WHERE
                        id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": courier_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class CourierDestRepository:
    def insert_courier(self, conn: Connection, courier_id: str, courier_name: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.prj_couriers(
	                    courier_id, courier_name)
                    VALUES (%(courier_id)s,%(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name
                """,
                {
                    "courier_id": courier_id,
                    "courier_name": courier_name
                },
            )

class CourierLoader:
    WF_KEY = "dds.prj_couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierOriginRepository(pg_origin)
        self.dds = CourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        
        with self.pg_dest.connection() as conn:            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(last_loaded)
        
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier_obj in load_queue:
                courier_str_json = json.loads(courier_obj.object_value.replace("'", "\""))
                for courier in courier_str_json:
                    self.dds.insert_courier(conn, courier["_id"], courier["name"])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
