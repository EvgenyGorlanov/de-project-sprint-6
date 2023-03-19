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

class FctStgObj(BaseModel):
    id: int
    object_value: str

class FctOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fcts(self, fct_threshold: int) -> List[FctStgObj]:
        with self._db.client().cursor(row_factory=class_row(FctStgObj)) as cur:
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
                    "threshold": fct_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class FctDestRepository:
    def insert_fct(self, conn: Connection, delivery_id: str, rate: int, sum: float, tip_sum: float) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    WITH delivery as(
                        SELECT 
                            id
                        FROM dds.prj_deliveries
                        WHERE 
                            delivery_id = %(delivery_id)s)

                    INSERT INTO dds.prj_fct_deliveries(
	                    delivery_id, rate, sum, tip_sum)
                    SELECT
                        delivery.id as delivery_id,
                        %(rate)s as rate,
                        %(sum)s as sum,
                        %(tip_sum)s as tip_sum
                    FROM delivery 
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "delivery_id": delivery_id,
                    "rate": rate,
                    "sum": sum,
                    "tip_sum": tip_sum
                },
            )

class FctLoader:
    WF_KEY = "dds.prj_fct_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctOriginRepository(pg_origin)
        self.dds = FctDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fcts(self):
        
        with self.pg_dest.connection() as conn:            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fcts(last_loaded)
        
            if not load_queue:
                self.log.info("Quitting.")
                return

            for fct_obj in load_queue:
                fct_str_json = json.loads(fct_obj.object_value.replace("'", "\""))
                for fct in fct_str_json:
                    self.dds.insert_fct(conn, fct["delivery_id"], fct["rate"], fct["sum"], fct["tip_sum"])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
