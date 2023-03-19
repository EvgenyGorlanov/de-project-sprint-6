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

class OrderStgObj(BaseModel):
    id: int
    object_value: str

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int) -> List[OrderStgObj]:
        with self._db.client().cursor(row_factory=class_row(OrderStgObj)) as cur:
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
                    "threshold": order_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class OrdersDestRepository:
    def insert_order(self, conn: Connection, order_id: str, order_ts: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    WITH ts as(
                        SELECT 
                            id
                        FROM dds.prj_timestamps
                        WHERE 
                            ts = %(order_ts)s
                    )

                    INSERT INTO dds.prj_orders(
	                    order_id, timestamp_id)
                    SELECT
                        %(order_id)s as order_id,
                        ts.id as timestamp_id
                    FROM ts
                    ON CONFLICT (order_id) DO NOTHING
                """,
                {
                    "order_ts": order_ts,
                    "order_id": order_id
                },
            )

class OrderLoader:
    WF_KEY = "dds.prj_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        
        with self.pg_dest.connection() as conn:            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded)
        
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order_obj in load_queue:
                order_str_json = json.loads(order_obj.object_value.replace("'", "\""))
                print(order_obj.object_value)
                for order in order_str_json:
                    print(order["order_id"] + "   " + order["order_ts"])
                    self.dds.insert_order(conn, order["order_id"], order["order_ts"])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
