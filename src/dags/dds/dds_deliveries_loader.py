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

class DeliveriesStgObj(BaseModel):
    id: int
    object_value: str

class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int) -> List[DeliveriesStgObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesStgObj)) as cur:
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
                    "threshold": delivery_threshold
                }
            )
            objs = cur.fetchall()
        return objs

class DeliveriesDestRepository:
    def insert_delivery(self, conn: Connection, delivery_id: str, address: str, courier_id: str, delivery_ts: str, order_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    WITH ts as(
                        SELECT 
                            '1' as key,
                            id
                        FROM dds.prj_timestamps
                        WHERE 
                            ts = %(delivery_ts)s
                    ), courier as (
                        SELECT 
                            '1' as key,
                            id
                        FROM dds.prj_couriers
                        WHERE 
                            courier_id = %(courier_id)s
                    ), ord as (
                        SELECT 
                            '1' as key,
                            id
                        FROM dds.prj_orders
                        WHERE 
                            order_id = %(order_id)s
                    )

                    INSERT INTO dds.prj_deliveries(
	                    delivery_id, address, courier_id, timestamp_id, order_id)
                    SELECT
                        %(delivery_id)s as delivery_id,
                        %(address)s as address,
                        courier.id as courier_id,
                        ts.id as timestamp_id,
                        ord.id as order_id
                    FROM ts 
                    LEFT JOIN courier ON ts.key = courier.key
                    LEFT JOIN ord ON ts.key = ord.key
                    ON CONFLICT (delivery_id) DO NOTHING
                """,
                {
                    "delivery_id": delivery_id,
                    "address": address,
                    "courier_id": courier_id,
                    "delivery_ts": delivery_ts,
                    "order_id": order_id
                },
            )

class DeliveryLoader:
    WF_KEY = "dds.prj_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        
        with self.pg_dest.connection() as conn:            
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded)
        
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery_obj in load_queue:
                delivery_str_json = json.loads(delivery_obj.object_value.replace("'", "\""))
                for delivery in delivery_str_json:
                    self.dds.insert_delivery(conn, delivery["delivery_id"], delivery["address"], delivery["courier_id"], delivery["delivery_ts"], delivery["order_id"])

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
