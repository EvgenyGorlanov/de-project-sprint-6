from datetime import datetime
from logging import Logger
from typing import List
from datetime import date

from project.stg.stg_settings import EtlSetting, StgEtlSettingsRepository
from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

import requests
import json

class StgObj:
    object_value: str
    object_value_json: json

class GetFromAPI:
    def __init__(self, link: str, headers) -> None:
        self.link = link
        self.headers = headers
    
    def get_StgObj(self, last_loaded: str, table_name: str, len_load_queue: str) -> StgObj:
        r = StgObj()
        print("offset in method = " + len_load_queue)
        if table_name == "stg.prj_couriers":
            
            r.object_value = requests.get(self.link + "?sort_field=name&sort_direction=asc&offset=" + len_load_queue, headers=self.headers).text
        else:
            r.object_value = requests.get(self.link + "?sort_field=delivery_ts&sort_direction=asc&from=" + last_loaded, headers=self.headers).text
            
        r.object_value_json = json.loads(r.object_value.replace("'", "\""))
        
        return r
    
class InsertToDB:
    def insert_stg_obj(self, conn: Connection, obj: StgObj, table_name: str) -> None:
        if table_name == "stg.prj_couriers":
            with conn.cursor() as cur:
                cur.execute( 
                    """
                        INSERT INTO stg.prj_couriers(object_value, update_ts) 
                        VALUES (%(object_value)s, NOW())
                    """,
                    {
                        "object_value": obj.object_value
                    },
                )
        else:
            with conn.cursor() as cur:
                cur.execute( 
                    """
                        INSERT INTO stg.prj_deliveries(object_value, update_ts) 
                        VALUES (%(object_value)s, NOW())
                    """,
                    {
                        "object_value": obj.object_value
                    },
                )    

#изменить код
class StgLoader:
    
    LAST_LOADED_KEY = "last_loaded_date"

    def __init__(self, origin_link: str, origin_headers, pg_dest: PgConnect, log: Logger, table_name: str) -> None:
        self.WF_KEY = table_name + "_origin_to_stg_workflow"
        self.pg_dest = pg_dest
        self.origin = GetFromAPI(origin_link, origin_headers)
        self.stg = InsertToDB()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log
        self.table_name = table_name

    def load_stg(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_KEY: "2022-01-01 00:00:00"})
                
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_KEY]
            len_load_queue = 0

            while True:
            
                load_queue = self.origin.get_StgObj(last_loaded, self.table_name, str(len_load_queue))
                if not load_queue:
                    self.log.info("Quitting.")
                    return

                if len(load_queue.object_value_json) == 0:
                    break

                len_load_queue = len_load_queue + len(load_queue.object_value_json)
                
                self.stg.insert_stg_obj(conn, load_queue, self.table_name)

                if self.table_name == "stg.prj_couriers":
                    last_loaded = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_loaded = datetime.strptime(max(g["delivery_ts"] for g in load_queue.object_value_json), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')


            wf_setting.workflow_settings[self.LAST_LOADED_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_KEY]}")


