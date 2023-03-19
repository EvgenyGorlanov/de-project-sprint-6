import logging

import pendulum
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from project.stg.stg_loader import StgLoader
from project.lib.pg_connect import ConnectionBuilder
from project.dds.dds_courier_loader import CourierLoader
from project.dds.dds_timestamps_loader import TimestampLoader
from project.dds.dds_orders_loader import OrderLoader
from project.dds.dds_deliveries_loader import DeliveryLoader
from project.dds.dds_fact_loader import FctLoader
from project.cdm.cdm_couriers_report_loader import ReportCourierLoader

log = logging.getLogger(__name__)
headers={
    "X-Nickname": "nutslyc", # авторизационные данные
    "X-Cohort": "6", # авторизационные данные
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f" # ключ API
    }

@dag(
    schedule_interval='0/60 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'prj'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_prj_loader_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="stg_load_couriers")
    def load_couriers():
        rest_loader_couriers = StgLoader("https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers", headers, dwh_pg_connect, log, "stg.prj_couriers")
        rest_loader_couriers.load_stg()  

    couriers = load_couriers()

    @task(task_id="stg_load_deliveries")
    def load_deliveries():
        rest_loader_deliveries = StgLoader("https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries", headers, dwh_pg_connect, log, "stg.prj_deliveries")
        rest_loader_deliveries.load_stg()  

    deliveries = load_deliveries()

    @task(task_id="stg_to_dds_load_couriers")
    def load_stg_dds_couriers():
        stg_dds_couriers = CourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        stg_dds_couriers.load_couriers()
    
    stg_dds_couriers = load_stg_dds_couriers()

    @task(task_id="stg_to_dds_load_timestamps")
    def load_stg_dds_timestamps():
        stg_dds_timestamps = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        stg_dds_timestamps.load_timestamps()
    
    stg_dds_timestamps = load_stg_dds_timestamps()

    @task(task_id="stg_to_dds_load_orders")
    def load_stg_dds_orders():
        stg_dds_orders = OrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        stg_dds_orders.load_orders()   

    stg_dds_orders = load_stg_dds_orders() 

    @task(task_id="stg_to_dds_load_deliveries")
    def load_stg_dds_deliveries():
        stg_dds_orders = DeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        stg_dds_orders.load_deliveries()   

    stg_dds_deliveries = load_stg_dds_deliveries()

    @task(task_id="stg_to_dds_load_fcts")
    def load_stg_dds_fcts():
        stg_dds_orders = FctLoader(dwh_pg_connect, dwh_pg_connect, log)
        stg_dds_orders.load_fcts()   

    stg_dds_fcts = load_stg_dds_fcts() 

    @task(task_id="cdm_load_report_couriers")
    def load_cdm_report_couriers():
        cdm_report_couriers = ReportCourierLoader()
        cdm_report_couriers.load_report_couriers(dwh_pg_connect)

    cdm_rep_couriers = load_cdm_report_couriers() 

    [deliveries, couriers] >> stg_dds_couriers >> stg_dds_timestamps >> stg_dds_orders >> stg_dds_deliveries >> stg_dds_fcts >> cdm_rep_couriers

prj_dag = sprint5_prj_loader_dag()
