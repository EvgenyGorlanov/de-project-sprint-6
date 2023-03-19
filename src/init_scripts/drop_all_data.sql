DELETE FROM dds.prj_fct_deliveries;
DELETE FROM dds.prj_deliveries;
DELETE FROM dds.prj_couriers;
DELETE FROM dds.prj_orders;
DELETE FROM dds.prj_srv_wf_settings;
DELETE FROM dds.prj_timestamps;

ALTER SEQUENCE dds.prj_couriers_id_seq RESTART WITH 1;
ALTER SEQUENCE dds.prj_deliveries_id_seq RESTART WITH 1;
ALTER SEQUENCE dds.prj_fct_deliveries_id_seq RESTART WITH 1;
ALTER SEQUENCE dds.prj_orders_id_seq RESTART WITH 1;
ALTER SEQUENCE dds.prj_srv_wf_settings_id_seq RESTART WITH 1;
ALTER SEQUENCE dds.prj_timestamps_id_seq RESTART WITH 1;

DELETE FROM stg.prj_couriers;
DELETE FROM stg.prj_deliveries;
DELETE FROM stg.prj_srv_wf_settings;

ALTER SEQUENCE stg.prj_couriers_id_seq RESTART WITH 1;
ALTER SEQUENCE stg.prj_deliveries_id_seq RESTART WITH 1;
ALTER SEQUENCE stg.prj_srv_wf_settings_id_seq RESTART WITH 1;

DELETE FROM cdm.prj_courier_payment_report;

ALTER SEQUENCE cdm.prj_courier_payment_report_id_seq RESTART WITH 1;



