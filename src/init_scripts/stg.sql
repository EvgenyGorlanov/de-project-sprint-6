DROP TABLE IF EXISTS stg.prj_srv_wf_settings;
CREATE TABLE IF NOT EXISTS stg.prj_srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

DROP TABLE IF EXISTS stg.prj_couriers;
CREATE TABLE IF NOT EXISTS stg.prj_couriers(
    id serial,
	object_value text,
	update_ts timestamp
);

DROP TABLE IF EXISTS stg.prj_deliveries;
CREATE TABLE IF NOT EXISTS stg.prj_deliveries(
    id serial,
	object_value text,
	update_ts timestamp
);