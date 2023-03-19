DROP TABLE IF EXISTS dds.prj_srv_wf_settings;
CREATE TABLE IF NOT EXISTS dds.prj_srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

DROP TABLE IF EXISTS dds.prj_couriers;
CREATE TABLE IF NOT EXISTS dds.prj_couriers (
    id serial PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
    CONSTRAINT dds_prj_couriers_courier_id_uniq UNIQUE (courier_id)    
);

DROP TABLE IF EXISTS dds.prj_timestamps;
CREATE TABLE IF NOT EXISTS dds.prj_timestamps (
    id serial PRIMARY KEY,
	ts timestamp NOT NULL,
	year smallint NOT NULL CONSTRAINT dm_timestamps_year_check check (year >= 2022 AND year < 2500),
	month smallint NOT NULL CONSTRAINT dm_timestamps_month_check check (month >= 1 AND month <= 12),
	day smallint NOT NULL CONSTRAINT dm_timestamps_day_check check (day >= 1 AND day <= 31),
	time time NOT NULL,
	date date NOT NULL,
    CONSTRAINT dds_prj_timestamps_ts_uniq UNIQUE (ts) 
);

DROP TABLE IF EXISTS dds.prj_orders;
CREATE TABLE IF NOT EXISTS dds.prj_orders (
    id serial PRIMARY KEY,
	order_id varchar NOT NULL,
    timestamp_id int NOT NULL,
    CONSTRAINT dds_prj_orders_timestamp_id_fk FOREIGN KEY (timestamp_id)
        REFERENCES dds.prj_timestamps(id),
    CONSTRAINT dds_prj_orders_order_id_uniq UNIQUE (order_id)
);

DROP TABLE IF EXISTS dds.prj_deliveries;
CREATE TABLE IF NOT EXISTS dds.prj_deliveries (
    id serial PRIMARY KEY,
    delivery_id varchar NOT NULL,
    address varchar NOT NULL,
    courier_id int NOT NULL,
    timestamp_id int NOT NULL,
    order_id int NOT NULL,
    CONSTRAINT dds_prj_deliveries_courier_id_fk FOREIGN KEY (courier_id)
        REFERENCES dds.prj_couriers(id),
    CONSTRAINT dds_prj_deliveries_timestamp_id_fk FOREIGN KEY (timestamp_id)
        REFERENCES dds.prj_timestamps (id),
    CONSTRAINT dds_prj_deliveries_order_id_fk FOREIGN KEY (order_id)
        REFERENCES dds.prj_orders(id),
    CONSTRAINT dds_prj_deliveries_delivery_id_uniq UNIQUE (delivery_id)    
);

DROP TABLE IF EXISTS dds.prj_fct_deliveries;
CREATE TABLE IF NOT EXISTS dds.prj_fct_deliveries (
    id serial PRIMARY KEY,
    delivery_id int NOT NULL,
    rate int NOT NULL DEFAULT 0 CONSTRAINT prj_fct_deliveries_rate_check check (rate >= 0),
    sum numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_fct_deliveries_sum_check check (sum >= 0),
    tip_sum numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_fct_deliveries_tip_sum_check check (tip_sum >= 0),
    CONSTRAINT dds_prj_fct_deliveries_delivery_id_fk FOREIGN KEY (delivery_id)
        REFERENCES dds.prj_deliveries(id)
    CONSTRAINT dds_prj_fct_deliveries_delivery_id_uniq UNIQUE (delivery_id)    
);