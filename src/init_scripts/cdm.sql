DROP TABLE IF EXISTS cdm.prj_courier_payment_report;
CREATE TABLE IF NOT EXISTS cdm.prj_courier_payment_report(
    id serial PRIMARY KEY,
    courier_id int NOT NULL,
    courier_name varchar NOT NULL,
    settlement_year smallint NOT NULL,
    settlement_month smallint NOT NULL,
    orders_count int NOT NULL,
    orders_total_sum numeric(14,2) NOT NULL,
    rate_avg numeric(2,1) NOT NULL DEFAULT 0 CONSTRAINT prj_courier_payment_report_rate_avg_check check (rate_avg >= 0),
    order_processing_fee numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_courier_payment_report_order_processing_fee_check check (order_processing_fee >= 0),
    courier_order_sum numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_courier_payment_report_courier_order_sum_check check (order_processing_fee >= 0),
    courier_tips_sum numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_courier_payment_report_courier_tips_sum_check check (courier_tips_sum >= 0),
    courier_reward_sum numeric(14,2) NOT NULL DEFAULT 0 CONSTRAINT prj_courier_payment_report_courier_reward_sum_check check (courier_reward_sum >= 0),
    CONSTRAINT cdm_prj_courier_payment_report_report_uniq UNIQUE (courier_id, settlement_year,settlement_month)
);