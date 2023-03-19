from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class ReportCourierLoader:
    def load_report_couriers(self, dest_conn: PgConnect) -> None:
        with dest_conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.prj_courier_payment_report(
                            courier_id, courier_name, settlement_year, 
                            settlement_month, orders_count, orders_total_sum, 
                            rate_avg, order_processing_fee, courier_order_sum, 
                            courier_tips_sum, courier_reward_sum)
                        SELECT 
                            c.id as courier_id,
                            c.courier_name as courier_name,
                            ts.year as settlement_year,
                            ts.month as settlement_month,
                            count(DISTINCT o.id) as orders_count,
                            sum(fct.sum)::numeric(14,2) as orders_total_sum,
                            avg(fct.rate)::numeric(2,1) as rate_avg,
                            CAST(sum(fct.sum) * 0.25 as numeric(14,2)) as order_processing_fee,
                            CASE 
                                WHEN avg(fct.rate)::numeric(2,1) < 4 THEN CAST(sum(fct.sum) * 0.05 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4 AND avg(fct.rate)::numeric(2,1) < 4.5 THEN CAST(sum(fct.sum) * 0.07 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4.5 AND avg(fct.rate)::numeric(2,1) < 4.9 THEN CAST(sum(fct.sum) * 0.08 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4.9 THEN CAST(sum(fct.sum) * 0.1 as numeric(14,2))
                            END as courier_order_sum,
                            sum(fct.tip_sum)::numeric(14,2) as courier_tips_sum,
                            CASE 
                                WHEN avg(fct.rate)::numeric(2,1) < 4 THEN CAST(sum(fct.sum) * 0.05 as numeric(14,2)) + CAST(sum(fct.tip_sum) * 0.95 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4 AND avg(fct.rate)::numeric(2,1) < 4.5 THEN CAST(sum(fct.sum) * 0.07 as numeric(14,2)) + CAST(sum(fct.tip_sum) * 0.95 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4.5 AND avg(fct.rate)::numeric(2,1) < 4.9 THEN CAST(sum(fct.sum) * 0.08 as numeric(14,2)) + CAST(sum(fct.tip_sum) * 0.95 as numeric(14,2))
                                WHEN avg(fct.rate)::numeric(2,1) >= 4.9 THEN CAST(sum(fct.sum) * 0.1 as numeric(14,2)) + CAST(sum(fct.tip_sum) * 0.95 as numeric(14,2))
                            END as courier_reward_sum
                        FROM dds.prj_fct_deliveries fct
                        LEFT JOIN dds.prj_deliveries d ON d.id = fct.delivery_id
                        LEFT JOIN dds.prj_couriers c ON c.id = d.courier_id
                        LEFT JOIN dds.prj_timestamps ts ON d.timestamp_id = ts.id
                        LEFT JOIN dds.prj_orders o ON o.id = d.order_id

                        WHERE
                           ( ts.month = EXTRACT(YEAR FROM NOW()) AND ts.year = EXTRACT(MONTH FROM NOW()))
                           OR
                           ( ts.month = EXTRACT(YEAR FROM EXTRACT(YEAR FROM (NOW() - interval '1 month'))) 
                                AND ts.year = EXTRACT(YEAR FROM (NOW() - interval '1 month')))

                        GROUP BY c.id, c.courier_name, ts.year, ts.month
                        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                        SET
                            orders_count = EXCLUDED.orders_count,
                            orders_total_sum = EXCLUDED.orders_total_sum,
                            rate_avg = EXCLUDED.rate_avg,
                            order_processing_fee = EXCLUDED.order_processing_fee,
                            courier_order_sum = EXCLUDED.courier_order_sum,
                            courier_tips_sum = EXCLUDED.courier_tips_sum,
                            courier_reward_sum = EXCLUDED.courier_reward_sum
                    """
                )
