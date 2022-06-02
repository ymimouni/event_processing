from src.config import *
from src.utils.helpers import load_csv_data, save_analysis_results_to_csv


ANALYSIS_QUERY = """
            /*
             * Given a dataset of three csv files containing HTTP endpoint event data, internet connectivity status logs
             * and orders data, the goal of this query is to know the connectivity environment of a device in the period
             * of time surrounding when an order is dispatched to it.
             *
             * We start by finding specific statistics to the following bounded periods of time:
             *      - Three minutes before the order creation time
             *      - Three minutes after the order creation time
             *      - One hour before the order creation time
             * Then, we move to find general statistics across unbounded period of time.
             */
             
            /*
             * First, we find the total count of all polling events, the count of each type of polling status_code and
             * the count of each type of polling error_code and the count of responses without error codes for the
             * bounded period of time: Three minutes before the order creation time.
             */
            WITH 3_min_before_helper_1 AS
            (
                SELECT DISTINCT orders.order_id,
                    -- The total count of all polling events.
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id) AS count_polling_events,
                    -- The count of each type of polling status_code.
                    polling.status_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.status_code) AS
                        count_status_code,
                    -- The count of each type of polling error_code.
                    polling.error_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.error_code) AS
                        count_error_code
                FROM orders INNER JOIN polling
                ON orders.device_id = polling.device_id
                WHERE orders.order_creation_time >= polling.polling_creation_time
                        AND unix_timestamp(orders.order_creation_time) - unix_timestamp(polling.polling_creation_time)
                         <= 180
            ),
            -- We use a left join since some orders do not have all the polling statistics.
            3_min_before_helper_2 AS
            (
                SELECT orders.order_id,
                        "THREE_MINUTES_BEFORE" AS period_of_time,
                        3_min_before_helper_1.count_polling_events,
                        3_min_before_helper_1.status_code,
                        3_min_before_helper_1.count_status_code,
                        3_min_before_helper_1.error_code,
                        3_min_before_helper_1.count_error_code
                FROM orders LEFT JOIN 3_min_before_helper_1
                ON orders.order_id = 3_min_before_helper_1.order_id
            ),
            
            /*
             * Second, we find the same statistics as above for the bounded period of time: Three minutes after the
             * order creation time.
             */
            3_min_after_helper_1 AS
            (
                SELECT DISTINCT orders.order_id,
                    -- The total count of all polling events.
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id) AS count_polling_events,
                    -- The count of each type of polling status_code.
                    polling.status_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.status_code) AS
                        count_status_code,
                    -- The count of each type of polling error_code.
                    polling.error_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.error_code) AS
                        count_error_code
                FROM orders INNER JOIN polling
                ON orders.device_id = polling.device_id
                WHERE orders.order_creation_time <= polling.polling_creation_time
                        AND unix_timestamp(polling.polling_creation_time) - unix_timestamp(orders.order_creation_time) 
                         <= 180
            ),
            -- Again, we use a left join since some orders do not have all the polling statistics.
            3_min_after_helper_2 AS
            (
                SELECT orders.order_id,
                        "THREE_MINUTES_AFTER" AS period_of_time,
                        3_min_after_helper_1.count_polling_events,
                        3_min_after_helper_1.status_code,
                        3_min_after_helper_1.count_status_code,
                        3_min_after_helper_1.error_code,
                        3_min_after_helper_1.count_error_code
                FROM orders LEFT JOIN 3_min_after_helper_1
                ON orders.order_id = 3_min_after_helper_1.order_id
            ),
            
            /*
             * Same, we find the above statistics for the bounded period of time: One hour before the order creation
             * time.
             */
            1_hour_before_helper_1 AS
            (
                SELECT DISTINCT orders.order_id,
                    -- The total count of all polling events.
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id) AS count_polling_events,
                    -- The count of each type of polling status_code.
                    polling.status_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.status_code) AS
                        count_status_code,
                    -- The count of each type of polling error_code.
                    polling.error_code,
                    COUNT(polling.device_id) OVER(PARTITION BY orders.order_id, polling.error_code) AS
                        count_error_code
                FROM orders INNER JOIN polling
                ON orders.device_id = polling.device_id
                WHERE orders.order_creation_time >= polling.polling_creation_time
                        AND unix_timestamp(orders.order_creation_time) - unix_timestamp(polling.polling_creation_time)
                         <= 3600
            ),
             -- Again, we use a left join since some orders do not have all the polling statistics.
            1_hour_before_helper_2 AS
            (
                SELECT orders.order_id,
                        "ONE_HOUR_BEFORE" AS period_of_time,
                        1_hour_before_helper_1.count_polling_events,
                        1_hour_before_helper_1.status_code,
                        1_hour_before_helper_1.count_status_code,
                        1_hour_before_helper_1.error_code,
                        1_hour_before_helper_1.count_error_code
                FROM orders LEFT JOIN 1_hour_before_helper_1
                ON orders.order_id = 1_hour_before_helper_1.order_id
            ),
            
            /*
             * Now we gather all the above statistics together. This way, we are finished with the specific statistics
             * that apply to bounded periods of time.
             */
            bounded_period_info_helper AS
            (
                SELECT *
                FROM 3_min_before_helper_2
                UNION SELECT *
                FROM 3_min_after_helper_2
                UNION SELECT *
                FROM 1_hour_before_helper_2
            ),
            
            
            /*
             * Let's move on to find general statistics that are unbounded in time.
             */
             
            /*
             * We start by finding the time of the polling event immediately preceding the order creation time.
             */
            preceding_polling_event AS
            (
                SELECT orders.order_id,
                        MAX(polling.polling_creation_time) AS preceding_event_time
                FROM orders INNER JOIN polling
                ON orders.device_id = polling.device_id
                WHERE orders.order_creation_time >= polling.polling_creation_time
                GROUP BY orders.order_id
            ),
            /*
             * Then we find the time of the polling event immediately following the order creation time.
             */
            following_polling_event AS
            (
                SELECT orders.order_id,
                    MIN(polling.polling_creation_time) AS following_event_time
                FROM orders INNER JOIN polling
                ON orders.device_id = polling.device_id
                WHERE orders.order_creation_time <= polling.polling_creation_time
                GROUP BY orders.order_id
            ),
            /*
             * We gather the two previous information together, it gives us the preceding and following polling events.
             */
            prec_flw_polling_events AS
            (
                SELECT COALESCE(ppe.order_id, fpe.order_id) AS order_id,
                        ppe.preceding_event_time,
                        fpe.following_event_time
                FROM preceding_polling_event ppe
                FULL OUTER JOIN following_polling_event fpe
                ON ppe.order_id = fpe.order_id
            ),
            
            /*
             * Now, the final general information we need is the most recent connectivity status (“ONLINE” or “OFFLINE”)
             * before an order, and at what time the order changed to this status. This can be across any period of time
             * before the order creation time.
             */
            -- We join the two tables concerned with this information: orders and connectivity_status.
            con_status_helper_1 AS
            (
                SELECT orders.order_id,
                        orders.device_id,
                        cs.cs_creation_time,
                        cs.status
                FROM orders INNER JOIN connectivity_status cs
                ON orders.device_id = cs.device_id
                WHERE orders.order_creation_time >= cs.cs_creation_time
            ),
            -- We save the timestamp when the status changes: status_change_time.
            con_status_helper_2 AS
            (
                SELECT order_id,
                        device_id,
                        status,
                        CASE WHEN LAG(status) OVER (PARTITION BY order_id, device_id ORDER BY cs_creation_time) = status
                            THEN NULL
                        ELSE cs_creation_time
                        END status_change_time
                FROM con_status_helper_1
            ),
            -- We find the most recent status change time.
            con_status_helper_3 AS
            (
                SELECT order_id,
                        status,
                        status_change_time,
                        MAX(status_change_time) OVER (PARTITION BY order_id, device_id) AS
                            most_recent_status_change_time
                FROM con_status_helper_2
            ),
            -- Finally, we select only the rows we need.
            con_status_helper_4 AS
            (
                SELECT order_id,
                        status AS most_recent_status,
                        most_recent_status_change_time
                FROM con_status_helper_3
                WHERE status_change_time = most_recent_status_change_time
            ),
            
            /*
             * Nice!! we have all the information needed across an unbounded period of time. Let's gather them together.
             */
            unbounded_period_info_helper AS
            (
                SELECT COALESCE(csh4.order_id, pfpe.order_id) AS order_id,
                        csh4.most_recent_status,
                        csh4.most_recent_status_change_time,
                        pfpe.preceding_event_time,
                        pfpe.following_event_time
                FROM con_status_helper_4 csh4
                FULL OUTER JOIN prec_flw_polling_events pfpe
                ON csh4.order_id = pfpe.order_id
            )
            
            /*
             * Now that we have all the information we need: information that is bounded in time (three minutes before,
             * three minutes after and one hour before) and information that is unbounded in time, all we have to do is
             * join them together.
             */
            SELECT bpih.order_id,
                    upih.most_recent_status,
                    upih.most_recent_status_change_time,
                    upih.preceding_event_time,
                    upih.following_event_time,
                    bpih.period_of_time,
                    bpih.count_polling_events,
                    bpih.status_code,
                    bpih.count_status_code,
                    bpih.error_code,
                    bpih.count_error_code
            -- At this stage, bounded_period_info_helper has all the orders, since we used a left join earlier.
            -- unbounded_period_info_helper does not have all the orders, so we use a left join.
            FROM bounded_period_info_helper bpih
            LEFT JOIN unbounded_period_info_helper upih
            ON bpih.order_id = upih.order_id
          """


def process_data(spark, input_path):
    """
    This functions launches the analysis on the data given in the input_path and returns a dataframe with the results of
    the analysis.
    """
    # Load data.
    orders_df = load_csv_data(spark, path=input_path + ORDERS_DATA, schema=orders_schema, header=True)
    polling_df = load_csv_data(spark, path=input_path + POLLING_DATA, schema=polling_schema, header=True)
    connectivity_status_df = load_csv_data(spark, path=input_path + CONNECTIVITY_STATUS_DATA,
                                           schema=connectivity_status_schema, header=True)

    # Cache all the dataframes because we would be using them again and again in different use cases below.
    orders_df.cache()
    polling_df.cache()
    connectivity_status_df.cache()

    # Register all the DataFrames as Temporary Views.
    orders_df.createOrReplaceTempView("orders")
    polling_df.createOrReplaceTempView("polling")
    connectivity_status_df.createOrReplaceTempView("connectivity_status")

    # Analyse the input data.
    analysis_results = spark.sql(ANALYSIS_QUERY)

    # Return analysis results.
    return analysis_results


def run_event_processing(input_path=DATA_FOLDER, output_path=ANALYSIS_RESULTS_FOLDER):
    """
    This function is the entrypoint to this module. It launches the analysis on the data given in the input_path and
     saves the results in the CSV file specified in the output_path.
    """
    # Get a spark session.
    spark = get_spark_session()

    # Analyse input data.
    analysis_results = process_data(spark, input_path)

    # Save the analysis results in a CSV file.
    save_analysis_results_to_csv(analysis_results, spark, path=output_path + ANALYSIS_RESULTS_FILE)


if __name__ == "__main__":
    run_event_processing(input_path=DATA_FOLDER, output_path=ANALYSIS_RESULTS_FOLDER)

    # spark = get_spark_session()

    # process_data(spark, DATA_FOLDER).show(n=1000, truncate=False)
