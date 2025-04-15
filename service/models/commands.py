from enum import Enum

class Command(Enum):
    RUN_BATCH = "run_batch"
    TRANSFORM = "transform"
    LOAD = "load"

    COMPUTE_CLTV = "compute_customer_lifetime_value"
    GET_REPEAT_CUSTOMERS = "get_repeat_customers"
    COMPUTE_TRANSACTION_VOLUME = "compute_transaction_volume"
    GET_TOTAL_TRANSACTIONS = "get_total_transactions"
    CLUSTER_CUSTOMERS_FCM = "cluster_customers"
    COMPUTE_WEEKLY_TRENDS = "compute_weekly_trends"
    GET_ACTIVITY_HEATMAP = "get_transaction_heatmap"

