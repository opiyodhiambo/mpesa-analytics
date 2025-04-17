import os
import logging
import pandas as pd
import plotly.express as px
import psycopg2
import dash
from dash import Dash, html, dcc, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from sqlalchemy import text
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

# load .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --- build your Dash app ---
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # for WSGI

# --- helper to get a fresh DB connection ---
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST", "localhost"),
        port=int(os.getenv("DATABASE_PORT", 5432)),
    )

def register_callbacks(app):
    # --- the callback must be a plain function, AFTER app & layout are defined ---
    @callback(
        Output("total-transactions", "children"),
        Output("total-amount",       "children"),
        Input("metrics-interval",    "n_intervals"),
    )
    def update_overall_metrics(n_intervals):
        try:
            # Explicitly convert to string to prevent SQLAlchemy clause issues
            query = """
                SELECT total_transactions, transaction_volume
                FROM transaction_metrics
                ORDER BY id DESC
                LIMIT 1
            """
            
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)  # Explicitly wrap with text()
                    row = cur.fetchone()
            
            if not row:
                logging.warning("No data found in transaction_metrics")
                return "--", "--"
            
            return f"{row[0]:,}", f"KES {row[1]:,.2f}"  # Added currency prefix
            
        except Exception as e:
            logging.error(f"Callback failed: {str(e)}", exc_info=True)
            return "--", "--"
    
        # 3.2 heatmap
    @app.callback(
        Output("peak-hours-heatmap", "figure"),
        Input("heatmap-interval", "n_intervals"),
    )
    def update_heatmap(n):
        try:
            # Get data using raw connection
            with get_connection() as conn:
                # Read data into DataFrame
                df = pd.read_sql_query("""
                    SELECT * FROM peak_hours
                    ORDER BY day_of_week
                """, conn, index_col="day_of_week")
            
            # Ensure all days and hours are present
            days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            hours = [str(h) for h in range(1, 25)]
            
            # Reindex and fill missing values
            df = df.reindex(
                index=days,
                columns=hours,
                fill_value=0
            ).astype(int)
            
            # Create heatmap figure
            fig = px.imshow(
                df.values,
                x=df.columns,
                y=df.index,
                labels=dict(x="Hour", y="Day", color="Transactions"),
                color_continuous_scale="RdBu",
                aspect="auto",
                zmin=0,  # Ensure scale starts at 0
                zmax=df.values.max()
            )
            
            # Style adjustments
            fig.update_layout(
                margin=dict(l=40, r=20, t=30, b=40),
                xaxis_title="Hour of Day",
                yaxis_title="Day of Week",
                coloraxis_colorbar=dict(title="Count")
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Heatmap update failed: {str(e)}", exc_info=True)
            # Return empty figure on error
            return px.imshow([[0]], labels=dict(x="Error", y="", color=""))

    # 3.3 transaction trends
    @app.callback(
        Output("transaction-trends-graph", "figure"),
        Input("trends-interval", "n_intervals"),
        Input("timescale-toggle", "value"),
    )
    def update_trends(n, scale):
        try:
            # Validating the scale to prevent SQL injection
            valid_scales = {"daily", "weekly", "monthly"}
            if scale not in valid_scales:
                raise ValueError(f"Invalid timescale: {scale}")
            
            with get_connection() as conn:
                df = pd.read_sql_query(f"""
                    SELECT transaction_time, total_transactions, total_amount 
                    FROM {scale}_trends
                    ORDER BY transaction_time
                """, conn, parse_dates=["transaction_time"])

            # Normalizing creating the figure
                df["total_transactions"] = (df["total_transactions"] - df["total_transactions"].min()) / \
                                (df["total_transactions"].max() - df["total_transactions"].min())
                df["total_amount"] = (df["total_amount"] - df["total_amount"].min()) / \
                                (df["total_amount"].max() - df["total_amount"].min())
            
            # The visualization
            fig = px.line(
                df,
                x="transaction_time",
                y=["total_transactions", "total_amount"],
                labels={
                    "transaction_time": "Date",
                    "value": "Count/Amount",
                    "variable": "Metric"
                },
                markers=True,
                title=f"{scale.capitalize()} Transaction Trends"
            )
            fig.update_layout(
                legend_title_text="Metric (normalized)",
                margin=dict(l=40, r=20, t=60, b=40),
                hovermode="x unified",
                yaxis_title="Count/Amount (KES)"
            )
            fig.update_yaxes(
                tickprefix="KES ",
                tickformat=",.0f"
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Trends update failed: {str(e)}", exc_info=True)
            # Return empty figure with error message
            return px.line(
                title="Error loading data",
                labels={"x": "Error", "y": ""}
            ).add_annotation(
                text=f"Error: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )


    # 3.4 top customers
    @app.callback(
        Output("top-customers-table", "data"),
        Input("customers-interval", "n_intervals"),
    )
    def update_top_customers(n):
        logging.info(f"Updating top customers (tick #{n})")
        try:
            # Get data using your connection method
            with get_connection() as conn:
                df = pd.read_sql_query("""
                    SELECT
                        msisdn, 
                        last_seen, 
                        loyalty_score, 
                        avg_spend, 
                        total_transactions
                    FROM customers
                    ORDER BY loyalty_score DESC
                    
                """, conn, parse_dates=["last_seen"])
            
            # Format data for display
            df["last_seen"] = df["last_seen"].apply(format_with_ordinal)
            df["loyalty_score"] = df["loyalty_score"].round(2)
            df["avg_spend"] = df["avg_spend"].round(2).apply(lambda x: f"KES {x:,.2f}")
            df["total_transactions"] = df["total_transactions"].apply(lambda x: f"{x:,}")
            
            return df.to_dict("records")
            
        except Exception as e:
            logging.error(f"Failed to fetch top customers: {str(e)}", exc_info=True)
            # Return empty dataset with error message
            return [{
                "msisdn": "Error",
                "last_seen": "N/A",
                "loyalty_score": 0,
                "avg_spend": f"Error: {str(e)}",
                "total_transactions": "N/A"
            }]

    @app.callback(
        Output("customer-segments-scatter", "figure"),
        Input("segments-interval", "n_intervals"),
    )
    def update_segments(n):
        logging.info(f"Updating customer segments with PCA (tick #{n})")
        try:
            with get_connection() as conn:
                # Get all relevant customer metrics
                df = pd.read_sql_query("""
                    SELECT 
                        total_transactions,
                        total_spend,
                        avg_spend,
                        days_since_last,
                        churn_score,
                        loyalty_score,
                        EXTRACT(DAY FROM NOW() - first_seen)::INTEGER as customer_age_days,
                        clv,
                        r_score,
                        f_score,
                        m_score
                    FROM customers
                    WHERE NOT is_churned
                """, conn)

            # Define feature columns
            feature_cols = [
                'total_transactions', 'total_spend', 'avg_spend',
                'days_since_last', 'churn_score', 'loyalty_score',
                'customer_age_days', 'clv', 'r_score', 'f_score', 'm_score'
            ]
            
            # Check for missing features
            missing_cols = [col for col in feature_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing columns in data: {missing_cols}")

            # Standardize features
            from sklearn.preprocessing import StandardScaler
            from sklearn.decomposition import PCA
            from sklearn.cluster import KMeans
            
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(df[feature_cols])
            
            # Apply PCA - store as instance variable for transform consistency
            if not hasattr(app, 'pca'):
                app.pca = PCA(n_components=2)
                principal_components = app.pca.fit_transform(X_scaled)
            else:
                principal_components = app.pca.transform(X_scaled)
            
            # Cluster in PCA space
            if not hasattr(app, 'kmeans'):
                app.kmeans = KMeans(n_clusters=4, random_state=42)
                df['segment'] = app.kmeans.fit_predict(principal_components)
            else:
                df['segment'] = app.kmeans.predict(principal_components)
            
            # Create visualization
            fig = px.scatter(
                df,
                x=principal_components[:, 0],
                y=principal_components[:, 1],
                color="segment",
                hover_data=feature_cols,
                title="Customer Segments (PCA Projection)",
                labels={
                    "x": f"PC1 ({app.pca.explained_variance_ratio_[0]:.1%})",
                    "y": f"PC2 ({app.pca.explained_variance_ratio_[1]:.1%})"
                }
            )
            
            # Style improvements
            fig.update_layout(
                margin=dict(l=40, r=40, t=80, b=40),
                legend_title_text="Segment",
                hovermode="closest"
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"PCA segmentation failed: {str(e)}", exc_info=True)
            return px.scatter().add_annotation(
                text=f"Error: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )

    def format_with_ordinal(dt):
        suffix = lambda d: 'th' if 11 <= d <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')
        return dt.strftime(f"%-d{suffix(dt.day)} %B, %-I:%M %p")