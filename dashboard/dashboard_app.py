import dash 
import logging
from dash import Dash, html, dcc, callback
from sqlalchemy import create_engine, text
from dash.dependencies import Input, Output
from dash import dash_table
from .callbacks import register_callbacks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logging.getLogger('dash').setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.INFO)

# Initialize Dash app (without Bootstrap)
app = Dash(__name__)
app.title = "M-Pesa Analytics Dashboard"

# Custom CSS for styling
app.layout = html.Div(style={
    'fontFamily': 'Arial, sans-serif',
    'padding': '20px',
    'backgroundColor': '#f9f9f9'  # Light background
}, children=[

    # Minimal Navbar
    html.Div(style={
        'borderBottom': '1px solid #ccc',
        'padding': '10px 0',
        'marginBottom': '30px'
    }, children=[
        html.H1("M-Pesa Analytics", style={
            'margin': '0',
            'color': '#333',
            'fontSize': '28px'
        })
    ]),

    # Top-line metrics
    # Top-line metrics (stacked, minimal, top-left)
    html.Div(style={
        'marginBottom': '30px'
    }, children=[
        html.Div(style={
            'backgroundColor': '#fff',
            'padding': '12px 16px',
            'borderLeft': '4px solid #375a7f',
            'marginBottom': '10px',
            'borderRadius': '4px',
            'maxWidth': '300px'
        }, children=[
            html.H4("Total Transactions", style={'margin': '0 0 5px', 'color': '#666'}),
            html.H3("389", id="total-transactions", style={'margin': 0, 'color': '#375a7f'})
        ]),
        html.Div(style={
            'backgroundColor': '#fff',
            'padding': '12px 16px',
            'borderLeft': '4px solid #375a7f',
            'borderRadius': '4px',
            'maxWidth': '300px'
        }, children=[
            html.H4("Total Amount Received", style={'margin': '0 0 5px', 'color': '#666'}),
            html.H3("KES 834,798.23", id="total-amount", style={'margin': 0, 'color': '#375a7f'})
        ])
    ]),

     # Top customers in a card
    html.Div(style={
        'backgroundColor': 'white',
        'padding': '20px',
        'borderRadius': '12px',
        'boxShadow': '0 4px 12px rgba(0, 0, 0, 0.06)',
        'marginBottom': '30px'
    }, children=[
        html.H3("Top Customers by Loyalty Score", style={
            'color': '#333',
            'marginBottom': '20px',
            'fontWeight': '600'
        }),
        dash_table.DataTable(
            id="top-customers-table",
            columns=[
                {"name": c.replace("_", " ").title(), "id": c}
                for c in ["msisdn", "last_seen", "loyalty_score", "avg_spend", "total_transactions"]
            ],
            page_size=5,
            style_table={
                'overflowX': 'auto',
                'borderRadius': '10px',
                'overflow': 'hidden',
            },
            style_header={
                'backgroundColor': '#f1f3f6',
                'color': '#333',
                'fontWeight': '600',
                'border': 'none',
                'padding': '12px',
            },
            style_cell={
                'padding': '12px',
                'fontSize': '15px',
                'textAlign': 'left',
                'border': 'none',
                'fontFamily': 'Segoe UI, sans-serif',
            },
            style_data={
                'border': 'none',
                'backgroundColor': '#ffffff'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f9f9f9'
                },
                {
                    'if': {'state': 'active'},
                    'backgroundColor': '#e8f0fe',
                    'border': 'none'
                }
            ],
            style_as_list_view=True,
            sort_action="native"
        )
    ]),


    # Heatmap and scatter plots
    html.Div(style={
        'display': 'flex',
        'gap': '20px',
        'marginBottom': '30px'
    }, children=[

        # Heatmap Card
        html.Div(style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 8px rgba(0,0,0,0.05)',
            'flex': '1'
        }, children=[
            html.H3("Peak Hours Heatmap", style={'color': '#333'}),
            dcc.Graph(
                id="peak-hours-heatmap",
                config={"displayModeBar": False},
                style={'height': '450px'}
            )
        ]),

        # Scatterplot Card
        html.Div(style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 8px rgba(0,0,0,0.05)',
            'flex': '1'
        }, children=[
            html.H3("Customer Segments Distribution", style={'color': '#333'}),
             dcc.Graph(
                id="customer-segments-scatter",
                style={'height': '500px', 'width': '100%'},
                config={"displayModeBar": False}
            )
        ])
    ]),

    # Trends
    html.Div(style={'marginBottom': '30px'}, children=[
        html.H3("Transaction Trends", style={'color': '#333'}),
        dcc.RadioItems(
            id="timescale-toggle",
            options=[
                {"label": "Daily", "value": "daily"},
                {"label": "Weekly", "value": "weekly"},
                {"label": "Monthly", "value": "monthly"},
            ],
            value="daily",
            style={'marginBottom': '15px'},
            labelStyle={'marginRight': '15px'}
        ),
        dcc.Graph(
            id="transaction-trends-graph",
            config={"displayModeBar": False}
        )
    ]),

    # Intervals with optimized refresh rates
    dcc.Interval(
        id="metrics-interval",
        interval=60 * 1_000,  # 1 minute (60,000ms)
        n_intervals=0
    ),
    dcc.Interval(
        id="heatmap-interval",
        interval=60 * 60 * 1_000,  # 1 hour (3,600,000ms)
        n_intervals=0
    ),
    dcc.Interval(
        id="trends-interval", 
        interval=24 * 60 * 60 * 1_000,  # 1 day (86,400,000ms)
        n_intervals=0
    ),
    dcc.Interval(
        id="customers-interval",
        interval=7 * 24 * 60 * 60 * 1_000,  # 1 week (604,800,000ms)
        n_intervals=0
    ),
    dcc.Interval(
        id="segments-interval",
        interval=7 * 24 * 60 * 60 * 1_000,  # 1 week (604,800,000ms)
        n_intervals=0
    )
])


register_callbacks(app=app)

server = app.server

def run():
    app.run(debug=True, host="0.0.0.0", port=8050, use_reloader=False)