import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import pandas as pd
import threading
import webbrowser
import time
from constants import OUTPUT_PATH
from pathlib import Path

performance_df = pd.DataFrame({"Date": [], "Ticker": [], "MarketCap": [], "Price": []})
composition_df = pd.DataFrame({"Date": [], "Ticker": [], "MarketCap": [], "Price": []})
changes_df = pd.DataFrame({"Date": [], "Ticker": [], "MarketCap": [], "Price": []})

# Load data
if Path(f"{OUTPUT_PATH}index_performance.csv").exists():
    performance_df = pd.read_csv(f"{OUTPUT_PATH}index_performance.csv")
if Path(f"{OUTPUT_PATH}daily_composition.csv").exists():
    composition_df = pd.read_csv(f"{OUTPUT_PATH}daily_composition.csv")
if Path(f"{OUTPUT_PATH}composition_changes.csv").exists():
    changes_df = pd.read_csv(f"{OUTPUT_PATH}composition_changes.csv")

# Convert dates to datetime
performance_df['Date'] = pd.to_datetime(performance_df['Date'])
composition_df['Date'] = pd.to_datetime(composition_df['Date'])
changes_df['Date'] = pd.to_datetime(changes_df['Date']).dt.date

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server

# ======================================================================
# Layout Configuration
# ======================================================================
app.layout = html.Div([
    # Summary Metrics Strip (Top)
    html.Div([
        html.Div(id='summary-metrics', style={
            'display': 'flex',
            'justifyContent': 'space-between',
            'gap': '8px',
            'height': '70px',
            'padding': '10px'
        })
    ], style={
        'margin': '4px',
        'padding': '6px',
        'backgroundColor': '#333333',
        'height': '90px',
        'borderRadius': '8px'
    }),
    
    # Upper Section (Chart + Changes Table)
    html.Div([
        # Performance Chart
        html.Div(
            dcc.Graph(id='performance-chart', style={'height': '247px'}),
            style={'flex': 1, 'marginRight': '4px', 'backgroundColor': '#444', 'borderRadius': '8px', 'padding': '8px'}
        ),
        
        # Composition Changes Table
        html.Div([
            html.Div("Composition Changes", style={
                'fontSize': '14px', 
                'marginBottom': '4px',
                'fontWeight': '600',
                'color': 'white',
                'height':'240',
                'text-align': 'center'
            }),
            dash_table.DataTable(
                id='changes-table',
                style_table={
                    'height': '220px',
                    'overflowY': 'auto'
                },
                style_cell={
                    'padding': '3px',
                    'fontSize': '14px',
                    'border': '1px solid #555',
                    'backgroundColor': '#333',
                    'color': 'white',
                    'textAlign': 'center'
                },
                style_header={
                    'backgroundColor': '#555',
                    'fontWeight': '600',
                    'color': 'white',
                    'textAlign': 'center'
                }
            )
        ], style={'flex': 1, 'marginLeft': '4px', 'backgroundColor': '#444', 'borderRadius': '8px', 'padding': '8px'})
    ], style={'display': 'flex', 'gap': '8px', 'margin': '8px', 'height': '260px'}),
    
    # Vertical Spacer
    html.Div(style={'height': '8px'}),
    
    # Lower Section (Composition Analysis)
    html.Div([
        # Left Column
        html.Div([
            html.Div([
                html.Div("Index Composition-Top 10", style={
                    'fontSize': '15px',
                    'marginBottom': '4px',
                    'fontWeight': '600',
                    'color': 'white',
                'text-align': 'center'
                }),
                dcc.DatePickerSingle(
                    id='date-picker',
                    min_date_allowed=composition_df['Date'].min(),
                    max_date_allowed=composition_df['Date'].max(),
                    date=composition_df['Date'].max(),
                    display_format='YYYY-MM-DD',
                    style={'marginBottom': '6px'}
                ),
                dcc.Graph(id='composition-chart', style={'height': '240px'})
            ], style={'backgroundColor': '#444', 'borderRadius': '8px', 'padding': '8px'})
        ], style={'flex': 1, 'marginRight': '4px'}),
        
        # Right Column
        html.Div([
            html.Div("Composition Details", style={
                'fontSize': '14px',
                'marginBottom': '4px',
                'fontWeight': '600',
                'color': 'white',
                'text-align': 'center'
            }),
            dash_table.DataTable(
                id='composition-table',
                style_table={
                    'height': '280px',
                    'overflowY': 'auto'
                },
                style_cell={
                    'padding': '3px',
                    'fontSize': '14px',
                    'border': '1px solid #555',
                    'backgroundColor': '#333',
                    'color': 'white',
                    'textAlign': 'center'
                },
                style_header={
                    'backgroundColor': '#555',
                    'fontWeight': '600',
                    'color': 'white',
                    'textAlign': 'center'
                }
            )
        ], style={'flex': 1, 'marginLeft': '4px', 'backgroundColor': '#444', 'borderRadius': '8px', 'padding': '8px'})
    ], style={'display': 'flex', 'gap': '8px', 'margin': '8px', 'height': '320px'})
])

# ======================================================================
# Callbacks
# ======================================================================
@app.callback(
    Output('performance-chart', 'figure'),
    Input('performance-chart', 'relayoutData')
)
def update_performance_chart(_):
    fig = px.line(performance_df, x='Date', y='Cumulative_Value', labels={'Cumulative_Value': 'Index Value'})
    
    # Add vertical lines for composition changes
    for change_date in changes_df['Date']:
        fig.add_vline(x=change_date, line_dash="dot", line_color="red")
    
    # Add gridlines
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='#555')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='#555')
    
    fig.update_layout(
        hovermode="x unified",
        plot_bgcolor='#222',
        paper_bgcolor='#222',
        font_color='white',
        margin=dict(l=20, r=20, t=20, b=20),
        height=240
    )
    return fig

@app.callback(
    [Output('composition-chart', 'figure'),
     Output('composition-table', 'data')],
    [Input('date-picker', 'date')]
)
def update_composition(selected_date):
    filtered = composition_df[composition_df['Date'] == selected_date]
    
    # Bar chart
    bar_fig = px.bar(filtered.nlargest(10, 'MarketCap'), 
                    x='Ticker', y='Weight',
                    labels={'Weight': 'Weight (%)'})
    
    bar_fig.update_layout(
        plot_bgcolor='#222',
        paper_bgcolor='#222',
        font_color='white',
        margin=dict(l=20, r=20, t=30, b=20),
        height=240
    )
    
    # Table data
    table_data = filtered.sort_values('MarketCap', ascending=False).to_dict('records')
    
    return bar_fig, table_data

@app.callback(
    Output('changes-table', 'data'),
    Input('changes-table', 'page_current')
)
def update_changes_table(_):
    return changes_df.sort_values('Date', ascending=False).to_dict('records')

@app.callback(
    Output('summary-metrics', 'children'),
    Input('date-picker', 'date')
)
def update_summary_metrics(selected_date):
    if selected_date is None:
        selected_date = composition_df['Date'].max()
    else:
        selected_date = pd.to_datetime(selected_date)
    
    daily_data = performance_df[performance_df['Date'] == selected_date]
    cumulative_value = daily_data['Cumulative_Value'].iloc[0] if not daily_data.empty else 0
    daily_return = daily_data['Daily_Return'].iloc[0] if not daily_data.empty else 0
    num_changes = len(changes_df)
    
    metrics = [
        ("Cumulative Value", f"{cumulative_value:.2f}"),
        ("Daily Change", f"{daily_return*100:.2f}%"),
        ("Total Changes", str(num_changes))
    ]
    
    return [
        html.Div(
            [
                html.Div(label, style={'fontSize': '17px', 'marginBottom': '2px', 'color': 'white'}),
                html.Div(value, style={'fontSize': '19px', 'fontWeight': '600', 'color': 'white'})
            ],
            style={
                'border': '1px solid #555',
                'padding': '6px',
                'flex': 1,
                'textAlign': 'center',
                'backgroundColor': '#222',
                'borderRadius': '8px'
            }
        ) for label, value in metrics
    ]

# =========ii=============================================================
# Run Server
# ======================================================================
def open_browser():
    time.sleep(1)
    webbrowser.open_new('http://localhost:8050/')

def main():
    threading.Thread(target=app.run_server, kwargs={'debug': True, 'use_reloader': False}).start()
    open_browser()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
