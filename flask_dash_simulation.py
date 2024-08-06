"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script mainly acts as a testing tool for displaying and 
                reflecting anomaly predictions at high intervals, allowing us 
                to verify the real-time data update and anomaly detection 
                functionality using Flask and Dash.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			09-Jul-2024		TSHN	Initial creation	
================================================================================
"""

from flask import Flask
import dash
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

# Initialize Flask app
server = Flask(__name__)

# Initialize Dash app
app = dash.Dash(__name__, server=server)

# Layout of the Dash app
app.layout = html.Div([
    html.H3("Real-Time Data Feed with Anomaly Prediction"),
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds
        n_intervals=0
    ),
    dash_table.DataTable(
        id='real-time-table',
        columns=[
            {'name': 'Date_Time', 'id': 'Date_Time'},
            {'name': 'unit_names', 'id': 'unit_names'},
            {'name': 'I_MEAS', 'id': 'I_MEAS'},
            {'name': 'TC_LD', 'id': 'TC_LD'},
            {'name': 'TC_CMB', 'id': 'TC_CMB'},
            {'name': 'TC_CPS', 'id': 'TC_CPS'},
            {'name': 'PD1', 'id': 'PD1'},
            {'name': 'PD2', 'id': 'PD2'},
            {'name': 'is_anomaly_pred', 'id': 'is_anomaly_pred'},
        ],
        style_table={'height': 'auto', 'overflowY': 'hidden'},  # Adjust height to 'auto' and hide overflow
        style_cell={'textAlign': 'left'},
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold',
        },
        style_data_conditional=[
            {
                'if': {'filter_query': '{is_anomaly_pred} = -1'},
                'backgroundColor': 'darkred',
                'color': 'white',
            },
        ],
    )
])

# Load the model
model = joblib.load('Created_files/best_isolation_forest_model.pkl')

# Load the dataframe and drop the 'is_anomaly_pred' and 'is_anomaly_truth' column temporarily for simulation
df_simulate = pd.read_pickle("Created_files/no_psu_with_fake_data_df_train.pkl")
df_simulate = df_simulate.drop(columns=['is_anomaly_pred', 'is_anomaly_truth'])

# For now, create another 16units, by changing the unit_names column
new_unit_names = ['L01'] + [f'L{str(i).zfill(2)}' for i in range(17, 33)]

# Select all from the current DataFrame
new_created_df = df_simulate.sample(frac=1, random_state=42)

# Assign a random new unit_name to these rows
new_created_df['unit_names'] = np.random.choice(new_unit_names, size=len(new_created_df))

# Combine the new and original DataFrames
final_df = pd.concat([df_simulate, new_created_df])

# Group by unit_names and convert to a dictionary of DataFrames
grouped_df_dict = {unit_name: group for unit_name, group in final_df.groupby('unit_names')}

# Function to predict anomaly and update the row
def predict_anomaly_and_update_row(row, model):
    # Prepare row data for prediction
    row_data = row.drop(columns=['unit_names', 'Date_Time'])  # Exclude non-feature columns
    
    # Predict using the model
    anomaly_label = model.predict(row_data)[0]  # Get the anomaly prediction (1 for outlier, -1 for inlier)
    row['is_anomaly_pred'] = int(anomaly_label)  # Add prediction to the row
    
    return row

# Function to generate real-time feed DataFrame
def generate_realtime_feed():
    # Create a DataFrame for real-time feed
    realtime_feed_df = pd.DataFrame()

    # Iterate over each group and select one row, update Date_Time, and predict anomaly
    for unit_name, group in grouped_df_dict.items():
        row = group.sample(n=1)  # Randomly select one row
        row.insert(row.columns.get_loc('PD2'), 'PD1', 9.99)
        row['Date_Time'] = datetime.now()  # Update Date_Time to current time
        
        # Predict anomaly and update row
        row = predict_anomaly_and_update_row(row, model)
        
        # Append to the real-time feed DataFrame
        realtime_feed_df = pd.concat([realtime_feed_df, row])
    
    return realtime_feed_df

# Dash callback to update table
@app.callback(
    Output('real-time-table', 'data'),
    Input('interval-component', 'n_intervals')
)

def update_table(n):
    realtime_feed_df = generate_realtime_feed()
    return realtime_feed_df.to_dict('records')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
