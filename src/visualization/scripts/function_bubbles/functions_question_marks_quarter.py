import os
import pandas as pd
import plotly.graph_objects as go
from shutil import copyfile
from datetime import datetime

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../data/visualizations/function_bubbles/question_marks_quarter"))

input_files = [f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f))]

supported_files = [f for f in input_files if f.endswith(('.xlsx', '.csv'))]

if not supported_files:
    raise FileNotFoundError(f"No valid .xlsx or .csv input file found in {base_dir}")

input_file_name = supported_files[0]
input_file_path = os.path.join(base_dir, input_file_name)

file_extension = os.path.splitext(input_file_name)[1].lower()

current_date = datetime.now().strftime('%Y_%m_%d')

script_name = 'function_bubbles_quarter'
output_directory = os.path.join(base_dir, f'{current_date}_{script_name}_output')
os.makedirs(output_directory, exist_ok=True)

input_copy_path = os.path.join(output_directory, f'{current_date}_{script_name}_input{file_extension}')
copyfile(input_file_path, input_copy_path)
output_file_path = os.path.join(output_directory, f'{current_date}_{script_name}_distribution.html')

if file_extension == '.xlsx':
    df = pd.read_excel(input_file_path, engine='openpyxl')
elif file_extension == '.csv':
    df = pd.read_csv(input_file_path)
else:
    raise ValueError(f"Unsupported file type: {file_extension}. Only .xlsx and .csv are supported.")

df['Usage Count'] = pd.to_numeric(df['Usage Count'], errors='coerce')
hazard_score_mapping = {'A': 1, 'B': 0.9, 'C': 0.5, 'D': 0.1, 'F': 0.0, '?': 0.25, 'U': None}
df['Hazard Score Numeric'] = df['Hazard Score'].map(hazard_score_mapping)

function_group_data = df.groupby('Function Group').agg({
    'Usage Count': 'sum',
    'Hazard Score Numeric': 'mean'
}).reset_index()

fig = go.Figure()

for _, row in function_group_data.iterrows():
    fig.add_trace(go.Scatter(
        x=[row['Usage Count']],
        y=[row['Hazard Score Numeric']],
        mode='markers+text',
        text=row['Function Group'],
        textposition='top center',
        marker=dict(
            size=row['Usage Count'] / function_group_data['Usage Count'].max() * 100,
            color=row['Hazard Score Numeric'],
            showscale=True
        ),
        name=row['Function Group'],
        hovertemplate=(
            "Function Group: %{text}<br>"
            "Usage Count: %{x}<br>"
            "Avg. Hazard Score: %{y:.2f}"
        ),
        showlegend=True
    ))

fig.update_layout(
    title='Function Group Bubble Distribution by Hazard Score and Usage Count',
    xaxis_title='Total Usage Count',
    yaxis_title='Average Hazard Score',
    showlegend=True,
    legend=dict(orientation='v', x=1, y=1),
    font=dict(size=12),
    margin=dict(l=50, r=50, t=50, b=50),
    yaxis=dict(range=[0, 1])
)

fig.write_html(output_file_path)
fig.show()

print(f"Visualization saved to {output_file_path}")
print(f"Input file copied to {input_copy_path}")
