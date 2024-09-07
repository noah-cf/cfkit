import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/06_gathered_data_report_input.xlsx')
output_file = os.path.join(script_dir, '../../../data/ingredient_intelligence_reports/06_gathered_data_report_output.xlsx')

df = pd.read_excel(input_file)

df = df.drop(columns=['final_id', 'final_source'])

if 'count' not in df.columns:
    df['count'] = 1
else:
    df['count'] = df['count'].fillna(1)

def transform_casrn_column(row):
    if isinstance(row, str):
        casrns = [d['additional_casrn'] for d in eval(row)]
        return ', '.join(casrns)
    else:
        return ''

df['final_additional_casrns'] = df['final_additional_casrns'].apply(transform_casrn_column)

df['Chemical Count'] = df.groupby('Final CAS')['count'].transform('sum')

df = df.rename(columns={
    'final_additional_casrns': 'Additional CAS',
    'final_ec_number': 'EC Number',
    'final_name': 'Ingredient Name',
    'final_inci': 'INCI',
    'final_hazard_band': 'Hazard Band',
    'final_rollup_score': 'C2C Score',
    'final_tco': 'TCO Status',
    'final_scil': 'SCIL Status',
    'final_status': 'ChemFORWARD Status'
})

df['Hazard Band'] = df['Hazard Band'].replace('z', '?').str.upper()

df['ChemFORWARD Status'] = df['ChemFORWARD Status'].replace({
    'pharos': 'Curated Chemical',
    'screening_only': 'Curated Chemical',
    'awaiting_assessor_response': 'Assessment in Progress',
    'awaiting_verifier_response': 'Assessment in Progress',
    'draft': 'Assessment in Progress',
    'submitted': 'Assessment in Progress',
    'in_review': 'Assessment in Progress',
    'verified': 'Full Chemical Hazard Assessment'
})

df['SCIL Status'] = df['SCIL Status'].replace({
    'unlisted': '',
    'green_circle': 'Full Green Circle',
    'green_half_circle': 'Half Green Circle',
    'yellow_triangle': 'Yellow Triangle',
    'tentative': 'Tentative'
})

df['TCO Status'] = df['TCO Status'].replace({
    'bm2': 'BM-2',
    'bm3': 'BM-3',
    'unlisted': '',
    'tentative': 'Tentative'
})

df['Final CAS'] = df['Final CAS'].astype(str).replace('nan', '')
df['EC Number'] = df['EC Number'].astype(str).replace('nan', '')
df['Additional CAS'] = df['Additional CAS'].astype(str)

cols_to_move = ['Valid Original',
                'Corrected CAS',
                'Final Valid',
                'Chemical Count',
                'Final CAS',
                'Additional CAS',
                'EC Number',
                'Ingredient Name',
                'INCI',
                'Hazard Band',
                'C2C Score',
                'ChemFORWARD Status',
                'SCIL Status',
                'TCO Status'
]

df = df[[col for col in df if col not in cols_to_move]
        + [col for col in cols_to_move if col in df]]

df.to_excel(output_file, index=False)
