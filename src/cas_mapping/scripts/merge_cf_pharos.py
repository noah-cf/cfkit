import pandas as pd
import os

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../assets"))
output_dir = os.path.join(base_dir, "cas_mapping")

os.makedirs(output_dir, exist_ok=True)

profiles_path = os.path.join(base_dir, "profiles.xlsx")
pharos_path = os.path.join(base_dir, "pharos.xlsx")

output_file_path = os.path.join(output_dir, "pharos_profiles.csv")

profiles = pd.read_excel(profiles_path)
profiles = profiles[["id",
                     "name",
                     "cas_rn",
                     "inci",
                     "status",
                     "scil_status",
                     "tco_status",
                     "manual_hazard_band_score",
                     "manual_rollup_score",
                     "additional_casrns"
]]

pharos = pd.read_excel(pharos_path)
pharos = pharos[["id", "casrn", "name", "scil_status", "tco_status", "hazard_band_score"]]

profiles = profiles.rename(columns={
    "id": "id",
    "cas_rn": "casrn",
    "name": "name",
    "inci": "inci",
    "scil_status": "scil_status",
    "manual_hazard_band_score": "hazard_band",
    "manual_rollup_score": "c2c_score",
    "additional_casrns": "additional_casrn"
})

pharos = pharos.rename(columns={
    "id": "id",
    "casrn": "casrn",
    "name": "name",
    "scil_status": "scil_status",
    "tco_status": "tco_status",
    "hazard_band_score": "hazard_band"
})

profiles['hazard_band'] = profiles['hazard_band'].replace('z', '?')
pharos = pharos[~pharos['casrn'].isin(profiles['casrn']) | pharos['casrn'].isna()]
combined = pd.concat([profiles, pharos], ignore_index=True)

combined.loc[combined['status'].isna(), 'status'] = 'pharos_screening'
combined = combined.sort_values(by='id')
combined = combined[
    ['id',
     'casrn',
     'additional_casrn',
     'name',
     'inci',
     'status',
     'hazard_band',
     'c2c_score',
     'scil_status',
     'tco_status'
]]

combined.to_csv(output_file_path, index=False)

print(f"Data successfully saved to {output_file_path}")
