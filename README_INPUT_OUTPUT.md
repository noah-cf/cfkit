# Project README: File Path Documentation

This document provides a reference for the file paths used in various scripts within the project.

---

### **Assessment Properties Tracker**
- **Location:** `cfkit/src/assessment_properties_tracker`
- **Output Directory:** `data/assessment_properties_tracker/{current_date}_output`
- **Output File:** `{current_date}_assessment_properties_tracker_output.xlsx`

---

### **Assessment Tracker**
- **Location:** `cfkit/src/assessment_tracker`
- **Output Directory:** `data/assessment_tracker/{current_date_folder}_assessment_tracker_output`
- **Output File:** `{current_date_folder}_assessment_tracker_output.xlsx`

---

### **CAS Mapping**
- **Location:** `cfkit/src/cas_mapping`
- **Input File:** `data/cas_mapping/input.xlsx`
- **Input File Notes**: File must have a column named 'cas'
- **Output Directory:** `data/cas_mapping/{current_date}_output`
- **Other Files:** 
  - `assets/cas_mapping/pharos_profiles.csv`
  - `assets/cas_mapping/cas_maps.xlsx`

---

### **Endpoint Report**
- **Location:** `cfkit/src/endpoint_report`
- **Base Directory:** `data/endpoint_reports/{current_date}`
- **Subdirectories:** 
  - `latest_versions_verified`
  - `endpoints`
  - `endpoint_ratings` ### Main output
- **Output Files:** 
  - `ghs_endpoints.csv`
  - `mw_endpoints.csv`
  - `ghs_ratings.csv` ### Main output
  - `mw_ratings.csv` ### Main output

---

### **H Statement Report**
- **Assessor Assigned Statements Script:**
  - **Location:** `cfkit/src/h_statement_report/scripts/assessor_assigned_statements.py`
  - **Output Directories:**
    - `data/h_statement_reports/assessor_assigned_statements/{current_date}`

- **Harmonized Statements Script:**
  - **Location:** `cfkit/src/h_statement_report/scripts/harmonized_statements.py`
  - **Output Directory:** `data/h_statement_reports/harmonized_statements`
  - **Output File:** `harmonized_statements_{current_date}.xlsx`

---

### **Ingredient Intelligence Report**
- **Location:** `cfkit/src/ingredient_intelligence_report`
- **Input Files:**
  - `data/ingredient_intelligence_reports/01_cas_cleaning_input.xlsx`
- **Output Files:**
  - `data/ingredient_intelligence_reports/01_cas_cleaning_output.xlsx`

---

### **Small Tasks**
- **Merge with Delimiter Script:**
  - **Input File:** Whatever .xlsx or .csv is within the folder `data/small_tasks/merge_by_delimiter`
  - **Output Directory:** `data/small_tasks/split_by_delimiter/{current_date}_output`

- **Split by Delimiter Script:**
  - **Input File:** Whatever .xlsx or .csv is within the folder `data/small_tasks/split_by_delimiter`
  - **Output Directory:** `data/small_tasks/split_by_delimiter/{current_date}_output`

---

### **User Report**
- **Output Directory:** `data/user_reports/{current_date_folder}_user_report_output`

---

### **Visualization Scripts**
- **Functions: Question Marks Null Script:**
  - **Input File**: Whatever .xlsx or .csv is within the folder `data/visualizations/function_bubbles/question_marks_quarter`
  - **Input File Guide**: Must contain the columns: 'Function Group', 'Usage Count', 'Hazard Score'.
  - **Output Directory:** `data/visualizations/function_bubbles/question_marks_null`

- **Functions: Question Marks Quarter Script:**
  - **Input File**: Whatever .xlsx or .csv is within the folder `data/visualizations/function_bubbles/question_marks_quarter`
  - **Input File Guide**: Must contain the columns: 'Function Group', 'Usage Count', 'Hazard Score'.
  - **Output Directory:** `data/visualizations/function_bubbles/question_marks_quarter`

---
