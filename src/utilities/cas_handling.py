import re
import pandas as pd

def verify_cas(cas):
    """
    Verifies the correctness of CAS numbers using checksum validation.
    """
    cas = str(cas).replace('-', '').strip()
    if not cas.isdigit():
        return False
    reverse_cas = cas[::-1]
    checksum = int(reverse_cas[0])
    rest = reverse_cas[1:]
    expected_checksum = sum(int(digit) * (index + 1) for index, digit in enumerate(rest)) % 10
    return checksum == expected_checksum

def correct_cas(cas):
    """
    Corrects common formatting errors in CAS numbers, including leading zeros, date formatting, and whitespace.
    """
    cas = re.sub(r'\s*-\s*', '-', str(cas).strip())
    cas = cas.replace(' 00:00:00', '')
    parts = cas.split('-')
    if len(parts) == 3:
        parts = [part.lstrip('0') for part in parts]
        cas = '-'.join(parts)
    return cas

def finalize_cas(row, cas_column='cas'):
    """
    Determines the final CAS number based on validation and correction steps.
    """
    original = row[cas_column]
    corrected = correct_cas(original)
    if verify_cas(original):
        return original
    elif verify_cas(corrected):
        return corrected
    return 'invalid_cas'

def process_cas_dataframe(df, cas_column='cas'):
    """
    Applies CAS verification and correction across a DataFrame and adds a final CAS column.
    """
    df['Corrected CAS'] = df[cas_column].apply(correct_cas)
    df['Valid Original'] = df[cas_column].apply(verify_cas)
    df['Valid Corrected'] = df['Corrected CAS'].apply(verify_cas)
    df['Final CAS'] = df.apply(finalize_cas, axis=1, cas_column=cas_column)
    return df

def clean_cas_column(df, cas_column='cas'):
    """
    Cleans the CAS number column by removing invalid characters and validating the format.
    """
    cas_pattern = r'^\d{2,7}-\d{2}-\d$'
    
    def clean_cas_number(cas_number):
        if pd.isnull(cas_number):
            return cas_number
        cas_number = re.sub(r'[^\d-]', '', str(cas_number))
        return cas_number if re.match(cas_pattern, cas_number) else pd.NA

    df[cas_column] = df[cas_column].apply(clean_cas_number)
    return df

def transform_cf_additional_casrns(df):
    """
    Transform CF additional CASRNs data.
    """
    def extract_casrn(data):
        return ', '.join(d.get('additional_casrn', '') for d in (data or []) if 'additional_casrn' in d)

    if 'additional_casrns' in df.columns:
        df['additional_casrns'] = df['additional_casrns'].apply(extract_casrn)
    return df

def analyze_cas(file_path, cas_column='cas'):
    """
    Reads data from a file, analyzes, and corrects CAS numbers using existing utility functions.
    
    :param file_path: path to the input file
    :type file_path: str
    :param cas_column: column name containing CAS numbers
    :type cas_column: str
    :return: data with analysis and corrections of CAS numbers
    :rtype: pandas.DataFrame
    """
    dtype = {cas_column: str}
    data = pd.read_excel(file_path, dtype=dtype) if file_path.endswith(('.xlsx', '.xls')) else pd.read_csv(file_path, dtype=dtype)
    return process_cas_dataframe(data, cas_column=cas_column)

if __name__ == "__main__":
    data = {'cas': ['123-45-6', '000123-045-006', '123-45-6 00:00:00', '123-45-67']}
    df = pd.DataFrame(data)
    processed_df = process_cas_dataframe(df, cas_column='cas')
    print(processed_df)
