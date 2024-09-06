import json
import os
import glob

def transform_record(record, record_date):
    # Remove PII
    record.pop('C_FIRST_NAME', None)
    record.pop('C_LAST_NAME', None)
    
    # Extract email domain from email address
    email = record.pop('C_EMAIL_ADDRESS', None)
    if email:
        domain = email.split('@')[-1]  # Extract the domain part of the email address
        record['C_EMAIL_DOMAIN'] = domain
    
    # Add the record_date field
    record['record_date'] = record_date
    
    return record

def process_files(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process JSON Lines files (e.g., .txt, .json)
    file_pattern = os.path.join(input_dir, '*.txt')
    files = glob.glob(file_pattern)
    
    for filepath in files:
        filename = os.path.basename(filepath)
        record_date = filename.split('.')[0]  # Extract date from filename (e.g., '2021-01-10')
        output_filepath = os.path.join(output_dir, f"transformed_{filename}")
        
        with open(filepath, 'r') as infile, open(output_filepath, 'w') as outfile:
            for line in infile:
                try:
                    record = json.loads(line)
                    transformed_record = transform_record(record, record_date)
                    outfile.write(json.dumps(transformed_record) + '\n')
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {filepath}: {e}")
    
    print("Transformation completed for all files.")

if __name__ == "__main__":
    input_directory = './input_data/'
    output_directory = './output_data/'
    process_files(input_directory, output_directory)
