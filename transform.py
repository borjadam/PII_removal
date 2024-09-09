import json
import os
import glob

def transform_record(record, record_date):
    """
    Transforms a record by removing PII, extracting the email domain, 
    and adding the record_date.

    Parameters:
    - record (dict): The input data record.
    - record_date (str): The date extracted from the filename.

    Returns:
    - dict: The transformed record with PII removed and email domain added.
    """
    # removes PII
    record.pop('C_FIRST_NAME', None)
    record.pop('C_LAST_NAME', None)
    
    # extracts email domain
    email = record.pop('C_EMAIL_ADDRESS', None)
    if email:
        domain = email.split('@')[-1]  # Extract the domain part of the email address
        record['C_EMAIL_DOMAIN'] = domain
    else:
        # several records don't have an email address, which would affect the tranformation. Instead of throwing an error, it gives you a warning
        print(f"Warning: missing email address for record with C_CUSTOMER_ID: {record.get('C_CUSTOMER_ID', 'Unknown')}")
    
    # adds record_date field
    record['record_date'] = record_date
    
    return record

def process_files(input_dir, output_dir, file_extension='txt', date_format_position=0):
    """
    Processes files in the input directory, transforms their records, 
    and saves the transformed data to the output directory.

    Parameters:
    - input_dir (str): Path to the input directory containing JSON Lines files
    - output_dir (str): Path to the output directory where transformed files will be saved
    - file_extension (str): File extension to process (default is '.txt')
    - date_format_position (int): Position of the date in the filename, split by '.'

    Returns:
    - None
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # allows multiple file formats, not just .txt
    file_pattern = os.path.join(input_dir, f'*.{file_extension}')
    files = glob.glob(file_pattern)
    
    for filepath in files:
        filename = os.path.basename(filepath)
        
        # allows flexibility in date extraction, making position configurable
        record_date = filename.split('.')[date_format_position]  
        output_filepath = os.path.join(output_dir, f"transformed_{filename}")
        
        with open(filepath, 'r') as infile, open(output_filepath, 'w') as outfile:
            for line in infile:
                try:
                    record = json.loads(line)

                    # basic check to ensure a record is a dict 
                    if isinstance(record, dict):
                        transformed_record = transform_record(record, record_date)
                        outfile.write(json.dumps(transformed_record) + '\n')
                    else:
                        print(f"Skipping incorrect record in {filepath}: {record}")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {filepath}: {e}")
    
    print("Transformation completed for all files")

if __name__ == "__main__":
    input_directory = './input_data/'
    output_directory = './output_data/'
    
    process_files(input_directory, output_directory)
