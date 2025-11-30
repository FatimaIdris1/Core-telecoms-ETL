import pandas as pd
import boto3
import s3fs
# Initialize S3 filesystem
fs = s3fs.S3FileSystem()

# TRANSFORM CALL CENTER LOGS
#--------------------------------------
# S3 bucket paths
bucket = "core-telecoms-dl-fatima-idris"
input_prefix = "call logs/"
#output_prefix = "cleaned/call logs/"

# List all parquet files in the folder
files = fs.glob(f"{bucket}/{input_prefix}*.parquet")

print(f"Found {len(files)} parquet files to process")

# Column rename mapping
rename_map = {
    "call ID": "call_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "call_start_time": "call_start_time",
    "call_end_time": "call_end_time",
    "resolutionstatus": "resolution_status",
    "callLogsGenerationDate": "call_logs_generation_date"
}

# Process each parquet file
for file_path in files:
    print(f"Processing {file_path} ...")

    # Read file from S3
    with fs.open(file_path, "rb") as f:
        df = pd.read_parquet(f)

    # Rename columns
    df = df.rename(columns=rename_map)

    # Build output path
    file_name = file_path.split("/")[-1]  # keep original file name
#    output_path = f"{bucket}/{output_prefix}{file_name}"

    # Write cleaned parquet back to S3
    with fs.open(output_path, "wb") as f:
        df.to_parquet(f, index=False)

    print(f"Cleaned file written → s3://{output_path}")

print("All files cleaned successfully.")



# TRANSFORM WEB CUSTOMER COMPLAINTS
#--------------------------------------

# S3 bucket paths
bucket = "core-telecoms-dl-fatima-idris"
input_prefix = "customer_complaints/"
output_prefix = "cleaned/web_customer_complaints/"

# # Initialize S3 filesystem
# fs = s3fs.S3FileSystem()

# List all parquet files in the folder
files = fs.glob(f"{bucket}/{input_prefix}*.parquet")

print(f"Found {len(files)} parquet files to process")

# Column rename mapping
rename_map = {
    "Column1": "column_1",
    "request_id": "request_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "resolutionstatus": "resolution_status",
    "request_date": "request_date",
    "resolution_date": "resolution_date",
    "webFormGenerationDate": "web_form_generation_date"
}

# Process each parquet file
for file_path in files:
    print(f"Processing {file_path} ...")

    # Read file from S3
    with fs.open(file_path, "rb") as f:
        df = pd.read_parquet(f)

    # Apply column renaming
    df = df.rename(columns=rename_map)

    # Build output cleaned path
    file_name = file_path.split("/")[-1]
    output_path = f"{bucket}/{output_prefix}{file_name}"

    # Write cleaned file
    with fs.open(output_path, "wb") as f:
        df.to_parquet(f, index=False)

    print(f"Cleaned file saved → s3://{output_path}")

print("All customer complaint files cleaned successfully.")

# TRANSFORM SOCIAL MEDIA 
#--------------------------------------
# S3 bucket info
bucket = "core-telecoms-dl-fatima-idris"
input_prefix = "social_medias/"
output_prefix = "cleaned/social_medias/"

# # Init S3 filesystem
# fs = s3fs.S3FileSystem()

# Get all parquet files in the social_medias folder
files = fs.glob(f"{bucket}/{input_prefix}*.parquet")

print(f"Found {len(files)} parquet files to process")

# Column rename dictionary
rename_map = {
    "complaint_id": "complaint_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "resolutionstatus": "resolution_status",
    "request_date": "request_date",
    "resolution_date": "resolution_date",
    "media_channel": "media_channel",
    "MediaComplaintGenerationDate": "media_complaint_generation_date"
}

# Process each file
for file_path in files:
    print(f"Processing {file_path} ...")

    # Read input parquet
    with fs.open(file_path, "rb") as f:
        df = pd.read_parquet(f)

    # Apply renaming
    df = df.rename(columns=rename_map)

    # Extract file name
    file_name = file_path.split("/")[-1]

    # Build output path
    output_path = f"{bucket}/{output_prefix}{file_name}"

    # Save cleaned parquet back to S3
    with fs.open(output_path, "wb") as f:
        df.to_parquet(f, index=False)

    print(f"Saved cleaned file → s3://{output_path}")

print("All social media complaint files cleaned successfully.")