import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError


def get_access_token(login_url, username, password):
    """
    Authenticate and retrieve the access token.

    Args:
    login_url: URL for the login endpoint.
    username: Username for authentication.
    password: Password for authentication.

    Returns:
    str: Access token.
    """
    response = requests.post(login_url, data={'username': username, 'password': password})
    response.raise_for_status()
    return response.json().get('access_token')


def fetch_data(url, access_token, params=None):
    """
    Fetch data from the API with pagination.

    Args:
    url: URL for the API endpoint.
    access_token: Bearer token for authorization.
    params: Query parameters for the API call.

    Returns:
    list: List of records.
    """
    headers = {'Authorization': f'Bearer {access_token}'}
    all_data = []

    while url:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data['data'])
        params = {'pagination_token': data['pagination']['next_token']} if data['pagination']['next_token'] else None
        url = url if params else None

    return all_data


def transform_data(data):
    """
    Transform the raw data for loading into the data lake.
    Args:
    data: List of raw records.
    Returns:
    pd.DataFrame: Transformed data as a DataFrame.
    """
    df = pd.DataFrame(data)
    # Just did basic transformations: Renamed columns, fill NaNs.
    df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
    df.fillna('', inplace=True)
    return df


def upload_to_s3(dataframe, bucket_name, file_name, aws_access_key, aws_secret_key):
    """
    Upload the transformed data to an S3 bucket.

    Args:
    dataframe: Transformed data.
    bucket_name: Name of the S3 bucket.
    file_name: File name for the uploaded data.
    aws_access_key: AWS access key.
    aws_secret_key: AWS secret key.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    try:
        csv_data = dataframe.to_csv(index=False)
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data)
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")


def main():
    """
    Main function to run the ETL pipeline.
    """
    # Configuration
    LOGIN_URL = "http://127.0.0.1:8000/login"
    API_URLS = {
        "apprenticeships": "http://127.0.0.1:8000/apprenticeships",
        "projects": "http://127.0.0.1:8000/apprenticeships/{id}/projects",
        "programmes": "http://127.0.0.1:8000/programmes"
    }
    USERNAME = "multiverse"
    PASSWORD = "mult1v3r53"
    BUCKET_NAME = "your-s3-bucket-name"
    AWS_ACCESS_KEY = "your-aws-access-key"
    AWS_SECRET_KEY = "your-aws-secret-key"

    # Extract
    token = get_access_token(LOGIN_URL, USERNAME, PASSWORD)

    apprenticeships_data = fetch_data(API_URLS["apprenticeships"], token)
    transformed_apprenticeships = transform_data(apprenticeships_data)
    upload_to_s3(transformed_apprenticeships, BUCKET_NAME, "apprenticeships.csv", AWS_ACCESS_KEY, AWS_SECRET_KEY)

    for apprenticeship in apprenticeships_data:
        project_data = fetch_data(API_URLS["projects"].format(id=apprenticeship['id']), token)
        transformed_projects = transform_data(project_data)
        upload_to_s3(transformed_projects, BUCKET_NAME, f"projects_{apprenticeship['id']}.csv", AWS_ACCESS_KEY, AWS_SECRET_KEY)

    programmes_data = fetch_data(API_URLS["programmes"], token)
    transformed_programmes = transform_data(programmes_data)
    upload_to_s3(transformed_programmes, BUCKET_NAME, "programmes.csv", AWS_ACCESS_KEY, AWS_SECRET_KEY)

if __name__ == "__main__":
    main()
