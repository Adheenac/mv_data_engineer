# Muliverse Data Engineering Task

Your objective is to demonstrate your proficiency in managing an ETL pipeline (Extract, Transform, Load) using Python.

You will design a strategy to export data using a sample Multiverse API, transform it, and store it in an S3-based data lake.

## Task Requirements:

- Design and implement a solution to extract data associated with Apprenticeships, Projects and Programmes.
- Implement necessary transformations to normalize and clean the data as required.
- Write the code to store the transformed data in an S3-based data lake (we understand that you won't be able to test this step without access to AWS)
- Ensure that your code is well documented, and that you have considered performance and scalability

## Submission:

- Don't spend more than 2-3 hours on the task, but do think about what else you would have done given more time so that we can discuss this in the interview
- Include a README file with clear instructions on how to set up and run your solution.
- Return your Python scripts as a ZIP file, ideally at least one day before your next interview is scheduled

## API Details

This is a FastAPI web application that implements an API for the Multiverse data engineer task.

The application includes endpoints for apprenticeships, projects, and programmes, and uses OAuth2 for authentication.

By following the process below, you will be able to run this API locally and test your script against this local instance of the API.

### Prerequisites

- Python 3.7+
- pip (Python package installer)

### Installation

1. Navigate to the this folder in a terminal:

   ```sh
   cd ~/Documents/mv_data_engineer/ # Path depends on where you have extracted the ZIP archive
   ```

2. Create a virtual environment and activate it:

   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required dependencies:

   ```sh
   pip install -r requirements.txt
   ```

4. Start the FastAPI application:

   ```sh
   uvicorn app:app --reload
   ```

5. Access the Swagger documentation at <http://localhost:8000/docs>. You can test API calls using the page.

### Authentication

The API uses a OAuth2 password flow, using the URL, username and password:
URL: http://127.0.0.1:8000/login
Username: multiverse
Password: mult1v3r53

A successful login will return a token which should be provided as a bearer token in the Authorization header in subsequent requests to authenticated endpoints

### Pagination

Some of the endpoints support pagination. In these cases, a `next_token` will be provided in the `pagination` block in the response.

To retrieve the next set of results, this should be provided in the next request as the value for the `pagination_token` query string parameter.

If there are no further pages, the `next_token` returned will be null.
