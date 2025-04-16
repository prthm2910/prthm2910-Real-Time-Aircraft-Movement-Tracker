import boto3

def lambda_handler(event, context):
    """
    AWS Lambda function to trigger an AWS Glue job named 'ad_etl'.

    This function:
    - Initializes the Glue client using boto3.
    - Starts the specified Glue job asynchronously.
    - Returns the JobRunId and a success message.

    Args:
        event (dict): Event data passed to the function during invocation.
        context (object): Runtime information provided by Lambda.

    Returns:
        dict: Contains the JobRunId and a success confirmation message.
    """
    try:
        # Initialize the AWS Glue client
        glue_client = boto3.client('glue')

        # Start the Glue job
        response = glue_client.start_job_run(
            JobName="ad_etl"
        )

        # Return JobRunId and success message
        return {
            "JobRunId": response["JobRunId"],
            "Message": "Glue job for ad_etl started successfully"
        }

    except Exception as e:
        # Catch and return any exceptions for better observability
        return {
            "Error": str(e),
            "Message": "Failed to start Glue job"
        }
