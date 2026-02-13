import json
import os
from datetime import datetime
import boto3
from app.logging_config import get_logger

logger = get_logger(__name__)

s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "deere-connector-data-demo")


def save_deere_data_to_s3(data: dict, data_type: str = "raw") -> dict:
    """
    Save Deere API response to S3 as JSON
    
    Args:
        data: JSON response from Deere API
        data_type: "raw" or other classification
    
    Returns:
        {"status": "success", "s3_key": "...", "bucket": "..."}
    
    Example S3 key: raw/year=2025/month=01/day=02/event_2025-01-02T04-30-15.json
    """
    try:
        now = datetime.utcnow()
        year, month, day = now.year, now.month, now.day
        timestamp = now.isoformat().replace(":", "-")
        
        # Add metadata
        data_with_meta = {
            **data,
            "_ingestion_timestamp": now.isoformat(),
            "_data_type": data_type
        }
        
        # S3 key with partition structure
        s3_key = f"{data_type}/year={year}/month={month:02d}/day={day:02d}/event_{timestamp}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data_with_meta, indent=2),
            ContentType="application/json",
        )
        
        logger.info(f"✅ Data saved to S3: s3://{BUCKET_NAME}/{s3_key}")
        
        return {
            "status": "success",
            "s3_key": s3_key,
            "bucket": BUCKET_NAME,
            "full_path": f"s3://{BUCKET_NAME}/{s3_key}"
        }
    
    except Exception as e:
        logger.error(f"❌ Failed to save to S3: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


def get_s3_file_content(s3_key: str) -> dict:
    """
    Retrieve a JSON file from S3
    """
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"✅ Retrieved from S3: {s3_key}")
        return {"status": "success", "data": content}
    
    except Exception as e:
        logger.error(f"❌ Failed to retrieve from S3: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}


def list_s3_files(prefix: str = "raw/", limit: int = 20) -> dict:
    """
    List recent files in S3
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=limit
        )
        
        files = [obj['Key'] for obj in response.get('Contents', [])]
        logger.info(f"✅ Listed {len(files)} files from S3")
        
        return {
            "status": "success",
            "count": len(files),
            "files": files
        }
    
    except Exception as e:
        logger.error(f"❌ Failed to list S3 files: {str(e)}", exc_info=True)
        return {"status": "error", "error": str(e)}
