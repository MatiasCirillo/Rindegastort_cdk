import json
import os
import logging
import base64
import hashlib
import boto3
from io import BytesIO
from datetime import datetime

import fitz  # PyMuPDF
from PIL import Image

from utils import send_sns_message, read_prompt_from_s3, extract_json

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
CLAUDE_MODEL = os.environ.get(
    "CLAUDE_MODEL", "anthropic.claude-3-haiku-20240307-v1:0"
)  # "anthropic.claude-3-5-sonnet-20240620-v1:0") # anthropic.claude-3-haiku-20240307-v1:0
BUCKET_NAME = os.environ.get("BUCKET_NAME")
FILE_KEY = os.environ.get("FILE_KEY")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
FAIL_TOPIC_ARN = os.environ.get("FAIL_TOPIC_ARN")

# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")
bedrock_client = boto3.client("bedrock-runtime")


# Lambda handler
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        try:
            # Extract information from the event
            bucket = event["s3"]["bucket"]
            key = event["s3"]["key"]
            id_usuario = event.get("id_usuario", "anonimo")
        except:
            body = json.loads(event["body"])
            bucket = body["s3"]["bucket"]
            key = body["s3"]["key"]
            id_usuario = body.get("id_usuario", "anonimo")

        logger.info(
            f"Processing file from bucket: {bucket}, key: {key}, id_usuario: {id_usuario}"
        )

        # Generate UUID
        file_name = f"{bucket}/{key}"
        uuid = hashlib.sha256(file_name.encode()).hexdigest()

        # Download the file from S3
        print(f"bucket: {bucket}, key: {key}")
        file_content = download_file_from_s3(bucket, key)

        # Determine file type based on extension
        _, file_extension = os.path.splitext(key)
        file_extension = file_extension.lower()
        logger.info(f"File extension: {file_extension}")

        images = process_file(file_content, file_extension)

        # Prepare content for Claude AI
        images_content = prepare_content_for_claude(images)

        logger.info("Llamando a Claude")
        claude_response = extract_json(call_claude(images_content))

        json_claude_response = json.loads(claude_response)

        dynamo_item = {
            "uuid": uuid,
            "s3_uri": f"s3://{bucket}/{key}",
            "timestamp": datetime.now().isoformat(),
            "id_usuario": id_usuario,
        }
        dynamo_item.update(json_claude_response)

        save_to_dynamodb(DYNAMODB_TABLE_NAME, dynamo_item)

        return json_claude_response

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        send_sns_message(
            f"Error in lambda_handler: {str(e)}",
            FAIL_TOPIC_ARN,
            f"Error: lambda generator",
        )
        raise e


# Asynchronous function to download file from S3
def download_file_from_s3(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read()
        file_size = len(file_content)
        logger.info(f"Downloaded file size: {file_size} bytes")
        return file_content
    except Exception as e:
        logger.error(f"Error downloading file from S3: {str(e)}")
        raise


# Asynchronous function to process the file based on its extension
def process_file(file_content, file_extension):
    images = []

    if file_extension == ".pdf":
        logger.info("File is a PDF, converting pages to images")
        images = convert_pdf_to_images(file_content)
    elif file_extension in [".jpg", ".jpeg", ".png"]:
        logger.info("File is an image")
        images.append(file_content)
    else:
        raise ValueError(f"Unsupported file type: {file_extension}")

    return images


# Function to convert PDF to images
def convert_pdf_to_images(pdf_content, max_images=20, pages_per_image=2):
    try:
        pdf_file = BytesIO(pdf_content)
        pdf_document = fitz.open(stream=pdf_file, filetype="pdf")

        total_pages = pdf_document.page_count
        logger.info(f"Total pages in PDF: {total_pages}")

        pages_to_process = min(total_pages, max_images * pages_per_image)
        logger.info(f"Processing {pages_to_process} pages")

        images = []
        total_size = 0

        for start_page in range(0, pages_to_process, pages_per_image):
            total_height = 0
            max_width = 0
            temp_images = []

            end_page = min(start_page + pages_per_image, pages_to_process)
            for page_num in range(start_page, end_page):
                page = pdf_document[page_num]
                pix = page.get_pixmap()
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                temp_images.append(img)
                total_height += img.height
                max_width = max(max_width, img.width)

            combined_image = Image.new("RGB", (max_width, total_height))
            y_offset = 0
            for img in temp_images:
                combined_image.paste(img, (0, y_offset))
                y_offset += img.height

            img_byte_arr = BytesIO()
            combined_image.save(img_byte_arr, format="PNG")
            image_data = img_byte_arr.getvalue()
            image_size = len(image_data)
            total_size += image_size
            images.append(image_data)

            logger.info(f"Created image of size {image_size} bytes")

        logger.info(
            f"Created {len(images)} combined images with total size {total_size} bytes"
        )
        return images
    except Exception as e:
        logger.error(f"Error converting PDF to images: {str(e)}")
        raise e


# Asynchronous function to prepare content for Claude AI
def prepare_content_for_claude(images):
    prompt_json_data = read_prompt_from_s3(
        BUCKET_NAME, FILE_KEY.replace(".txt", ".json")
    )
    prompt = read_prompt_from_s3(BUCKET_NAME, FILE_KEY)

    content = []

    max_size_base64_bytes = 5 * 1024 * 1024  # 5 MB
    max_original_size_bytes = int(max_size_base64_bytes / 1.33)

    for i, image in enumerate(images):
        img = Image.open(BytesIO(image))
        img_format = img.format or "PNG"
        quality = 100
        optimization_attempts = 0

        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format=img_format)
        img_base64 = base64.b64encode(img_byte_arr.getvalue()).decode("utf-8")
        img_base64_size = len(img_base64)

        while img_base64_size > max_size_base64_bytes and optimization_attempts < 10:
            scale_factor = ((max_size_base64_bytes / img_base64_size) ** 0.5) * (
                max_size_base64_bytes / img_base64_size
            )
            new_width = int(img.width * scale_factor)
            new_height = int(img.height * scale_factor)

            img = img.resize((new_width, new_height), Image.LANCZOS)
            img_byte_arr = BytesIO()
            img.save(img_byte_arr, format=img_format, optimize=True, quality=quality)
            img_base64 = base64.b64encode(img_byte_arr.getvalue()).decode("utf-8")
            img_base64_size = len(img_base64)

            optimization_attempts += 1
            logger.info(
                f"Optimized image {i + 1}: attempt {optimization_attempts}, size {img_base64_size} bytes"
            )

        if optimization_attempts >= 10:
            logger.error(f"Failed to optimize image {i + 1} below size limit")
            raise Exception(f"Image {i + 1} exceeds size limit after optimization")

        logger.info(f"Appending image {i + 1} with size {img_base64_size} bytes")

        content.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": f"image/{img_format.lower()}",
                    "data": img_base64,
                },
            }
        )

    content.append(
        {
            "type": "text",
            "text": f"{prompt.replace('<example>', prompt_json_data)} \n Assistant: {'{'}",
        }
    )

    content_size = len(json.dumps(content).encode("utf-8"))
    logger.info(f"Total size of content: {content_size} bytes")

    return content


# Optionally, save the final payload to DynamoDB
def save_to_dynamodb(table_name, item_content):
    try:
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=item_content)
        logger.info(f"Saved to DynamoDB: {response}")
    except Exception as e:
        logger.error(f"Error saving to DynamoDB: {str(e)}")
        raise e


def call_claude(content):
    try:
        logger.info("Calling Claude with content:")
        response = bedrock_client.invoke_model(
            modelId=CLAUDE_MODEL,
            body=json.dumps(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": content,
                        }
                    ],
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 200000,
                    "temperature": 0.1,
                    "top_k": 2,
                    "top_p": 0.2,
                }
            ),
        )

        response_json = json.loads(response["body"].read())

        return response_json["content"][0]["text"]
    except Exception as e:
        logger.error(f"Error calling Claude: {str(e)}")
        raise e
