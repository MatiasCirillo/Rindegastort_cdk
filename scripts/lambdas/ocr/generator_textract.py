import json
import os
import io
import logging
import hashlib
import boto3
from io import BytesIO
from datetime import datetime
from botocore.exceptions import ClientError

# Elimina estos imports si no se utilizan en otras partes del código
# import fitz  # PyMuPDF
# from PIL import Image

from utils import send_sns_message, read_prompt_from_s3, extract_json

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables de entorno
TITAN_MODEL = os.environ.get("TITAN_MODEL", "amazon.titan-text-premier-v1:0")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
FILE_KEY = os.environ.get("FILE_KEY")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
FAIL_TOPIC_ARN = os.environ.get("FAIL_TOPIC_ARN")

# Inicializar clientes de AWS
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")
bedrock_client = boto3.client("bedrock-runtime")


# Handler de Lambda
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        try:
            # Extraer información del evento
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

        # Generar UUID
        file_name = f"{bucket}/{key}"
        uuid = hashlib.sha256(file_name.encode()).hexdigest()

        # Cliente de Textract
        textract_client = boto3.client("textract")

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            stream = io.BytesIO(response["Body"].read())
        except ClientError as e:
            logger.error(f"Error al obtener el documento de S3: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error inesperado: {e}")
            raise e

        # Llamar a Amazon Textract para analizar el documento
        try:
            textract_response = textract_client.analyze_document(
                Document={"Bytes": stream.read()}, FeatureTypes=["FORMS", "TABLES"]
            )
        except ClientError as e:
            logger.error(f"Error al analizar el documento con Textract: {e}")
            raise e

        # Mapear los IDs de los bloques
        blocks = textract_response["Blocks"]
        block_map = {}
        for block in blocks:
            block_map[block["Id"]] = block

        # Extraer los pares clave-valor
        kvs = get_kv_relationships(blocks, block_map)

        # Convertir los pares clave-valor a texto
        kv_text = ""
        for key_text, value_text in kvs.items():
            kv_text += f"{key_text}: {value_text}\n"

        # Extraer el texto de las tablas (opcional)
        table_text = extract_tables(blocks, block_map)

        # Preparar el texto extraído
        extracted_text = kv_text + "\n" + table_text

        # Leer el prompt y los datos de ejemplo desde S3
        prompt_json_data = read_prompt_from_s3(
            BUCKET_NAME, FILE_KEY.replace(".txt", ".json")
        )
        prompt = read_prompt_from_s3(
            BUCKET_NAME, FILE_KEY.replace(".txt", "_textract.txt")
        )

        print(f"##### Textract result: {extracted_text}")

        # Preparar el texto de entrada para el modelo Titan
        input_text = f"{prompt.replace('<textract_example>', extracted_text).replace('<example>', prompt_json_data)}\nAssistant: {{"

        # Llamar al modelo Titan
        logger.info("Llamando al modelo Titan")
        titan_response = extract_json(call_titan(input_text))
        # print(f"######## Respuesta text: {titan_response}")

        json_titan_response = json.loads(titan_response)
        print(f"######## Respuesta JSON: {json_titan_response}")

        # Preparar el elemento para guardar en DynamoDB
        dynamo_item = {
            "uuid": uuid,
            "s3_uri": f"s3://{bucket}/{key}",
            "timestamp": datetime.now().isoformat(),
            "id_usuario": id_usuario,
        }
        dynamo_item.update(json_titan_response)

        # Guardar el resultado en DynamoDB
        save_to_dynamodb(DYNAMODB_TABLE_NAME, dynamo_item)

        return json_titan_response

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        send_sns_message(
            f"Error in lambda_handler: {str(e)}",
            FAIL_TOPIC_ARN,
            f"Error: lambda generator",
        )
        raise e


# Función para guardar en DynamoDB
def save_to_dynamodb(table_name, item_content):
    try:
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=item_content)
        logger.info(f"Guardado en DynamoDB: {response}")
    except Exception as e:
        logger.error(f"Error al guardar en DynamoDB: {str(e)}")
        raise e


# Función para llamar al modelo Titan
def call_titan(input_text):
    try:
        logger.info("Llamando al modelo Titan con el texto de entrada")

        # Preparar el cuerpo de la solicitud para el modelo Titan
        request_body = {
            "inputText": input_text,
            "textGenerationConfig": {
                "maxTokenCount": 3072,
                "stopSequences": [],
                "temperature": 0.1,
                "topP": 0.2,
            },
        }

        response = bedrock_client.invoke_model(
            modelId=TITAN_MODEL,
            accept="application/json",
            contentType="application/json",
            body=json.dumps(request_body).encode("utf-8"),
        )

        response_body = response["body"].read()
        response_json = json.loads(response_body)

        generated_text = response_json.get("results", [{}])[0].get("outputText", "")
        # print(f"####### raw_result: {generated_text}")
        return generated_text

    except Exception as e:
        logger.error(f"Error al llamar al modelo Titan: {str(e)}")
        raise e


# ------------------------ TEXTRACT -------------------------------


def get_kv_relationships(blocks, block_map):
    kvs = {}
    for block in blocks:
        if block["BlockType"] == "KEY_VALUE_SET" and "KEY" in block["EntityTypes"]:
            key = get_text(block, block_map)
            value_block = find_value_block(block, block_map)
            if value_block:
                value = get_text(value_block, block_map)
                kvs[key] = value
    return kvs


def get_text(result, blocks_map):
    text = ""
    if "Relationships" in result:
        for rel in result["Relationships"]:
            if rel["Type"] == "CHILD":
                for child_id in rel["Ids"]:
                    word = blocks_map[child_id]
                    if word["BlockType"] == "WORD":
                        text += word["Text"] + " "
                    elif word["BlockType"] == "SELECTION_ELEMENT":
                        if word["SelectionStatus"] == "SELECTED":
                            text += "X "
    return text.strip()


def find_value_block(key_block, block_map):
    if "Relationships" in key_block:
        for rel in key_block["Relationships"]:
            if rel["Type"] == "VALUE":
                for value_id in rel["Ids"]:
                    value_block = block_map[value_id]
                    return value_block
    return None


def extract_tables(blocks, block_map):
    table_text = ""
    for block in blocks:
        if block["BlockType"] == "TABLE":
            table_text += "Tabla:\n"
            rows = get_rows_columns_map(block, block_map)
            for row_index, cols in rows.items():
                for col_index, cell in cols.items():
                    cell_text = get_text(cell, block_map)
                    table_text += f"{cell_text}\t"
                table_text += "\n"
            table_text += "\n"
    return table_text


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    if "Relationships" in table_result:
        for relationship in table_result["Relationships"]:
            if relationship["Type"] == "CHILD":
                for child_id in relationship["Ids"]:
                    cell = blocks_map[child_id]
                    if cell["BlockType"] == "CELL":
                        row_index = cell["RowIndex"]
                        col_index = cell["ColumnIndex"]
                        if row_index not in rows:
                            rows[row_index] = {}
                        rows[row_index][col_index] = cell
    return rows
