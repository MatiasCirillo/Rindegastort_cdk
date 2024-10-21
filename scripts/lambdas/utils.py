import json
import boto3
import datetime
import json
import re
from decimal import Decimal

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def create_item_in_dynamodb(content, table_name):
    # Create a DynamoDB resource
    dynamodb = boto3.resource("dynamodb")

    # Obtain the table reference
    table = dynamodb.Table(table_name)

    # Specify the new item's attributes
    content = float_to_decimal(content)

    try:
        # Write the new record in the DynamoDB table
        table.put_item(Item=content)
        print(f"New item created in DynamoDB with id: {content['scanId']}")

    except Exception as e:
        print("Error to create new item in DynamoDB")
        raise e


def get_item_from_dynamo(process_id, table_name):
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        # Recupera un documento JSON por clave de partición
        response = table.query(KeyConditionExpression=Key("id").eq(process_id))
        return response["Items"]
    except Exception as e:
        print(f"Error: {e}")
        raise e


def update_to_dynamodb(id, table_name, update_dict):
    """
    Actualiza un item en una tabla DynamoDB con nuevos datos de un diccionario.

    Args:
    id (str): El ID de la clave primaria del item a actualizar.
    table_name (str): El nombre de la tabla DynamoDB.
    update_dict (dict): Un diccionario con los campos a actualizar y sus nuevos valores.

    Returns:
        Imprime el resultado de la operación de actualización y cualquier error.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    update_dict = float_to_decimal(update_dict)

    # Construir la expresión de actualización y los valores de atributos
    update_expression = "SET " + ", ".join(f"#{k} = :{k}" for k in update_dict.keys())
    expression_attribute_names = {f"#{k}": k for k in update_dict.keys()}
    expression_attribute_values = {f":{k}": v for k, v in update_dict.items()}

    try:
        response = table.update_item(
            Key={"scanId": id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
        print(f"Actualización exitosa para el id {id}.")
        print(
            response
        )  # Imprime la respuesta para ver el resultado de la actualización

    except Exception as e:
        print(f"Error al actualizar DynamoDB: {str(e)}")
        raise


def send_to_lambda(toLambda, message):
    """Send message to another lambda"""
    print(f"############# Sending event to Lambda --> {toLambda}  ############")
    lambda_client = boto3.client("lambda")
    try:
        response = lambda_client.invoke(
            FunctionName=toLambda,
            InvocationType="Event",  # 'Event' para invocación asíncrona, 'RequestResponse' para sincrónica
            Payload=message,
        )
    except ClientError as e:
        print(
            f"Error AWS: {e.response['Error']['Code']}, {e.response['Error']['Message']}"
        )
        raise e


def send_sns_message(message, topic_arn, subject):
    try:
        sns_client = boto3.client("sns")
        print("send_sns_message")
        current_timestamp = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        message = str(message)

        response = sns_client.publish(
            TargetArn=topic_arn,
            Message=json.dumps(
                {
                    "timestamp": current_timestamp,
                    "message": message,
                }
            ),
            Subject="Historical data box ingest message",
        )

        print("send_sns_message => SNS Message response: ", response)
    except Exception as e:
        print(
            f"Error AWS: {e.response['Error']['Code']}, {e.response['Error']['Message']}"
        )
        raise e


def read_prompt_from_s3(bucket_name, file_key):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response["Body"].read().decode("utf-8")
    return content


def extract_json(text):

    # Buscar el primer '{' y el último '}'
    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1:
        raise ValueError("No se encontró un JSON válido en el texto")

    # Extraer el posible JSON
    json_string = text[start : end + 1]

    # Limpiar cualquier texto adicional antes o después de las llaves
    json_string = re.sub(r"^[^{]*", "", json_string)
    json_string = re.sub(r"[^}]*$", "", json_string)

    try:
        # Intentar parsear el JSON
        parsed_json = json.loads(json_string)
        return json_string
    except json.JSONDecodeError:
        return text


def merge_json_results(results):
    merged_result = {}
    for result in results:
        try:
            json_result = json.loads(result)
            merged_result.update(json_result)
        except json.JSONDecodeError:
            print(f"Error decoding JSON: {result}")
    return merged_result


def assign_value(target, key, value, source):
    if isinstance(value, dict):
        target[key] = {}
        for sub_key, sub_value in value.items():
            assign_value(target[key], sub_key, sub_value, source)
    else:
        target[key] = source.get(value, "")


def float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [float_to_decimal(v) for v in obj]
    return obj
