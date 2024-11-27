from aws_cdk import (
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_s3_deployment as s3_deployment,
    Duration,
    RemovalPolicy,
    Duration,
    Stack,
)
from constructs import Construct


class RindegastORTCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ########################### BUCKET ############################

        # Bucket donde dejaremos el prompt a utilizar
        rindegastort_data_bucket = s3.Bucket(
            self,
            "RindeGastORTDataBucket",
            bucket_name=f"rindegastort-data-bucket-v2",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
        )

        ########################### SNS ############################

        # Create an SNS topic to fail processing
        fail_topic = sns.Topic(self, "FailTopic", topic_name="fail_topic")

        fail_email_ch = "matiascirilloj@gmail.com"
        # fail_topic.add_subscription(subs.EmailSubscription(fail_email_ch))

        ########################### DYNAMO DB ############################

        # Create a DynamoDB table
        users_table = dynamodb.Table(
            self,
            "UsersTable",
            table_name=f"rindegastort_users",
            partition_key=dynamodb.Attribute(
                name="id_usuario", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        # Agregar el índice global secundario (GSI) para `email`
        users_table.add_global_secondary_index(
            index_name="email-index",
            partition_key=dynamodb.Attribute(
                name="email", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,  # Puedes cambiar a `INCLUDE` o `KEYS_ONLY` si solo necesitas ciertos atributos
        )

        file_metadata_table = dynamodb.Table(
            self,
            "FileMetadataTable",
            table_name=f"ocr_files_data",
            partition_key=dynamodb.Attribute(
                name="uuid", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        # Agregar el índice global secundario (GSI) para `id_usuario`
        file_metadata_table.add_global_secondary_index(
            index_name="id_usuario-index",
            partition_key=dynamodb.Attribute(
                name="id_usuario", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,  # Puedes cambiar a `INCLUDE` o `KEYS_ONLY` si solo necesitas ciertos atributos
        )

        ########################## Lambda #########################

        # -------------------------- Layers --------------------------#

        pyMUPDF_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PyMUPDFLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p38-PyMUPDF:14",
        )

        pillow_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PillowLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p38-pillow:1",
        )

        # -------------------------- Lambdas --------------------------#

        # Funcion lambda clasificadora
        generator_function = _lambda.Function(
            self,
            "GeneratorFunction",
            function_name="rinde_gastos_ocr_generator_function",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="ocr/generator_textract.lambda_handler",
            code=_lambda.Code.from_asset("scripts/lambdas"),
            layers=[pillow_layer, pyMUPDF_layer],
            timeout=Duration.seconds(300),
            memory_size=1024,
            environment={
                "BUCKET_NAME": rindegastort_data_bucket.bucket_name,
                "FILE_KEY": "prompt_engineering/prompt.txt",
                "DYNAMODB_TABLE_NAME": file_metadata_table.table_name,
                "FAIL_TOPIC_ARN": fail_topic.topic_arn,
            },
        )
        generator_file_url = generator_function.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE
        )
        rindegastort_data_bucket.grant_read(generator_function)
        file_metadata_table.grant_read_write_data(generator_function)
        fail_topic.grant_publish(generator_function)
        # agregar politica de acceso a modelos de amazon bedrock
        generator_function.add_to_role_policy(
            iam.PolicyStatement(actions=["bedrock:*"], resources=["*"])
        )
        # agregar politicas de acceso completo a textract
        generator_function.add_to_role_policy(
            iam.PolicyStatement(actions=["textract:*"], resources=["*"])
        )

        # ############## ApiGateway ##############

        # Crear API Gateway con proxy habilitado
        api = apigateway.LambdaRestApi(
            self,
            "MyApiGateway",
            handler=generator_function,
            proxy=True,  # Cambiar a True para habilitar el proxy
        )

        # Agregar un endpoint para manejar las invocaciones
        items = api.root.add_resource("extract")
        items.add_method("POST")  # Definir el método GET

        ########################### Deployamos prompt.txt dentro del bucket ###########################

        # Llevamos el prompt en .txt al bucket
        s3_deployment.BucketDeployment(
            self,
            "ScriptDeployment",
            destination_bucket=rindegastort_data_bucket,
            sources=[s3_deployment.Source.asset("./prompt_engineering")],
            destination_key_prefix="prompt_engineering/",
            prune=False,
        )
