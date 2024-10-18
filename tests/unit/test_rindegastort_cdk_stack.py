import aws_cdk as core
import aws_cdk.assertions as assertions

from rindegastort_cdk.rindegastort_cdk_stack import RindegastortCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in rindegastort_cdk/rindegastort_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RindegastortCdkStack(app, "rindegastort-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
