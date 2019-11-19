import json

try:
    from parquet_converter import Converter
    from response_handler import Response
except Exception as e:
    pass

def convert(event, context):
    try:
        return Converter().convert_to_parquet()
    except Exception as e:
        print("error in lambda handler : ", str(e))
        return Response().server_error(additional_message = "Bad Gateway")