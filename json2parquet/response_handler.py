class Response:
    def __init__(self): 
        pass
    def server_error(self, additional_message = None):
        body = {
            "message": "Internal Server Error"
        }
        if additional_message is not None:
            body["message"] += " - " + str(additional_message)
        return {
            "statusCode": 500,
            "body":body
        }
    def bad_request(self, additional_message):
        body = {
            "message": "Bad Request"
        }
        if additional_message is not None:
            body["message"] += " - " + str(additional_message)
        return {
            "statusCode": 400,
            "body":body
        }
    def success(self, additional_message):
        body = {
            "message": "Success"
        }
        if additional_message is not None:
            body["message"] += " - " + str(additional_message)
        return {
            "statusCode": 200,
            "body":body
        }