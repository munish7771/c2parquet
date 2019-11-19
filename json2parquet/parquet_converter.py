try:
    import unzip_requirements
    print("Unzip requirements successfull, importing them")
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import boto3
    import os
    import io
    from datetime import datetime
    from response_handler import Response
except ImportError:
  print("Error while unzipping requirements")

def add_partition_table(bucket_path,partition_key):
    try:
        table = 'experience_analytics_dev_parquet_part'
        database = 's3_analytics'
        athena = boto3.client('athena')
        # checkQueryStart = athena.start_query_execution(
        #     QueryString = "show partitions " + table + " partition(dt='" + str(partition_key) + "')",
        #     QueryExecutionContext = {
        #     ## move to env variable
        #     'Database': database,
        #     },
        #     ResultConfiguration = {
        #         "OutputLocation":"s3://analytics-athena-lambda-results/"
        #     }
        # )
        # queryExecution = athena.get_query_execution(checkQueryStart["QueryExecutionId"])
        # print("Check query execution result is : ", queryExecution)
        queryStart = athena.start_query_execution(
        QueryString = "alter table " + table + " add partition (dt = '" + str(partition_key) + "') location 's3://" + str(bucket_path) + "/" + str(partition_key) + "/'" ,
        QueryExecutionContext = {
            ## move to env variable
            'Database': database
        },
        ResultConfiguration = {
            "OutputLocation":"s3://analytics-athena-lambda-results/"
        }
        )
        # queryExecution = athena.get_query_execution(queryStart.QueryExecutionId)
        print("Partition query executed successfully")
        return True
    except Exception as e:
        print("Error while adding partition in table. or partition already exists", str(e))
        return False

def list_bucket_objects(bucket_name, prefix):
    # Retrieve the list of bucket objects
    s_3 = boto3.client('s3')
    try:
        response = s_3.list_objects_v2(Bucket=bucket_name, Prefix=prefix + '/')
    except Exception as e:
        print(str(bucket_name) + " : bucket not found")
        print("Message : " + str(e))
        return None

    # Only return the contents if we found some keys
    if response['KeyCount'] > 0:
        return response['Contents']
    
    return None

def create_dataframe(s3_bucket_name, s3_key_list, today):
    try:
        data_frames = []
        if(s3_key_list == None):
            print("Empty folder in s3 or some error occured")
        elif(len(s3_key_list) == 0):
            print("Empty folder in s3.")
        else:
            for obj in s3_key_list:
                print("Json key name:" + obj['Key'])
                buffer = download_file(s3_bucket_name, obj['Key'])
                df = pd.read_json(buffer, lines = "True")
                ## convert eventTime to datetime.
                df["eventTime"] = pd.to_datetime(df["eventTime"])
                ## add new column as date
                frame_length = len(df.index)
                df["dt"] = frame_length * [today]
                data_frames.append(df)
                print(obj['Key'] + " added to the dataframes")
        print("total data frames are : ", len(data_frames))
        result = pd.concat(data_frames)
        return result
    except Exception as e:
        print("Some error occured")
        print("Message : " + str(e))
        return None

def download_file(bucket_name, file_name):
    try:
        s3 = boto3.client('s3')
        s3_response_object = s3.get_object(Bucket=bucket_name, Key=file_name)
        object_content = s3_response_object['Body'].read()
        return object_content
    except Exception as e:
       print("Error while downloading file ")
       return None 

def write_to_s3(bucket_name, key_name, file_stream):
    try:
        # s3 = boto3.resource('s3')
        # object = s3.Bucket(bucket_name).Object(key_name + '.parquet')
        # object.put(Body=file_stream)
        # s3 = boto3.client('s3')
        # with open(file_stream, 'rb') as data:
        #     s3.upload_fileobj(data, bucket_name, key_name)
        s3 = boto3.client('s3')
        s3.put_object(Body = file_stream, Bucket = bucket_name, Key = key_name)
        print("Object added to bucket " + bucket_name + " with key_name" + "-" + str(key_name))
        return True
    except Exception as e:
        print("Some error occured while uploading data")
        print("Message : " + str(e))
        return False
class Converter:
    def __init__(self):
        ## TO-DO move to lambda environment variables during deployment.
        ## TO-DO change with current date later.
        self.today = '2019-11-19'
        # self.s3_bucket_name = 'heatmap-analytics-plugin'
        # self.s3_bucket_name_output = 'heatmap-analytics-plugin-parquet'
        self.s3_bucket_name = os.environ['input_bucket']
        self.s3_bucket_name = os.environ['output_bucket']
        # self.aws_access_key = os.environ['access_key'] 
        # self.aws_secret_key = os.environ['secret_key']

    def convert_to_parquet(self, filename = None):
        fields = [
            pa.field('sessionId', pa.string()),
            pa.field('eventTime', pa.timestamp("s", tz="UTC")),
            # pa.field('eventTime', pa.string()),
            pa.field('eventLabel', pa.string()),
            pa.field('eventComponent', pa.string()),
            pa.field('fullURL', pa.string()),
            pa.field('browser', pa.string()),
            pa.field('resolution', pa.string()),
            pa.field('xCoord', pa.int64()),
            pa.field('yCoord', pa.int64()),
            pa.field('pageXCoord', pa.int64()),
            pa.field('pageYCoord', pa.int64()),
            pa.field('additionalInfo', pa.string()),
            pa.field('utm', pa.string()),
            pa.field('demographicInfo', pa.string()),
            pa.field('screenshot', pa.string()),
            pa.field('dt', pa.string())
        ]
        table_schema = pa.schema(fields)
        try:
            parquet_stream = io.BytesIO()
            if filename == None:
                # list all the objects in the s3 bucket/folder.
                key_list = list_bucket_objects(self.s3_bucket_name, self.today)
                if  key_list == None:
                    raise Exception("no files to convert in the folder")
                else:
                    print("Got key list from s3")
                    df = create_dataframe(self.s3_bucket_name, key_list, self.today)
                    if df is None:
                        return "Empty dataframe, some error occured."
                    table = pa.Table.from_pandas(df,schema=table_schema)
                    pq.write_table(table, parquet_stream, compression="SNAPPY")
                    # parquet_stream.seek(0) ## to reset the stream to initial position.
                    if not write_to_s3(self.s3_bucket_name_output, self.today + "/" + self.today+"_"+str(len(key_list)), parquet_stream.getvalue()):
                        return Response().server_error(additional_message = "Convertion successful but upload failed")
                    if add_partition_table(self.s3_bucket_name_output, self.today):
                        return Response().success(additional_message = "Conversion successfull and file<" + self.today+"_"+str(len(key_list)) +"> added to output bucket, added partition to table.")
                    else:
                        return Response().success(additional_message = "Conversion successfull and file<" + self.today+"_"+str(len(key_list)) +"> added to output bucket, could not add partition to table.")
            else:
                raise Exception("file name not provided")
        except Exception as e:
            print("Error inside convert_to_parquet - " + str(e))
            return Response().server_error(additional_message = "Unhandled Exception")
    def list_bucket_items(self):
        result = list_bucket_objects(self.s3_bucket_name, self.today)
        if result == None:
            print("Some error occured")
            return -99
        else:
            return len(result)

## fetch file from s3
    # df = pd.read_json("/Users/munishsharma/Downloads/json-files/Jsonfile.json", lines="True")
    # print("The data frame is",df)

## parse the file
    # table = pa.Table.from_pandas(df)
    # print("Table formed is",table)

## conversion and writing to s3 or to a buffer
    # pq.write_table(table, "/Users/munishsharma/Downloads/json-files/Parquetfile.parquet", compression="SNAPPY")
## upload to s3.