
import json,boto3

def lambda_handler(event, context):
    # TODO implement
    print(event)
    bucketname='sagemakersmarcallroutingoutput'
    s3 = boto3.client('s3')
    if event:
        print(event)
        file_obj = event['Records'][0]
        filename = str(file_obj['s3']['object']['key'])
        print("Filename: ", filename)
        fileObj = s3.get_object(Bucket=bucketname, Key=filename)
        #rows = fileObj['Body'].read().split('\n')
        file_content = fileObj["Body"].read().decode('utf-8').split('\n')
        
        print(type(file_content))
        print(file_content)
        import json
        a= file_content[0]
        for each_str in file_content:
            each_str=each_str.replace("\'",'\"')
            print(type(each_str))
            a_dict = json.loads(each_str)
            print(type(a_dict))
            if '+' not in str(a_dict['Cust_Id']):
                a_dict['Cust_Id'] = '+' + str(a_dict['Cust_Id'])
            if 'Complaint_Queue' in a_dict['Queue_nm']:
                pass
                #a_dict['Queue_nm'] = 'arn:aws:connect:us-east-1:472795581601:instance/0312c394-8013-4981-8d43-dfc1e0a0c7bc/queue/4c7fcf0d-166e-44f4-8967-87300ddbdd86'
            elif 'Generic_Queue' in a_dict['Queue_nm']:
                pass
                #a_dict['Queue_nm'] = 'arn:aws:connect:us-east-1:472795581601:instance/0312c394-8013-4981-8d43-dfc1e0a0c7bc/queue/d2cf2d54-4b45-42e7-b689-517e19047d06'
            elif 'Payment_Queue' in a_dict['Queue_nm']:
                pass
                #a_dict['Queue_nm'] = 'arn:aws:connect:us-east-1:472795581601:instance/0312c394-8013-4981-8d43-dfc1e0a0c7bc/queue/56186267-18fd-4302-8c50-eac39c4c11af'
            
            dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
            table = dynamodb.Table('DEV_ML_smart_call_routing')
            response1 = table.put_item(
                Item = {
                    'Queue_nm': a_dict.get('Queue_nm'),
                    'cust_num' : str(a_dict.get('Cust_Id'))
                }
                )
            return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
