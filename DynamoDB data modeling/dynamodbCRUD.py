import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
dynamodb = boto3.resource("dynamodb", region_name='us-west-2')
table = dynamodb.Table('projectmanagement')
orgId=1234
orgIdp=1235

prID= 234
prIDy= 934
prIDB= 834
prIDfixed= 734

Empid1=96543
Empid2=96544

Empid3=96545
Empid4=96546

orgname='ORG#ABC'
empname = 'EMP#Toni'
###################### Access pattern ############################
'''
#query1- find org

# try:
#     response = table.get_item(Key={'PK': "ORG#1234",
#              'SK': '#METADATA#1234'})
# except ClientError as e:
#     print(e.response['Error']['Message'])
# if response:
#         print("Get ORG succeeded:")
#         print(response)

#Output
#{'Item': {'orgid': Decimal('1234'), 'SK': '#METADATA#1234', 'PK': 'ORG#1234', 'name': 'Happy inc', 'tier': 'free-trial'},
# 'ResponseMetadata': {'RequestId': '7FLLVJ2M80C1FOV84VHS051QERVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Sat, 03 Oct 2020 09:48:30 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '132', 'connection': 'keep-alive', 'x-amzn-requestid': '7FLLVJ2M80C1FOV84VHS051QERVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '506934849'}, 'RetryAttempts': 0}}
'''

'''
#query2- find all project in an org
try:
    # type 1
    # response = table.query(
    #     KeyConditionExpression=Key('PK').eq('ORG#1234') & Key('SK').begins_with('PRO')
    # )

    #type 2
    response = table.query(
        KeyConditionExpression="#PK= :PK and begins_with(#SK,:SK)",
        ExpressionAttributeNames={"#PK": "PK","#SK": "SK"},
        ExpressionAttributeValues={":PK": "ORG#1234",":SK": "PRO#"}
    )

    print(response['Items'])
#{'Items': [{'SK': 'PRO#agile#234', 'project_id': Decimal('234'), 'PK': 'ORG#1234', 'name': 'project X'}, {'SK': 'PRO#agile#934', 'project_id': Decimal('934'), 'PK': 'ORG#1234', 'name': 'project Y'}],
# 'Count': 2, 'ScannedCount': 2, 'ResponseMetadata': {'RequestId': '6J2J5D3OPV82MTED1PEHF4UICBVV4KQNSO5AEMVJF66Q9ASUAAJG', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Server', 'date': 'Sat, 03 Oct 2020 10:57:02 GMT', 'content-type': 'application/x-amz-json-1.0', 'content-length': '240', 'connection': 'keep-alive', 'x-amzn-requestid': '6J2J5D3OPV82MTED1PEHF4UICBVV4KQNSO5AEMVJF66Q9ASUAAJG', 'x-amz-crc32': '2444780744'}, 'RetryAttempts': 0}}
'''
'''
#query3- find all employee in a project
try:
    response = table.query(
        KeyConditionExpression="#PK= :PK ",
        ExpressionAttributeNames={"#PK": "PK"},
        ExpressionAttributeValues={":PK": "ORG#1234#PRO234"}
    )
    print(response['Items'])
except ClientError as e:
    print(e.response['Error']['Message'])
'''
'''
#query4- find all projects in which Emp1 works using Inverted INdex
try:
    response = table.query(
        IndexName="SK-PK-index",
        KeyConditionExpression="#SK= :SK ",
        ExpressionAttributeNames={"#SK": "SK"},
        ExpressionAttributeValues={":SK": "ORG#1234#EMP96543"}
    )
    print(response['Items'])
except ClientError as e:
    print(e.response['Error']['Message'])
'''
#query5- find name of projects/EMP/ORG
# 1st way using GSI

# try:
#     response = table.query(
#         IndexName="PK-orgname-index",
#         KeyConditionExpression="#PK=:PK and #SK= :SK ",
#         ExpressionAttributeNames={"#PK":"PK","#SK": "orgname"},
#         ExpressionAttributeValues={":PK":"ORG#1235",":SK": "EMP#Toni"}
#     )
#     print(response['Items'])
# except ClientError as e:
#     print(e.response['Error']['Message'])

# 2nd Way

# try:
#     response = table.scan(
#         FilterExpression=Attr('orgname').eq("EMP#Toni")    )
#     print(response['Items'])
# except ClientError as e:
#     print(e.response['Error']['Message'])
###################### Preparing items(org, company) in table ############################

# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId,
#             'SK': '#METADATA#%s'%orgId,
#             'name':'Happy inc',
#            'tier':'free-trial'
#         }
#     )
# response = table.delete_item(
#        Key={
#             'PK': "ORG#1234",
#             'SK': '#METADATA1234'
#             }
#     )

# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgIdp,
#             'SK': '#METADATA#%s'%orgIdp,
#             'name':'ABC',
#            'tier':'professional'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId,
#             'SK': 'PRO#agile#%s'%prID,
#             'name':'project X',
#            'project_id':prID
#         }
#     )
# response = table.update_item(
#     Key={
#         'PK': "ORG#%s"%orgId,
#         'SK': '#METADATA#%s'%orgId
#     },
#     UpdateExpression="set #orgid=:orgid",
#     ExpressionAttributeNames={"#orgid":"orgid"},
#     ExpressionAttributeValues={
#         ':orgid': orgId
#     }
# )
# response = table.update_item(
#     Key={
#         'PK': "ORG#%s"%orgIdp,
#         'SK': '#METADATA#%s'%orgIdp
#     },
#     UpdateExpression="set #orgid=:orgid",
#     ExpressionAttributeNames={"#orgid":"orgid"},
#     ExpressionAttributeValues={
#         ':orgid': orgIdp
#     }
# )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId,
#             'SK': 'PRO#agile#%s'%prIDy,
#             'name':'project Y',
#            'project_id':prIDy
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgIdp,
#             'SK': 'PRO#fixedbid#%s'%prIDfixed,
#             'name':'fixedbid',
#            'project_id':prIDfixed
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgIdp,
#             'SK': 'PRO#agile#%s'%prIDB,
#             'name':'project B',
#            'project_id':prIDB
#         }
#     )
#print(response)

# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId,
#             'SK': '#EMP#%s'%Empid1,
#             'name':'Raman',
#            'email':'Raman@123'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId,
#             'SK': '#EMP#%s'%Empid2,
#             'name':'Alia',
#            'email':'Alia@123'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgIdp,
#             'SK': '#EMP#%s'%Empid3,
#             'name':'Jacob',
#            'email':'Jacob@123'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgIdp,
#             'SK': '#EMP#%s'%Empid4,
#             'name':'Toni',
#            'email':'Toni@123'
#         }
#     )

# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId + "#PRO%s" %prID,
#             'SK': "ORG#%s"%orgId+ "#EMP%s" %Empid1,
#             'name':'Raman',
#             'projectname':'project X',
#            'dateofjoining':'2019-01-01'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId+"#PRO%s"%prIDy,
#             'SK': "ORG#%s"%orgId+"#EMP%s"%Empid1,
#             'name':'Raman',
#             'projectname':'project Y',
#            'dateofjoining':'2019-01-01'
#         }
#     )
# response = table.put_item(
#        Item={
#             'PK': "ORG#%s"%orgId + "#PRO%s" %prID,
#             'SK': "ORG#%s"%orgId+ "#EMP%s" %Empid2,
#             'name':'Alia',
#             'projectname':'project X',
#            'dateofjoining':'2019-01-01'
#         }
#     )

# response = table.update_item(
#     Key={
#         'PK': "ORG#1235",
#         'SK': '#EMP#96546'
#     },
#     UpdateExpression="set #orgname=:orgname",
#     ExpressionAttributeNames={"#orgname":"orgname"},
#     ExpressionAttributeValues={
#         ':orgname': empname
#     }
# )
