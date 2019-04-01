# DynamoDumpNet
Simple backup and restore .net core application for Amazon DynamoDB.

It has only been tested with small amounts of data.

Supports [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) as well as accounts with MFA enabled.

# Usage
```
usage: dotnet DynamoDUmpNet.dll [-m {backup,restore}] [-p PROFILE] [-r REGION] [-f file-name] [-l {true,false}]

Simple DynamoDB backup/restore.

optional arguments:
  -h, --help            Shows this help info
  -m {backup,restore}, --mode {backup,restore}
                        Determines if an import or an export is performed
  -t table-name, --table table-name
                        Source/Description DynamoDB table name to backup from or restore to
  -f file-name, --file file-name   
                        Configures the json file used to restore/backup data.
                        If not supplied, DynamoDBData.json is used as default.
  -p profile, --profile profile
                        AWS credentials file profile to use
  -r REGION, --region REGION
                        AWS region to use, e.g. 'eu-west-2'. Not required for DynamoDB local

```


AWS example
-----------

AWS Table backup:
```
dotnet DynamoDumpNet.dll -m backup -t MyDynamoDbTable -f MyDynamoDbTableData.json -p DevProfile -r eu-west-2
```

AWS Table restore:
```
dotnet DynamoDumpNet.dll -m restore -t MyDynamoDbTable -f MyDynamoDbTableData.json -p DevProfile -r eu-west-2
```

Local example
-----------

Local Table backup:
```
dotnet DynamoDumpNet.dll -m backup -t MyDynamoDbTable -f MyDynamoDbTableData.json -l true
```

Local Table restore:
```
dotnet DynamoDumpNet.dll -m restore -t MyDynamoDbTable -f MyDynamoDbTableData.json -l true
```
