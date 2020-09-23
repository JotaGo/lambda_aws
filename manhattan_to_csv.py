from datetime import datetime, timedelta
import json
import boto3
import pandas as pd
import io


def lambda_handler(event, context):
   s3 = boto3.client("s3")
   s3_resource = boto3.resource("s3")
   if event:
      s3_records = event["Records"][0]
      bucket_name = str(s3_records["s3"]["bucket"]["name"])
      file_name = str(s3_records["s3"]["object"]["key"])
      print(file_name)
      file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
      file_content = file_obj["Body"].read()

      read_excel_data = io.BytesIO(file_content)


      if file_name.startswith('data-entry/Perú Trujillo/HOR-R-OI-PR-001 Reporte de Producción Diario'):
         df = pd.read_excel(read_excel_data, '1. Lotes de Proceso', header=1)
         df.to_csv("/tmp/file.csv", encoding="utf-8", index=False)
         df['MP Venta Nacional'] = df['MP Venta Nacional'].fillna(0)
         df['MP Venta Nacional'] = df['MP Venta Nacional'].astype(int)
         if file_name.endswith('v0032.xlsx'):
            s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/prd-salaverry/peru_prod_salaverry.csv")
         else:
            s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/prd-chao/peru_prod_chao.csv")
      elif file_name.startswith('data-entry/Perú Trujillo/HOR-B-OI-PR-007 Antiguedad PT'):
         df = pd.read_excel(read_excel_data, 'Data', header=0)
         #df = df.drop(columns='.')
         df.to_csv("/tmp/file.csv", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/antiguedad-pt/peru_antiguedad_pt.csv")
      elif file_name.startswith('data-entry/Perú Trujillo/HOR-B-OI-PR-001 Costo Mano de Obra'):
         df = pd.read_excel(read_excel_data, 'BD')
         cols = [col for col in df if col.startswith('Unnamed:')]
         df = df.drop(columns=cols)
         if file_name.endswith('Semanal-T.xlsx'):
            df = df.rename(columns={' ':'PLANTA'})
            df.to_csv("/tmp/file.csv", index=False)
            s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/costo-mo/peru_cmo.csv")
         elif file_name.endswith('Diario -T.xlsx'):
            df.to_csv("/tmp/file.csv", index=False)
            s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/costo-mo-diario/peru_cmo_diario.csv")
      elif file_name.startswith('data-entry/Perú Trujillo/HOR-B-OI-PR-004'):
         df = pd.read_excel(read_excel_data, 'BD-Tableau', header=1)
         cols = [col for col in df if col.startswith('Unnamed:')]
         df = df.drop(columns=cols)
         df.to_csv("/tmp/file.csv", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/costo-materiales/peru_materiales.csv")
      elif file_name.startswith('data-entry/Perú Trujillo/HOR-B-OI-PR-005'):
         df = pd.read_excel(read_excel_data, sheet_name='1. BD',  index_col=1, header=0)
         df.to_csv("/tmp/file.csv", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/flujo-cae/rpt-flujo-cae.csv")
      elif file_name.startswith('data-entry/Perú Trujillo/HOR-R-OI-PR-005'):
         df = pd.read_excel(read_excel_data, sheet_name='1. BD Enfriamiento',  index_col=1, header=4)
         cols = [col for col in df if col.startswith('Unnamed:')]
         df = df.drop(columns=cols)
         df.to_csv("/tmp/file.csv", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "trujillo/control-enfriamiento/ctrl-enfriamiento.csv")
      elif file_name.startswith('data-entry/Estados Unidos/Llegadas USA/Proct Rec acumulado.xlsx'):
         df = pd.read_excel(read_excel_data, sheet_name='Sheet1', header=0)
         cols = [col for col in df if col.startswith('Unnamed:')]
         df = df.drop(columns=cols)
         #df['Season'] = df['Season'].fillna(0)
         #df['Season'] = df['Season'].astype(int)
         #df['Week'] = df['Week'].fillna(0)
         #df['Week'] = df['Week'].astype(int)
         #df['PO'] = df['PO'].fillna(0)
         #df['PO'] = df['PO'].astype(int)
         #df.drop(df.tail(1).index,inplace=True)
         df.to_csv("/tmp/file.csv", encoding="utf-8", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "estados-unidos/llegada/acumulado.csv")
      elif file_name.startswith('data-entry/Estados Unidos/Proceso USA/WZ_PROD_ORDR_DET_HANA.xlsx'):
         df = pd.read_excel(read_excel_data, sheet_name='WZ_PROD_ORDR_DET_HANA')
         cols = [col for col in df if col.startswith('Unnamed:')]
         df = df.drop(columns=cols)
         df.drop(df.tail(1).index,inplace=True)
         df['Production Order'] = df['Production Order'].astype(int)
         df.to_csv("/tmp/file.csv", encoding="utf-8", index=False)
         s3_resource.Bucket("manhattan-data").upload_file("/tmp/file.csv", "estados-unidos/proceso/prod_ordr_det_hana.csv")
   

   return {
      'statusCode': 200,
      'body': json.dumps('Hello from Lambda!')
   }

if __name__ == "__main__":
    event = {  
   "Records":[  
      {  
         "eventVersion":"2.2",
         "eventSource":"aws:s3",
         "awsRegion":"us-west-2",
         "eventTime":"The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, when Amazon S3 finished processing the request",
         "eventName":"event-type",
         "userIdentity":{  
            "principalId":"Amazon-customer-ID-of-the-user-who-caused-the-event"
         },
         "requestParameters":{  
            "sourceIPAddress":"ip-address-where-request-came-from"
         },
         "responseElements":{  
            "x-amz-request-id":"Amazon S3 generated request ID",
            "x-amz-id-2":"Amazon S3 host that processed the request"
         },
         "s3":{  
            "s3SchemaVersion":"1.0",
            "configurationId":"ID found in the bucket notification configuration",
            "bucket":{  
               "name":"manhattan-onedrive-sync",
               "ownerIdentity":{  
                  "principalId":"Amazon-customer-ID-of-the-bucket-owner"
               },
               "arn":"bucket-ARN"
            },
            "object":{  
               "key":"data-entry/Perú Trujillo/HOR-B-OI-PR-001 Costo Mano de Obra 2020-2021 Diario -T.xlsx",
               "size":"object-size",
               "eTag":"object eTag",
               "versionId":"object version if bucket is versioning-enabled, otherwise null",
               "sequencer": "a string representation of a hexadecimal value used to determine event sequence, only used with PUTs and DELETEs"
            }
         },
         "glacierEventData": {
            "restoreEventData": {
               "lifecycleRestorationExpiryTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, of Restore Expiry",
               "lifecycleRestoreStorageClass": "Source storage class for restore"
            }
         }
      }
   ]
}               
    print(event["Records"][0]["s3"]["bucket"]["name"])
    context = {

    }
    lambda_handler(event, context)
