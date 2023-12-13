import boto3
import configparser
import psycopg2

#Load configs

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

IAM_ROLE_ARN           = config.get("IAM_ROLE","ARN")
VPC_SG                 = config.get("AWS","VPC_SG")



#Create redhsift cluster in US-West-2

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                        )


try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        VpcSecurityGroupIds=[VPC_SG],
        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[IAM_ROLE_ARN]  
    )
except Exception as e:
    print(e)