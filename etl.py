import configparser
import pyspark.sql.functions as F
from datetime import datetime



#Read Configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

#Read AWS Access key
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']

#Read AWS Secret key
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description:
    Create SparkSession
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark




if __name__ == "__main__":
    main()
