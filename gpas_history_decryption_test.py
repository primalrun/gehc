from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from pyspark.sql.functions import col, regexp_replace
import sys
import base64
import re

# Create a SparkSession
spark = SparkSession.builder.appName("AESDecryption").getOrCreate()

#s3Path = r'https://s3.console.aws.amazon.com/s3/buckets/odp-fin-nprod-prl-sensitive?region=us-east-1&prefix=history_test/GPAS_file-c000.snappy.parquet' #"complete S3 path with .parquet extenstion"
s3Path = r'c:\temp\GPAS_file-c000.snappy.parquet' #"complete S3 path with .parquet extenstion"
df = spark.read.parquet(s3Path)

columns_to_decrypt = ['sso_id', 'employee_name']  # Replace with your encrypted column names

# Create a sample DataFrame
#df = spark.createDataFrame(data, ["encrypted_column1", "other_column"])

# Register the UDF
def clean_text(text):
    return re.sub(r'\x07', '', text) if text else None



# Function for AES decryption
def decrypt_data(encrypted_data):
    key = bytes.fromhex('c24e7989f6613d2cf6836f893e6d3468') ## Pass the key used for encryption here
    #encrypted_data = base64.b64decode(columns_to_decrypt)
    #if isinstance(encrypted_data, list):
    #    encrypted_data = ''.join(encrypted_data)
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    decryptor = cipher.decryptor()
    #decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
    decrypted_data=decryptor.update(base64.b64decode(encrypted_data)) + decryptor.finalize()
    return decrypted_data.decode('utf-8')  # Decode assuming it's a UTF-8 string

# Decrypt specific columns using PySpark UDF
decrypt_udf = udf(decrypt_data, StringType())

#register UDF
clean_text_udf = udf(clean_text, StringType())

# Apply decryption to DataFrame columns

decrypted_df = df.withColumn("sso_id", clean_text_udf("sso_id"))
decrypted_df = df.withColumn("sso_id", decrypt_udf("sso_id"))
decrypted_df = decrypted_df.withColumn("employee_name", decrypt_udf("employee_name"))

decrypted_df = decrypted_df.withColumn("sso_id", clean_text_udf("sso_id"))
decrypted_df = decrypted_df.withColumn("employee_name", clean_text_udf("employee_name"))
decrypted_df = decrypted_df.withColumn("sso_id", regexp_replace(col("sso_id"), "[\x07\x04\x03]", ""))
decrypted_df = decrypted_df.withColumn("employee_name", regexp_replace(col("employee_name"), "[\x00-\x1F\x07\x04\x03\x10\x0F]", ""))


# Display the decrypted DataFrame
decrypted_df.display()
