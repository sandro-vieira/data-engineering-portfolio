# Databricks notebook source
# Credenciais
storageAccountName = "dlavanadeanalyticslab"
sasToken = dbutils.secrets.get(scope="azure-key-vault-secrets", key="datalake-container-files-sas-token")
container = "files"

# COMMAND ----------

# Configurando o acesso
spark.conf.set(f"fs.azure.account.auth.type.{storageAccountName}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageAccountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageAccountName}.dfs.core.windows.net", f"{sasToken}")

# COMMAND ----------

# Path
path = f"abfss://{container}@{storageAccountName}.dfs.core.windows.net"
bronze = f"{path}/raw"
silver = f"{path}/processed"
gold = f"{path}/presentation"

# COMMAND ----------

display(dbutils.fs.ls(path))
