# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount: Montagem do armazenamento de objetos em nuvem no Databricks
# MAGIC As montagens do Databricks criam um link entre o *workspace* e o *storage*, o que permite que você interaja com o armazenamento de objetos em nuvem usando caminhos de arquivo familiares em relação ao sistema de arquivos Databricks. É criado um alias local no diretório **/mnt** que armazena as seguintes informações:
# MAGIC 
# MAGIC - Localização *storage*.
# MAGIC - Especificações de driver para se conectar à conta de armazenamento *(Access Key)* ou contêiner *(SAS Url)*.
# MAGIC - Credenciais de segurança *(Tokens)* necessárias para acessar os dados.
# MAGIC 
# MAGIC Para maiores informações: <a href="https://docs.databricks.com/dbfs/mounts.html" target="_blank" rel="noreferrer noopener">Mounting cloud object storage on Databricks documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount ADLS Gen2 ou Blob Storage com ABFS
# MAGIC Para utilizar o recurso é preciso utilizar o Azure Active Directory (Azure AD) application service principal para autenticação.
# MAGIC 
# MAGIC Para maiores informações: <a href="https://docs.databricks.com/security/aad-storage-service-principal.html" target="_blank" rel="noreferrer noopener">Access storage with Azure Active Directory</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acesso direto ao ADLS Gen2 ou Blob Storage
# MAGIC Outra maneira é acessar o recurso diretamente utilizando uma **Access Key** ou uma **SAS URL**.
# MAGIC 
# MAGIC #### ***Importante***
# MAGIC Vamos utilizar os recursos do **Azure Key Vault** e o **Databricks Secret Scope**. Assim as credencias podem ser usadas de forma segura nos Notebooks.
# MAGIC 
# MAGIC No Databricks a opção não está visível no workspace, mas pode ser acessada adicionado **secrets/createScope** na URL.
# MAGIC 
# MAGIC > https://{suaInstancia}.azuredatabricks.net/?o={suaInstancia}#secrets/createScope 

# COMMAND ----------

# Credenciais
storageAccountName = dbutils.secrets.get(scope="azure-key-vault-secrets", key="datalake-storage-account-name")
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

# COMMAND ----------

# MAGIC %md
# MAGIC https://stackoverflow.com/questions/73335391/how-to-mount-adls-gen2-account-in-databrikcs-using-access-keys
# MAGIC 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts
