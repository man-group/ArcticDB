# Getting started with Azure Blob Storage

[Azure Blob Storage](https://azure.microsoft.com/en-gb/products/storage/blobs) has an extensive set of configuration and access settings.
There are detailed guides in the [Azure Blob Storage documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/).
Best practice can depend on your situation.
This guide is intended as a quick-start, to help you get going with ArcticDB.

You will need an azure account with permission to create storage-accounts.
- [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) or use the browser based [Cloud Shell](https://shell.azure.com/).
- If you installed Azure CLI then you will also need to [login](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

### 1. Select a region.  
A region close to your client will mean greater performance.
You can list your available regions with.
```sh
az account list-locations -o table
```

### 2. Create a resource-group

This is not required but best practice would be to create a new resource-group to try out arcticdb.  Resource groups are there to help you collect together and manage related resources in Azure.
Set your chosen `<REGION>` here.  If you use an existing resource-group then replace that in the examples below.
```sh
az group create --name arcticdb --location <REGION>
```

### 3. Create a blob storage account 
This is created within your resource-group.  Choose a `<STORAGE_NAME>`, it needs to be globally unique across all of Azure.
```sh
az storage account create -g arcticdb --allow-blob-public-access false --sku Standard_LRS -n <STORAGE_NAME>
```
`-g arcticdb` is the resource-group you created in the last step.

### 4. Create a container
Create a container within the storage account.  Depending on your account and CLI setup you may need to provide [authorization](https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli) for this step.
```sh
az storage container create  --name data --account-name <STORAGE_NAME>
```

### 5. Connect to the storage account

- Get the connection string.
```sh
az storage account show-connection-string -g arcticdb --query connectionString -n <STORAGE_NAME> | sed 's,",,g'
```
The connection string includes the `AccountKey` for authentication and so you should store it securely.

- [Install ArcticDB](https://github.com/man-group/ArcticDB#readme).
- Find your CA_CERT_PATH path. See the [ArcticAB API docs](https://docs.arcticdb.io/api/arcticdb/arcticdb.Arctic#arcticdb.Arctic) for more information.
- Replace `<CONNECTION_STRING>` and `<CA_CERT_PATH>` in the following example.
```python
from arcticdb import Arctic
connection_string = '<CONNECTION_STRING>'
ca_cert_path = '<CA_CERT_PATH>'
arctic = Arctic(f"azure://{connection_string};Container=data;CA_cert_path={ca_cert_path}")
```
