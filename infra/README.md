# Bootstrap Azure Resources

## Set up Azure Resource Group and Storage Container

### Step-by-Step summary
Note: please ensure that any `*tfstate`, `*.tfstate.*` and `.terraform` content is placed in the `.gitignore` file, so that `infra/bootstrap/terraform.tfstate` is not committed to the repository.

1. Please ensure terraform is installed. 
2. Ensure the Azure CLI is installed. Use Homebrew by running ```brew install azure-cli```
3. Run ```az login``` to access your Azure account in the CLI. If you have more than one subscription, please select the one you wish to use by running the command `az account set --subscription <id>` before step 5.
4. Create the `infra/bootstrap` directory and provision the resource group, storage account and container, and role assignment in `main.tf`. Create the subscription_id variable in `variables.tf`. 
5. Export `TF_VAR_subscription_id` by running ```export TF_VAR_subscription_id=$(az account show --query id -o tsv)```
6. From `infra/bootstrap/`, run `terraform init` and `terraform apply`. 
7. Create the `backend.tf` file in `infra/terraform`, which points the main Terraform stack at the storage container. 
8. Run the following command `./infra/bootstrap/create_deploy_identity.sh` This script creates the Entra app 
   registration, which creates the identity for GitHub Actions and also assigns the service principal its roles and 
   adds the GitHub login credentials. Your Azure Account must have the **Owner** or **User Access Administrator** 
   role on the subscription, because handing out roles requires permission to manage access. If you created the subscription yourself, you are its Owner already. To check, open the subscription in the Azure portal, go to Access control (IAM), and select View my access.
9. Please use the output of the command in step 8 and save them as GitHub secrets. This prints the `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_SUBSCRIPTION_ID`. Save the value of these outputs under the GitHub secrets using the output names provided.

### FAQ

#### What is an Azure Resource Group, what purpose does it serve?
A resource group is a labelled folder in Azure that holds a project's related cloud services together. It lets the team see, control access to, track costs of, and remove everything that belongs to one project as a single unit.

#### What is the Azure Storage Account? What is the Storage Container? 
A storage account is a secure space in Azure for keeping files, and a storage container is a folder inside it that holds one particular set of files. Here they hold the record of everything the deployment tooling has built, so each new deployment knows what already exists and changes only what's needed.

# Creating Modules

## Workspace
1. Define local variables: `project_environment` as ```"${var.project}-${var.environment}"``` and the common tags 
   (project, environment, managed_by and module).
2. Set up `azurerm_resource_group` (name, location, tags) and `azurerm_databricks_workspace` (name, location, 
   resource_group_name, tags. Define sku as premium, required by Databricks.) 
3. Define the `resource_group_name` and `location` outputs for the `storage` and `access` modules, respectively. 
   Define `workspace_url` to define the `databricks` provider in the root `main.tf` file that it needs for the the 
   `host`. Define the `workspace_id`, which the `databricks` provider uses to sign in through Azure.

## Storage
1. Create a storage account by provisioning `azurerm_storage_account`. The four buckets required in AWS S3 becomes 
   one account, and each layer becomes a container inside it. 
   * Define the name (max 24 characters, lowercase, no hyphens, digits nor underscore). 
   * Define `resource_group_name` and `location`, taking the outputs from the workspace module. Create these as variables in `storage/variables.tf`, use the variables as values in `storage/main.tf` resources. To define the value of the variables, pass these using the `module "storage"` in the root `main.tf`.
   * Set `account_tier` to `"Standard"` and `account_replication_type` to `"LRS"`.
   * Set `is_hns_enabled` to `true`. This sets the hierarchical namespace and is required for `abfss://` paths and 
     the Unity Catalog.
   * Set `min_tls_version = "TLS1_2"` and `allow_nested_items_to_be_public = false`, which is the equivalent to the 
     S3 public access block, and is configured the same way in the bootstrap file.
   * Set common tags.
2. Create a storage container by provisioning `azurerm_storage_container`, using the `for_each` loop over the 
   to define the `storage_account_id` for each medallion layer, in a similar manner to `for_each = var.bucket_names` in AWS S3.

