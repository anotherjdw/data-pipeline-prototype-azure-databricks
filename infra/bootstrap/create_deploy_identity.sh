#!/usr/bin/env bash
# Creates the GitHub Actions deployment identity in Azure: an Entra app registration, its
# service principal, its role assignments, and the GitHub OIDC federated credentials.
# Safe to rerun: each step checks whether its piece already exists.
# Run after `az login`, as a user who may create app registrations and role assignments
# (Owner or User Access Administrator on the subscription).
set -euo pipefail

APP_NAME="github-data-pipeline-prototype"
GITHUB_REPO="anotherjdw/data-pipeline-prototype-azure-databricks"
STATE_RESOURCE_GROUP="data-pipeline-prototype-prototype-tfstate"
STATE_STORAGE_ACCOUNT="prototypetfstate"

SUBSCRIPTION_ID=$(az account show --query id -o tsv)
TENANT_ID=$(az account show --query tenantId -o tsv)
SUBSCRIPTION_SCOPE="/subscriptions/${SUBSCRIPTION_ID}"

# 1. App registration and service principal
APP_ID=$(az ad app list --display-name "$APP_NAME" --query "[0].appId" -o tsv)
if [[ -z "$APP_ID" ]]; then
  APP_ID=$(az ad app create --display-name "$APP_NAME" --query appId -o tsv)
  echo "Created app registration $APP_NAME"
fi

SP_OBJECT_ID=$(az ad sp list --filter "appId eq '$APP_ID'" --query "[0].id" -o tsv)
if [[ -z "$SP_OBJECT_ID" ]]; then
  SP_OBJECT_ID=$(az ad sp create --id "$APP_ID" --query id -o tsv)
  echo "Created service principal"
fi

# 2. Role assignments. --assignee-object-id avoids failures while a new service principal
#    is still replicating through Entra ID.
assign_role() {
  local role="$1" scope="$2"
  local existing
  existing=$(az role assignment list --assignee "$SP_OBJECT_ID" --role "$role" --scope "$scope" \
    --query "[0].id" -o tsv)
  if [[ -z "$existing" ]]; then
    az role assignment create --assignee-object-id "$SP_OBJECT_ID" \
      --assignee-principal-type ServicePrincipal --role "$role" --scope "$scope" -o none
    echo "Assigned $role"
  fi
}

assign_role "Contributor" "$SUBSCRIPTION_SCOPE"
assign_role "Role Based Access Control Administrator" "$SUBSCRIPTION_SCOPE"

# 3. Access to the Terraform state (the storage account must exist: run infra/bootstrap first)
STATE_ACCOUNT_ID=$(az storage account show --name "$STATE_STORAGE_ACCOUNT" \
  --resource-group "$STATE_RESOURCE_GROUP" --query id -o tsv)
assign_role "Storage Blob Data Contributor" "$STATE_ACCOUNT_ID"

# 4. GitHub OIDC federated credentials: one per subject the workflow logs in with
add_federated_credential() {
  local name="$1" subject="$2"
  local existing
  existing=$(az ad app federated-credential list --id "$APP_ID" \
    --query "[?name=='$name'].name" -o tsv)
  if [[ -z "$existing" ]]; then
    az ad app federated-credential create --id "$APP_ID" -o none --parameters "{
      \"name\": \"$name\",
      \"issuer\": \"https://token.actions.githubusercontent.com\",
      \"subject\": \"$subject\",
      \"audiences\": [\"api://AzureADTokenExchange\"]
    }"
    echo "Added federated credential $name"
  fi
}

add_federated_credential "github-main" "repo:anotherjdw@82816504/data-pipeline-prototype-azure-databricks@1420735755:ref:refs/heads/main"   # plan job
add_federated_credential "github-env-prod" "repo:anotherjdw@82816504/data-pipeline-prototype-azure-databricks@1420735755:environment:prod"  # apply job

# 5. Values for the GitHub repository secrets
echo
echo "AZURE_CLIENT_ID=$APP_ID"
echo "AZURE_TENANT_ID=$TENANT_ID"
echo "AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID"
