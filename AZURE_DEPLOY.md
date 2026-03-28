# Azure App Service Deployment

## Prerequisites
- Azure CLI installed
- Azure subscription access
- GitHub repo connected to Azure

## Deploy Steps

### 1. Create Azure Resources
```bash
# Login
az login

# Create resource group
az group create --name enpro-ai --location westus2

# Create App Service Plan (Linux)
az appservice plan create \
  --name enpro-ai-plan \
  --resource-group enpro-ai \
  --sku B1 \
  --is-linux

# Create Web App
az webapp create \
  --name ariba-coupa-agent \
  --resource-group enpro-ai \
  --plan enpro-ai-plan \
  --deployment-container-image-name nginx
```

### 2. Configure Environment Variables
```bash
az webapp config appsettings set \
  --name ariba-coupa-agent \
  --resource-group enpro-ai \
  --settings \
  AZURE_BLOB_CONNECTION_STRING="your-connection-string" \
  AZURE_BLOB_CONTAINER_NAME="ariba-coupa" \
  STAGING_SQL_SERVER="your-sql-server" \
  STAGING_SQL_DATABASE="po_staging" \
  STAGING_SQL_USERNAME="your-username" \
  STAGING_SQL_PASSWORD="your-password"
```

### 3. Enable GitHub Actions
1. Go to Azure Portal → App Service → Deployment Center
2. Select GitHub as source
3. Authorize and select repo: `simplebalance89-ai/ariba-coupa-agent`
4. Branch: `master`
5. Save

### 4. Deploy
Push to master branch triggers auto-deploy:
```bash
git push origin master
```

## URLs
- App: `https://ariba-coupa-agent.azurewebsites.net`
- Review Portal: `https://ariba-coupa-agent.azurewebsites.net/review`
- Health: `https://ariba-coupa-agent.azurewebsites.net/health`
