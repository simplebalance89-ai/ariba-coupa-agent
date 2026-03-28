# Render.com Sandbox Deployment

## Quick Deploy

### Option 1: Blue Button (Easiest)
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/simplebalance89-ai/ariba-coupa-agent)

### Option 2: Manual

1. Go to https://dashboard.render.com
2. Click **New +** → **Web Service**
3. Connect GitHub repo: `simplebalance89-ai/ariba-coupa-agent`
4. Branch: `master`
5. Settings:
   - **Name**: `ariba-coupa-agent-sandbox`
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn server:app --host 0.0.0.0 --port $PORT`
   - **Plan**: Starter ($7/month)

6. Add Environment Variables:
   ```
   AZURE_BLOB_CONNECTION_STRING=<your-sandbox-connection>
   AZURE_BLOB_CONTAINER_NAME=ariba-coupa-sandbox
   ENVIRONMENT=sandbox
   ```

7. Click **Create Web Service**

## URLs

| Environment | URL |
|-------------|-----|
| Sandbox | `https://ariba-coupa-agent-sandbox.onrender.com` |
| Portal | `/` or `/review` |
| Health | `/health` |
| API | `/api/v1/...` |

## Free Tier Limits
- Sleep after 15 min inactivity (cold start ~30s)
- 512 MB RAM
- 0.1 CPU
- For always-on: Upgrade to Starter ($7/month)

## vs Azure Production

| Feature | Render (Sandbox) | Azure (Production) |
|---------|-----------------|-------------------|
| Cost | Free / $7 mo | ~$13-30 mo |
| Sleep | Yes (free) | No |
| Best for | Testing, demos | Live PO processing |
| Data | Isolated sandbox | Production staging DB |
