# GCS Setup Instructions for Render

## 1. Create a GCS Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project (or create one)
3. Go to **IAM & Admin** → **Service Accounts**
4. Click **Create Service Account**
5. Name it something like `crypto-data-collector`
6. Click **Create and Continue**

## 2. Add Permissions

Add these roles to your service account:
- **Storage Admin** (for full bucket access)
- Or **Storage Object Admin** (for object-level access only)

## 3. Create and Download Key

1. Click on your newly created service account
2. Go to the **Keys** tab
3. Click **Add Key** → **Create New Key**
4. Choose **JSON** format
5. Download the key file

## 4. Add to Render

1. In Render dashboard, go to your service
2. Navigate to **Environment** → **Secret Files**
3. Click **Add Secret File**
4. Set filename as: `gcs-key.json`
5. Paste the entire contents of your downloaded JSON key file
6. Save

## 5. Verify Bucket Access

Make sure your bucket `bananazone` exists and the service account has access to it.

## 6. Test

After deploying, check the logs to see:
- `Using GCS bucket: bananazone` (instead of local storage)
- No GCS-related errors in the logs

## Troubleshooting

If you see errors like:
- `Using local storage backend (GCS not available or configured)` - The key file isn't found
- `403 Forbidden` - Service account doesn't have proper permissions
- `404 Not Found` - Bucket doesn't exist or service account can't access it