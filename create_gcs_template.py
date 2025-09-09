#!/usr/bin/env python3
"""
Create a template GCS key file for users to fill in their credentials.
"""

import json

template = {
    "type": "service_account",
    "project_id": "your-project-id",
    "private_key_id": "your-private-key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
    "client_email": "your-service-account@your-project-id.iam.gserviceaccount.com",
    "client_id": "your-client-id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project-id.iam.gserviceaccount.com"
}

with open("gcs-key.json.template", "w") as f:
    json.dump(template, f, indent=2)

print("Created gcs-key.json.template")
print("To use GCS storage:")
print("1. Copy this file to gcs-key.json")
print("2. Replace the placeholder values with your actual GCS service account credentials")
print("3. Ensure your service account has Storage Admin permissions on the bucket")
print("")
print("If you don't have GCS credentials, the system will automatically use local file storage.")