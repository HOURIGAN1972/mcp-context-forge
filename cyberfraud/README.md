# Cyberfraud Protected Secrets Adapter

The files in this folder are specific to the Cyberfraud project. The files are copied to the context forge image in the Containerfile.cyberfraud docker file.
The cyberfraud_protected_secrets_adapter.py is inserted into the mcpgateway/__init__.py during the dockerfile build. The file initializes at startup and reads the defined secrets from the protected secrets file, and inserts them into the runtime os.environ

