#!/usr/bin/env python
"""Quick test to verify CRT is actually being used."""
import logging
import sys

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(message)s')

# Check if CRT is available
try:
    import awscrt
    print(f"awscrt version: {awscrt.__version__}")
    import awscrt.s3
    print(f"is_optimized_for_system: {awscrt.s3.is_optimized_for_system()}")
except ImportError:
    print("awscrt NOT installed - CRT unavailable")
    sys.exit(1)

# Check botocore CRT detection
from botocore.compat import HAS_CRT
print(f"botocore HAS_CRT: {HAS_CRT}")

# Test 1: Current conda approach (S3Transfer directly) - does NOT use CRT
print("\n--- Test 1: S3Transfer class (current conda approach) ---")
from boto3.s3.transfer import S3Transfer, TransferConfig
from boto3.session import Session

client = Session().client("s3")
config = TransferConfig(max_concurrency=10)
transfer = S3Transfer(client, config)
print(f"S3Transfer type: {type(transfer)}")
print("This does NOT use CRT - it's the classic Python transfer manager")

# Test 2: Using client.download_file with CRT config
print("\n--- Test 2: client.download_file with preferred_transfer_client='crt' ---")
from boto3.s3.transfer import TransferConfig as Boto3TransferConfig
config_crt = Boto3TransferConfig(preferred_transfer_client='crt')
print(f"TransferConfig preferred_transfer_client: {config_crt.preferred_transfer_client}")
print("This WILL use CRT when calling client.download_file(Config=config_crt)")

# Test 3: Check what create_transfer_manager returns
print("\n--- Test 3: What transfer manager gets created ---")
from boto3.s3.transfer import create_transfer_manager
config_auto = Boto3TransferConfig()  # default: auto
config_crt = Boto3TransferConfig(preferred_transfer_client='crt')

print(f"With auto (default): preferred_transfer_client={config_auto.preferred_transfer_client}")
print(f"With crt: preferred_transfer_client={config_crt.preferred_transfer_client}")

# The actual manager creation requires a real client, but we can see the logic
from boto3.s3.transfer import _should_use_crt
print(f"_should_use_crt(config_auto): {_should_use_crt(config_auto)}")
print(f"_should_use_crt(config_crt): {_should_use_crt(config_crt)}")
