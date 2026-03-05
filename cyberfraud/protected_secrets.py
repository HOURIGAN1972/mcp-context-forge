# Standard
import json
import logging
import os
from os import environ as os_environ
from pathlib import Path

# Third-Party
from jwcrypto import jwe, jwk

logger = logging.getLogger(__name__)

def _get_conf() -> str:
    conf_path = Path("/var/run/creds/conf.json")
    if not conf_path.exists():
        return ""
    with conf_path.open() as fp:
        conf_encrypted = fp.read()
    return conf_encrypted

def _get_key_text() -> str:
    # key.jwk is in a random UUID dir
    jwk_path = next(Path("/var/run/creds").glob("*/key.jwk"))
    with jwk_path.open() as fp:
        key_text = fp.read()
    return key_text

def decrypt_secrets() -> dict:
    try:
        conf_encrypted = _get_conf()
        if not conf_encrypted:
            return {}
        key_text = _get_key_text()
        key = jwk.JWK.from_json(key_text)
        jwe_obj = jwe.JWE.from_jose_token(conf_encrypted)
        jwe_obj.decrypt(key)
        conf = json.loads(jwe_obj.plaintext)
        return conf
    except Exception:
        logger.exception("Error extracting secrets")
        return {}

def get_config() -> dict:
  try:
    if "DEBUG" in os.environ and os.environ["DEBUG"] == True:
        with open("config.json") as cfg:
          d = json.load(cfg)
          cfg.close()
          return d
    else:
        return decrypt_secrets() 
  except Exception:
        logger.exception("Error extracting secrets")
        return {}
