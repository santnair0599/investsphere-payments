"""
Secret *references* — placeholders only, never real credential values.

The source ingestors (REST token, SFTP user/password, Salesforce client id/secret)
need credentials, but this reference codebase must never embed them. ``get_secret``
returns the Databricks secret **reference string** (``{{secrets/scope/key}}``), not
a value — the same shape the platform uses in job configs.

In Databricks the value is resolved at runtime with
``dbutils.secrets.get(scope, key)`` against the Key Vault-backed scope provisioned
by Terraform (``infra/terraform/modules/secrets``); the keys here line up with the
placeholders that module creates (``rest-api-token``, ``sftp-user`` / ``sftp-password``,
``salesforce-client-id`` / ``salesforce-client-secret``, …).
"""
from __future__ import annotations

DEFAULT_SCOPE = "investsphere_payments"

# logical credential name -> Key Vault secret key (matches the Terraform secrets)
SECRET_KEYS = {
    "rest_api_token": "rest-api-token",
    "sftp_user": "sftp-user",
    "sftp_password": "sftp-password",
    "salesforce_client_id": "salesforce-client-id",
    "salesforce_client_secret": "salesforce-client-secret",
}


def secret_ref(key, scope=DEFAULT_SCOPE):
    """The Databricks secret reference string for a Key Vault-backed key."""
    return "{{secrets/%s/%s}}" % (scope, key)


def get_secret(logical_name, scope=DEFAULT_SCOPE):
    """Return a secret REFERENCE (never a value) for a logical credential name.

    Raises KeyError for an unknown credential, so a typo fails loudly instead of
    silently sending an empty token.
    """
    key = SECRET_KEYS[logical_name]
    return secret_ref(key, scope)


def is_reference(value):
    """True if ``value`` is a secret reference placeholder (not a real value)."""
    return isinstance(value, str) and value.startswith("{{secrets/")
