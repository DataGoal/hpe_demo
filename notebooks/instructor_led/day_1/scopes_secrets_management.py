# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Databricks Secrets – Put / Get / Manage
# MAGIC
# MAGIC `dbutils.secrets` is **read-only**. Creating scopes, writing secrets, and managing ACLs is done here
# MAGIC with the Databricks SDK (`WorkspaceClient`), which authenticates as the notebook user automatically.
# MAGIC
# MAGIC Secret values are entered through the widgets at the top — never hard-code them in this notebook.
# MAGIC
# MAGIC | Section | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Parameters (widgets) |
# MAGIC | 2 | Scopes: create / list / delete |
# MAGIC | 3 | Secrets: put / list / delete |
# MAGIC | 4 | Read secrets with `dbutils` |
# MAGIC | 5 | Read secrets with SQL |
# MAGIC | 6 | ACLs |
# MAGIC | 7 | CLI and REST API reference |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("scope", "financial_integrations", "Scope")
dbutils.widgets.text("key", "payment_gw_token", "Key")
dbutils.widgets.text("secret_value", "", "Secret value (for put)")
dbutils.widgets.text("principal", "", "Principal (user/group/SP for ACLs)")
dbutils.widgets.dropdown("permission", "READ", ["READ", "WRITE", "MANAGE"], "ACL permission")

SCOPE = dbutils.widgets.get("scope")
KEY = dbutils.widgets.get("key")
PRINCIPAL = dbutils.widgets.get("principal")
PERMISSION = dbutils.widgets.get("permission")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Scopes

# COMMAND ----------

# Create scope (skips if it already exists)
existing_scopes = {s.name for s in w.secrets.list_scopes()}
if SCOPE in existing_scopes:
    print(f"Scope '{SCOPE}' already exists")
else:
    w.secrets.create_scope(scope=SCOPE)
    print(f"Created scope '{SCOPE}'")

# COMMAND ----------

# List scopes
for s in w.secrets.list_scopes():
    print(s.name, s.backend_type)

# COMMAND ----------

# Delete scope – removes ALL secrets in it. Uncomment to run.
# w.secrets.delete_scope(scope=SCOPE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Secrets

# COMMAND ----------

# Put secret (creates or overwrites)
secret_value = dbutils.widgets.get("secret_value")
if not secret_value:
    raise ValueError("Fill in the 'secret_value' widget before running this cell")

w.secrets.put_secret(scope=SCOPE, key=KEY, string_value=secret_value)
print(f"Stored secret '{KEY}' in scope '{SCOPE}'")

# Clear the widget so the value doesn't linger in the notebook state
dbutils.widgets.remove("secret_value")
dbutils.widgets.text("secret_value", "", "Secret value (for put)")

# COMMAND ----------

# Put binary secret (example) – uncomment to run
# import base64
# with open("/Volumes/<catalog>/<schema>/<volume>/cert.pem", "rb") as f:
#     w.secrets.put_secret(scope=SCOPE, key="payment_gw_cert", bytes_value=base64.b64encode(f.read()).decode())

# COMMAND ----------

# List keys in scope (names only, never values)
for s in w.secrets.list_secrets(scope=SCOPE):
    print(s.key, s.last_updated_timestamp)

# COMMAND ----------

# Delete secret – uncomment to run
# w.secrets.delete_secret(scope=SCOPE, key=KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read secrets with `dbutils`
# MAGIC
# MAGIC Printing a secret in a notebook always shows `[REDACTED]`. Verify it via length or hash instead.

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(SCOPE)

# COMMAND ----------

import hashlib

token = dbutils.secrets.get(scope=SCOPE, key=KEY)
raw = dbutils.secrets.getBytes(scope=SCOPE, key=KEY)

print(f"The retrieved token is: {token}")  # -> [REDACTED]
print(f"Length: {len(token)}")
print(f"SHA-256 prefix: {hashlib.sha256(token.encode()).hexdigest()[:12]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read secrets with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT secret('financial_integrations', 'payment_gw_token') AS token  -- shows [REDACTED]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ACLs

# COMMAND ----------

# Grant permission on scope
if not PRINCIPAL:
    raise ValueError("Fill in the 'principal' widget before running this cell")

w.secrets.put_acl(scope=SCOPE, principal=PRINCIPAL, permission=AclPermission(PERMISSION))
print(f"Granted {PERMISSION} on '{SCOPE}' to '{PRINCIPAL}'")

# COMMAND ----------

# List ACLs
for acl in w.secrets.list_acls(scope=SCOPE):
    print(acl.principal, acl.permission)

# COMMAND ----------

# Get ACL for one principal
if PRINCIPAL:
    print(w.secrets.get_acl(scope=SCOPE, principal=PRINCIPAL))

# COMMAND ----------

# Delete ACL – uncomment to run
# w.secrets.delete_acl(scope=SCOPE, principal=PRINCIPAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reference: CLI (run from your local terminal)
# MAGIC
# MAGIC Add `-p <profile>` to target a specific workspace.
# MAGIC
# MAGIC ```bash
# MAGIC # Scopes
# MAGIC databricks secrets create-scope financial_integrations
# MAGIC databricks secrets list-scopes
# MAGIC databricks secrets delete-scope financial_integrations
# MAGIC
# MAGIC # Put (interactive prompt – keeps value out of shell history)
# MAGIC databricks secrets put-secret financial_integrations payment_gw_token
# MAGIC # Put inline / stdin / file
# MAGIC databricks secrets put-secret financial_integrations payment_gw_token --string-value "my-token-value"
# MAGIC echo -n "my-token-value" | databricks secrets put-secret financial_integrations payment_gw_token
# MAGIC databricks secrets put-secret financial_integrations payment_gw_token < token.txt
# MAGIC
# MAGIC # List / get / delete
# MAGIC databricks secrets list-secrets financial_integrations
# MAGIC databricks secrets get-secret financial_integrations payment_gw_token -o json | jq -r .value | base64 --decode
# MAGIC databricks secrets delete-secret financial_integrations payment_gw_token
# MAGIC
# MAGIC # ACLs
# MAGIC databricks secrets put-acl financial_integrations "data-engineers" READ
# MAGIC databricks secrets list-acls financial_integrations
# MAGIC databricks secrets get-acl financial_integrations "data-engineers"
# MAGIC databricks secrets delete-acl financial_integrations "data-engineers"
# MAGIC ```
# MAGIC
# MAGIC Legacy CLI (v0.18 and older):
# MAGIC ```bash
# MAGIC databricks secrets create-scope --scope financial_integrations
# MAGIC databricks secrets put --scope financial_integrations --key payment_gw_token --string-value "my-token-value"
# MAGIC databricks secrets list --scope financial_integrations
# MAGIC databricks secrets delete --scope financial_integrations --key payment_gw_token
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Reference: REST API
# MAGIC
# MAGIC ```bash
# MAGIC HOST=https://<workspace>.cloud.databricks.com
# MAGIC TOKEN=<pat>
# MAGIC
# MAGIC curl -X POST $HOST/api/2.0/secrets/scopes/create -H "Authorization: Bearer $TOKEN" \
# MAGIC   -d '{"scope":"financial_integrations"}'
# MAGIC
# MAGIC curl -X POST $HOST/api/2.0/secrets/put -H "Authorization: Bearer $TOKEN" \
# MAGIC   -d '{"scope":"financial_integrations","key":"payment_gw_token","string_value":"my-token-value"}'
# MAGIC
# MAGIC curl -G $HOST/api/2.0/secrets/get -H "Authorization: Bearer $TOKEN" \
# MAGIC   --data-urlencode scope=financial_integrations --data-urlencode key=payment_gw_token
# MAGIC
# MAGIC curl -G $HOST/api/2.0/secrets/list -H "Authorization: Bearer $TOKEN" \
# MAGIC   --data-urlencode scope=financial_integrations
# MAGIC ```