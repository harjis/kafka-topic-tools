Setup secrets:

Confluent:

```bash
az keyvault secret show --vault-name kv-cfl-eu-qa --name eu-qa-api-key | jq '.value' -r
az keyvault secret show --vault-name kv-cfl-eu-qa --name eu-qa-api-secret | jq '.value' -r
```

Paste the secrets in ConfluentTopic.java#createAdminClient

Aiven:

```bash
cd ./kafka-topic-config-checker-java/src/main/java/org/example
az keyvault list --query "[?(tags.ServiceUser == 'dataops-gitlab-ci' && contains(resourceGroup, 'rg-eu-qa'))] | [].name" -o tsv
kvdataops-gitlab-cib423

az keyvault secret show --vault-name kvdataops-gitlab-cif0e8 --name apac-prod-ca-cert | jq '.value' -r > ca.pem
az keyvault secret show --vault-name kvdataops-gitlab-cif0e8 --name cert-region-one | jq '.value' -r > service.cert
az keyvault secret show --vault-name kvdataops-gitlab-cif0e8 --name key-region-one | jq '.value' -r > service.key
```