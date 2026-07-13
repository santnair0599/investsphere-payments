// InvestSphere GenAI plane — Azure infrastructure (Bicep).
// Provisions the Azure AI Foundry + OpenAI + AI Search + Container App stack the
// Enterprise Decision Agent runs on. Deploy with azd / az deployment group create.
// Secrets are stored in Key Vault and surfaced to the Container App via references;
// the app authenticates to Azure + Databricks with its managed identity.

@description('Base name for all resources')
param name string = 'investsphere-ai'

@description('Location')
param location string = resourceGroup().location

@description('OpenAI chat model deployment name')
param chatDeployment string = 'gpt-4o'

@description('Embedding model deployment name')
param embedDeployment string = 'text-embedding-3-small'

@description('Container image (ACR) for the agent, e.g. myreg.azurecr.io/investsphere-agent:tag')
param agentImage string

@description('ACR login server for image pull. Empty = derive from agentImage. The agent identity needs AcrPull on this registry.')
param acrLoginServer string = ''

@secure()
@description('Databricks SQL Warehouse access token (read-only service principal)')
param databricksToken string
param databricksHost string
param databricksHttpPath string
param databricksCatalog string = 'investsphere_prod'

// ---- APIM gateway (rate-limit-by-key + token metering + multi-deployment fallback) ----
@description('Provision APIM as the Azure OpenAI gateway. Opt-in: Developer SKU provisions in ~30-45 min. When true, the agent routes its OpenAI traffic through APIM.')
param enableApim bool = false
@allowed([ 'Developer', 'Basic', 'Standard', 'Premium' ]) // NOT Consumption — rate-limit-by-key needs Developer+
param apimSku string = 'Developer'
param apimPublisherEmail string = 'platform@investsphere.example'
param apimPublisherName string = 'InvestSphere Platform'
@description('Optional secondary Azure OpenAI endpoint (e.g. another region) for multi-deployment fallback. Empty = single backend.')
param secondaryOpenAiEndpoint string = ''
@description('Per-key tokens-per-minute limit enforced at the gateway (token metering + throttle).')
param openAiTokensPerMinute int = 20000
@description('Per-key requests-per-minute limit enforced at the gateway.')
param openAiRequestsPerMinute int = 120

// ---- Azure AI service feature flags -------------------------------------------------
// The app reads these as env vars. They default ON because the backing accounts are now
// provisioned below; the code keeps its deterministic fallback either way, so flipping
// one off degrades a capability rather than breaking the agent.
@description('Call Azure AI Content Safety Prompt Shields in the input guard. The deterministic regex backstop always runs regardless.')
param enablePromptShields bool = true
@description('Call Azure AI Document Intelligence for settlement/invoice reconciliation.')
param enableDocIntel bool = true
@description('Derive review/case sentiment with Azure AI Language instead of the offline lexicon.')
param enableSentiment bool = true

var tags = { project: 'investsphere', plane: 'genai' }
var apimName = '${name}-apim'
var apimGatewayUrl = 'https://${apimName}.azure-api.net'
// Route the agent's OpenAI traffic through APIM when enabled. Built from the NAME string
// (not a reference to the conditional APIM resource) so the template stays valid when disabled.
var agentOpenAiEndpoint = enableApim ? apimGatewayUrl : openai.properties.endpoint
// Built-in roles
var openAiUserRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '5e0bd9bd-7b93-4f28-af87-19fc36ad61bd') // Cognitive Services OpenAI User
var kvSecretsUserRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6') // Key Vault Secrets User
// Storage account name: 3-24 chars, lowercase alphanumeric only (no hyphens), globally unique.
var hubStorageName = take('st${uniqueString(resourceGroup().id, name)}', 24)
// ACR login server for the Container App to pull the image (managed-identity pull).
var acrServer = empty(acrLoginServer) ? split(agentImage, '/')[0] : acrLoginServer

// ---- identity ------------------------------------------------------------
resource uami 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${name}-id'
  location: location
  tags: tags
}

// ---- observability -------------------------------------------------------
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${name}-logs'
  location: location
  properties: { retentionInDays: 30 }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${name}-ai'
  location: location
  kind: 'web'
  properties: { Application_Type: 'web', WorkspaceResourceId: logs.id }
}

// ---- Azure OpenAI (Foundry models) --------------------------------------
resource openai 'Microsoft.CognitiveServices/accounts@2024-10-01' = {
  name: '${name}-openai'
  location: location
  kind: 'OpenAI'
  sku: { name: 'S0' }
  properties: { customSubDomainName: '${name}-openai', publicNetworkAccess: 'Enabled' }
}

resource chat 'Microsoft.CognitiveServices/accounts/deployments@2024-10-01' = {
  parent: openai
  name: chatDeployment
  sku: { name: 'Standard', capacity: 20 }
  properties: { model: { format: 'OpenAI', name: 'gpt-4o', version: '2024-08-06' } }
}

resource embed 'Microsoft.CognitiveServices/accounts/deployments@2024-10-01' = {
  parent: openai
  name: embedDeployment
  dependsOn: [ chat ]
  properties: { model: { format: 'OpenAI', name: 'text-embedding-3-small', version: '1' } }
}

// ---- Azure AI Search (hybrid + semantic RAG) ----------------------------
resource search 'Microsoft.Search/searchServices@2024-06-01-preview' = {
  name: '${name}-search'
  location: location
  sku: { name: 'basic' }
  properties: { semanticSearch: 'standard', replicaCount: 1, partitionCount: 1 }
}

// ---- Azure AI Content Safety (Prompt Shields) ---------------------------
// The input guard calls shieldPrompt here when PROMPT_SHIELDS_ENABLED is set. Without
// this account the flag was inert: the guard silently stayed on the regex backstop, and
// the red-team suite could not verify its toxicity/bias case at all.
resource contentSafety 'Microsoft.CognitiveServices/accounts@2024-10-01' = {
  name: '${name}-safety'
  location: location
  kind: 'ContentSafety'
  sku: { name: 'S0' }
  properties: { customSubDomainName: '${name}-safety', publicNetworkAccess: 'Enabled' }
}

// ---- Azure AI Document Intelligence (settlement/invoice reconciliation) --
resource docIntel 'Microsoft.CognitiveServices/accounts@2024-10-01' = {
  name: '${name}-docintel'
  location: location
  kind: 'FormRecognizer'
  sku: { name: 'S0' }
  properties: { customSubDomainName: '${name}-docintel', publicNetworkAccess: 'Enabled' }
}

// ---- Azure AI Language (sentiment enrichment) ---------------------------
resource language 'Microsoft.CognitiveServices/accounts@2024-10-01' = {
  name: '${name}-language'
  location: location
  kind: 'TextAnalytics'
  sku: { name: 'S' }
  properties: { customSubDomainName: '${name}-language', publicNetworkAccess: 'Enabled' }
}

// ---- Storage account backing the Foundry hub (REQUIRED by the ML workspace) ---
// Regular blob storage (no hierarchical namespace) — distinct from the Databricks
// ADLS Gen2 lake (that's provisioned by Terraform on the data plane).
resource hubStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: hubStorageName
  location: location
  tags: tags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    allowSharedKeyAccess: true          // ML workspace default-datastore auth
  }
}

// ---- Azure AI Foundry (hub + project) -----------------------------------
// A Hub requires an associated storage account + key vault (App Insights optional).
resource aiHub 'Microsoft.MachineLearningServices/workspaces@2024-10-01' = {
  name: '${name}-hub'
  location: location
  kind: 'Hub'
  identity: { type: 'SystemAssigned' }
  properties: {
    friendlyName: 'InvestSphere AI Hub'
    storageAccount: hubStorage.id
    keyVault: kv.id
    applicationInsights: appInsights.id
  }
}

resource aiProject 'Microsoft.MachineLearningServices/workspaces@2024-10-01' = {
  name: '${name}-project'
  location: location
  kind: 'Project'
  identity: { type: 'SystemAssigned' }
  properties: { friendlyName: 'InvestSphere Decision Agent', hubResourceId: aiHub.id }
}

// ---- Key Vault (secrets referenced by the Container App) -----------------
resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${name}-kv'
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
  }
}

resource kvDbxToken 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'databricks-token'
  properties: { value: databricksToken }
}

// Azure OpenAI key for the agent SDK. When routing through APIM (keyless backend), APIM
// ignores it for auth and uses it only as the per-key throttle/metering counter-key.
resource kvOpenAiKey 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'openai-api-key'
  properties: { value: openai.listKeys().key1 }
}

resource kvContentSafetyKey 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'content-safety-key'
  properties: { value: contentSafety.listKeys().key1 }
}

resource kvDocIntelKey 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'doc-intel-key'
  properties: { value: docIntel.listKeys().key1 }
}

resource kvLanguageKey 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'language-key'
  properties: { value: language.listKeys().key1 }
}

// The agent's user-assigned identity must be able to READ the Key Vault secrets it references.
resource uamiKvRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(kv.id, uami.id, 'kv-secrets-user')
  scope: kv
  properties: {
    roleDefinitionId: kvSecretsUserRoleId
    principalId: uami.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// ==== APIM — Azure OpenAI gateway (opt-in via enableApim) =================
// Provides rate-limit-by-key + token metering (azure-openai-token-limit), token metrics
// to App Insights (azure-openai-emit-token-metric), and multi-deployment fallback
// (backend pool + retry). Keyless to the backend: APIM authenticates to Azure OpenAI with
// its own managed identity. The agent routes here when enableApim=true (agentOpenAiEndpoint).
resource apim 'Microsoft.ApiManagement/service@2023-09-01-preview' = if (enableApim) {
  name: apimName
  location: location
  tags: tags
  sku: { name: apimSku, capacity: 1 }
  identity: { type: 'SystemAssigned' }
  properties: {
    publisherEmail: apimPublisherEmail
    publisherName: apimPublisherName
  }
}

// APIM managed identity → Azure OpenAI (keyless backend auth)
resource apimOpenAiRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (enableApim) {
  name: guid(openai.id, apimName, 'openai-user')
  scope: openai
  properties: {
    roleDefinitionId: openAiUserRoleId
    principalId: apim.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// App Insights logger — required for azure-openai-emit-token-metric
resource apimLogger 'Microsoft.ApiManagement/service/loggers@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'appinsights'
  properties: {
    loggerType: 'applicationInsights'
    resourceId: appInsights.id
    credentials: { instrumentationKey: appInsights.properties.InstrumentationKey }
  }
}

// Numeric limits as named values, so the policy XML can stay verbatim ({{oaiTpm}}/{{oaiRpm}})
resource nvTpm 'Microsoft.ApiManagement/service/namedValues@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'oaiTpm'
  properties: { displayName: 'oaiTpm', value: string(openAiTokensPerMinute) }
}
resource nvRpm 'Microsoft.ApiManagement/service/namedValues@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'oaiRpm'
  properties: { displayName: 'oaiRpm', value: string(openAiRequestsPerMinute) }
}

// Backends: primary Azure OpenAI + optional secondary, behind a pool for failover
resource bePrimary 'Microsoft.ApiManagement/service/backends@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'openai-primary'
  properties: {
    url: '${openai.properties.endpoint}openai'
    protocol: 'http'
    circuitBreaker: {
      rules: [ {
        name: 'openai-breaker'
        failureCondition: {
          count: 3
          interval: 'PT1M'
          statusCodeRanges: [ { min: 429, max: 429 }, { min: 500, max: 599 } ]
        }
        tripDuration: 'PT1M'
        acceptRetryAfter: true
      } ]
    }
  }
}
resource beSecondary 'Microsoft.ApiManagement/service/backends@2023-09-01-preview' = if (enableApim && !empty(secondaryOpenAiEndpoint)) {
  parent: apim
  name: 'openai-secondary'
  properties: { url: '${secondaryOpenAiEndpoint}openai', protocol: 'http' }
}
resource bePool 'Microsoft.ApiManagement/service/backends@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'openai-pool'
  dependsOn: [ bePrimary, beSecondary ]
  properties: {
    type: 'Pool'
    pool: {
      services: concat(
        [ { id: '${apim.id}/backends/openai-primary', priority: 1, weight: 1 } ],
        !empty(secondaryOpenAiEndpoint) ? [ { id: '${apim.id}/backends/openai-secondary', priority: 2, weight: 1 } ] : []
      )
    }
  }
}

// API + wildcard operation for the Azure OpenAI inference surface
resource oaiApi 'Microsoft.ApiManagement/service/apis@2023-09-01-preview' = if (enableApim) {
  parent: apim
  name: 'azure-openai'
  properties: {
    displayName: 'Azure OpenAI (gateway)'
    path: 'openai'
    protocols: [ 'https' ]
    subscriptionRequired: false
    serviceUrl: '${openai.properties.endpoint}openai'
  }
}
resource oaiOp 'Microsoft.ApiManagement/service/apis/operations@2023-09-01-preview' = if (enableApim) {
  parent: oaiApi
  name: 'post-all'
  properties: {
    displayName: 'OpenAI inference (chat/embeddings)'
    method: 'POST'
    urlTemplate: '/{*path}'
    templateParameters: [ { name: 'path', type: 'string', required: false } ]
  }
}
resource oaiPolicy 'Microsoft.ApiManagement/service/apis/policies@2023-09-01-preview' = if (enableApim) {
  parent: oaiApi
  name: 'policy'
  dependsOn: [ bePool, apimLogger, nvTpm, nvRpm, oaiOp ]
  properties: {
    format: 'rawxml'
    value: '''<policies>
  <inbound>
    <base />
    <authentication-managed-identity resource="https://cognitiveservices.azure.com" />
    <azure-openai-token-limit counter-key='@(context.Request.Headers.GetValueOrDefault("api-key","anon"))' tokens-per-minute="{{oaiTpm}}" estimate-prompt-tokens="true" remaining-tokens-header-name="x-ratelimit-remaining-tokens" tokens-consumed-header-name="x-tokens-consumed" />
    <rate-limit-by-key calls="{{oaiRpm}}" renewal-period="60" counter-key='@(context.Request.Headers.GetValueOrDefault("api-key","anon"))' />
    <azure-openai-emit-token-metric namespace="investsphere-genai">
      <dimension name="caller" value='@(context.Request.Headers.GetValueOrDefault("api-key","anon"))' />
    </azure-openai-emit-token-metric>
    <set-backend-service backend-id="openai-pool" />
  </inbound>
  <backend>
    <retry condition='@(context.Response.StatusCode == 429 || context.Response.StatusCode >= 500)' count="2" interval="1" first-fast-retry="true">
      <forward-request buffer-request-body="true" />
    </retry>
  </backend>
  <outbound>
    <base />
  </outbound>
  <on-error>
    <base />
  </on-error>
</policies>'''
  }
}

// ---- Container Apps environment + agent app ------------------------------
resource caeEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${name}-cae'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}

resource agent 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${name}-agent'
  location: location
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${uami.id}': {} } }
  dependsOn: [ uamiKvRole ]
  properties: {
    managedEnvironmentId: caeEnv.id
    configuration: {
      ingress: { external: true, targetPort: 8000 }
      // Pull the image from the private ACR using the agent's managed identity.
      registries: [ { server: acrServer, identity: uami.id } ]
      secrets: [
        { name: 'databricks-token', keyVaultUrl: kvDbxToken.properties.secretUri, identity: uami.id }
        { name: 'openai-api-key', keyVaultUrl: kvOpenAiKey.properties.secretUri, identity: uami.id }
        { name: 'content-safety-key', keyVaultUrl: kvContentSafetyKey.properties.secretUri, identity: uami.id }
        { name: 'doc-intel-key', keyVaultUrl: kvDocIntelKey.properties.secretUri, identity: uami.id }
        { name: 'language-key', keyVaultUrl: kvLanguageKey.properties.secretUri, identity: uami.id }
      ]
    }
    template: {
      containers: [ {
        name: 'agent'
        image: agentImage
        resources: { cpu: json('1.0'), memory: '2Gi' }
        env: [
          // Routes through APIM when enableApim=true, else straight to Azure OpenAI.
          { name: 'AZURE_OPENAI_ENDPOINT', value: agentOpenAiEndpoint }
          { name: 'AZURE_OPENAI_API_KEY', secretRef: 'openai-api-key' }
          { name: 'AZURE_OPENAI_DEPLOYMENT', value: chatDeployment }
          { name: 'AZURE_OPENAI_EMBED_DEPLOYMENT', value: embedDeployment }
          { name: 'AZURE_SEARCH_ENDPOINT', value: 'https://${search.name}.search.windows.net' }
          { name: 'AZURE_SEARCH_INDEX', value: 'investsphere-policies' }
          { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
          { name: 'DATABRICKS_HOST', value: databricksHost }
          { name: 'DATABRICKS_HTTP_PATH', value: databricksHttpPath }
          { name: 'DATABRICKS_CATALOG', value: databricksCatalog }
          { name: 'DATABRICKS_TOKEN', secretRef: 'databricks-token' }
          // ---- Azure AI services + the flags that switch them on -------------
          // These flags were previously absent from the Container App, so the code's
          // Azure-native paths could never activate in a deployed environment and
          // silently ran on their offline fallbacks.
          { name: 'AZURE_CONTENT_SAFETY_ENDPOINT', value: contentSafety.properties.endpoint }
          { name: 'AZURE_CONTENT_SAFETY_KEY', secretRef: 'content-safety-key' }
          { name: 'PROMPT_SHIELDS_ENABLED', value: string(enablePromptShields) }
          { name: 'AZURE_DOC_INTEL_ENDPOINT', value: docIntel.properties.endpoint }
          { name: 'AZURE_DOC_INTEL_KEY', secretRef: 'doc-intel-key' }
          { name: 'DOC_INTEL_ENABLED', value: string(enableDocIntel) }
          { name: 'AZURE_LANGUAGE_ENDPOINT', value: language.properties.endpoint }
          { name: 'AZURE_LANGUAGE_KEY', secretRef: 'language-key' }
          { name: 'SENTIMENT_ENRICHMENT_ENABLED', value: string(enableSentiment) }
        ]
      } ]
      scale: { minReplicas: 1, maxReplicas: 5 }
    }
  }
}

output agentFqdn string = agent.properties.configuration.ingress.fqdn
output openaiEndpoint string = openai.properties.endpoint
output searchEndpoint string = 'https://${search.name}.search.windows.net'
output contentSafetyEndpoint string = contentSafety.properties.endpoint
output docIntelEndpoint string = docIntel.properties.endpoint
output languageEndpoint string = language.properties.endpoint
output apimEnabled bool = enableApim
output apimGatewayUrl string = enableApim ? apimGatewayUrl : ''
output agentOpenAiEndpoint string = agentOpenAiEndpoint
output hubStorageAccount string = hubStorage.name
// Grant this principal AcrPull on your ACR so the Container App can pull the image:
//   az role assignment create --assignee <this> --role AcrPull --scope <acr-resource-id>
output agentIdentityPrincipalId string = uami.properties.principalId
