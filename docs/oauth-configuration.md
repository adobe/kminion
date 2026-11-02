# OAuth Configuration Guide

kminion supports OAUTHBEARER SASL mechanism with two flavors:

1. **Generic OAuth** - Standard OAuth 2.0 client credentials flow
2. **Adobe IMS OAuth** - Adobe Identity Management System integration

## Generic OAuth Configuration

For standard OAuth 2.0 providers, configure the following:

```yaml
kafka:
  sasl:
    enabled: true
    mechanism: "OAUTHBEARER"
    oauth:
      # Leave type empty for generic OAuth
      type: ""
      tokenEndpoint: "https://your-oauth-provider.com/oauth/token"
      clientId: "your-client-id"
      clientSecret: "your-client-secret"
      scope: "kafka"
```

### Environment Variables

```bash
KAFKA_SASL_ENABLED=true
KAFKA_SASL_MECHANISM=OAUTHBEARER
KAFKA_SASL_OAUTH_TYPE=""
KAFKA_SASL_OAUTH_TOKENENDPOINT=https://your-oauth-provider.com/oauth/token
KAFKA_SASL_OAUTH_CLIENTID=your-client-id
KAFKA_SASL_OAUTH_CLIENTSECRET=your-client-secret
KAFKA_SASL_OAUTH_SCOPE=kafka
```

## Adobe IMS OAuth Configuration

For Adobe Identity Management System, configure the following:

```yaml
kafka:
  sasl:
    enabled: true
    mechanism: "OAUTHBEARER"
    oauth:
      type: "AdobeIMS"
      tokenEndpoint: "https://ims-na1.adobelogin.com"
      clientId: "your-ims-client-id"
      clientSecret: "your-ims-client-secret"
      additional:
        clientCode: "your-ims-code"
```

### Environment Variables

```bash
KAFKA_SASL_ENABLED=true
KAFKA_SASL_MECHANISM=OAUTHBEARER
KAFKA_SASL_OAUTH_TYPE=AdobeIMS
KAFKA_SASL_OAUTH_TOKENENDPOINT=https://ims-na1.adobelogin.com
KAFKA_SASL_OAUTH_CLIENTID=your-ims-client-id
KAFKA_SASL_OAUTH_CLIENTSECRET=your-ims-client-secret
KAFKA_SASL_OAUTH_ADDITIONAL_CLIENTCODE=your-ims-code
```

## How It Works

### Generic OAuth Flow

1. kminion uses the client credentials grant type
2. Sends a POST request to the token endpoint with client ID and secret
3. Receives an access token
4. Uses the token for Kafka authentication

### Adobe IMS Flow

1. kminion uses the Adobe IMS Go SDK (`github.com/adobe/ims-go`)
2. Creates an IMS client with the configured endpoint
3. Requests a token using the IMS-specific authentication flow
4. Stores both the access token and refresh token
5. Uses the access token for Kafka authentication
6. **Automatically refreshes the token** when it expires in less than 15 minutes
7. Updates the refresh token if a new one is provided during refresh

## Switching Between Providers

The `type` field determines which OAuth provider to use:

- **Empty or omitted**: Uses generic OAuth with `tokenEndpoint`, `clientId`, `clientSecret`, and `scope`
- **"AdobeIMS"**: Uses Adobe IMS with `tokenEndpoint`, `clientId`, `clientSecret`, and `additional.clientCode`

### Field Reuse

Both generic OAuth and Adobe IMS share common fields:
- `tokenEndpoint`: OAuth token endpoint URL (for Adobe IMS, this is the IMS endpoint)
- `clientId`: OAuth client ID
- `clientSecret`: OAuth client secret

Provider-specific fields go in the `additional` block:
- For Adobe IMS: `clientCode` (authorization code)

## Validation

- **Generic OAuth**: Validates that `tokenEndpoint`, `clientId`, and `clientSecret` are provided
- **Adobe IMS**: Validation happens during client creation when connecting to Kafka

## Token Refresh Behavior

### Generic OAuth

Generic OAuth fetches a new token on every authentication request using the client credentials flow. This is simple but may result in more frequent token requests to the OAuth provider.

### Adobe IMS

Adobe IMS implements intelligent token refresh with resilient error handling:

- **Initial Token**: Obtained during startup using the authorization code with retry logic
- **Refresh Token**: Stored securely and used for token renewal
- **Automatic Refresh**: Tokens are automatically refreshed when they expire in less than 15 minutes
- **Thread-Safe**: Token refresh is protected by mutex to prevent race conditions in concurrent environments
- **Long-Running Support**: Designed for long-running Kafka clients that need to maintain connections for extended periods
- **Retry Logic**: Automatic retry with exponential backoff for transient network errors

The 15-minute refresh threshold ensures that:
- Tokens are refreshed proactively before expiration
- There's sufficient time to complete the refresh operation
- Kafka connections remain authenticated without interruption

### Error Resilience

Adobe IMS token operations (both initial fetch and refresh) include automatic retry logic for transient errors:

**Retryable Errors:**
- Network timeouts (connection timeout, i/o timeout)
- DNS resolution failures (temporary errors, timeouts)
- Connection refused errors (service not ready)
- Connection reset errors (connection dropped)
- Network dial errors (can't establish connection)
- Closed network connection errors
- IMS HTTP errors (408, 429, 500, 502, 503, 504)

**Retry Strategy:**
- Maximum 3 retry attempts
- Exponential backoff: 1s, 2s, 4s, 8s, 16s (capped at 30s)
- Non-retryable errors (e.g., invalid credentials) fail immediately
- Context cancellation is respected during retry backoff

This ensures that temporary network issues or IMS service hiccups don't cause permanent authentication failures.

## Dependencies

- Generic OAuth: Built-in HTTP client
- Adobe IMS: `github.com/adobe/ims-go` v0.19.1+