package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/adobe/ims-go/ims"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"go.uber.org/zap"
)

const (
	// maxRetries is the maximum number of retry attempts for IMS token operations
	maxRetries = 10
	// tokenRefreshThreshold is the time before expiration when token refresh is triggered
	tokenRefreshThreshold = 15 * time.Minute
	// defaultMaxBackoff is the default maximum backoff duration between retries
	defaultMaxBackoff = 60 * time.Second
	// initialTokenTimeout is the timeout for fetching the initial token
	initialTokenTimeout = 60 * time.Second
)

// retryConfig holds the retry configuration based on urgency
type retryConfig struct {
	maxRetries int
	maxBackoff time.Duration
}

// calculateRetryConfig determines retry behavior based on time until token expiration.
// The closer to expiration, the more aggressive the retries become.
func calculateRetryConfig(timeUntilExpiry time.Duration) retryConfig {
	switch {
	case timeUntilExpiry > 10*time.Minute:
		// Plenty of time: be conservative (fewer retries, default backoff)
		return retryConfig{
			maxRetries: 2,
			maxBackoff: defaultMaxBackoff,
		}
	case timeUntilExpiry > 5*time.Minute:
		// Moderate urgency: standard retries, default backoff
		return retryConfig{
			maxRetries: 3,
			maxBackoff: defaultMaxBackoff,
		}
	case timeUntilExpiry > 2*time.Minute:
		// High urgency: more retries, faster backoff
		return retryConfig{
			maxRetries: 5,
			maxBackoff: 15 * time.Second,
		}
	default:
		// Critical: very aggressive (token about to expire or already expired)
		return retryConfig{
			maxRetries: 8,
			maxBackoff: 5 * time.Second,
		}
	}
}

type AdobeImsOAuthBearer struct {
	clientID      string
	clientSecret  string
	clientCode    string
	tokenEndpoint string
	client        *ims.Client
	logger        *zap.Logger

	// Token state protected by mutex
	mu           sync.RWMutex
	accessToken  string
	refreshToken string
	expiresAt    time.Time
}

// isRetryableError determines if an error is transient and should be retried.
// It checks for:
// 1. IMS-specific HTTP status codes (429, 500, 502, 503, 504, 408)
// 2. Network errors (timeouts, connection refused, connection reset, dial errors)
// 3. DNS errors (lookup failures, timeouts)
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for IMS-specific errors with retryable HTTP status codes
	var imsErr *ims.Error
	if errors.As(err, &imsErr) {
		// Retryable HTTP status codes
		switch imsErr.StatusCode {
		case 408: // Request Timeout
			return true
		case 429: // Too Many Requests (rate limiting)
			return true
		case 500: // Internal Server Error
			return true
		case 502: // Bad Gateway
			return true
		case 503: // Service Unavailable
			return true
		case 504: // Gateway Timeout
			return true
		default:
			// Non-retryable IMS errors (4xx client errors like 401, 403, etc.)
			return false
		}
	}

	// Network timeouts
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// DNS errors
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// NXDOMAIN errors should not be retried
		if dnsErr.IsNotFound {
			return false
		}
		return dnsErr.IsTemporary || dnsErr.IsTimeout
	}

	// Network operation errors (dial, connection refused, etc.)
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Dial operations are retryable (service may not be ready)
		if opErr.Op == "dial" {
			return true
		}
		// Recursively check the wrapped error
		return isRetryableError(opErr.Err)
	}

	// String-based error checks for common retryable network errors
	errStr := err.Error()
	if errStr != "" {
		// Connection refused - service may not be ready yet
		if strings.Contains(errStr, "connection refused") {
			return true
		}
		// Connection reset - connection was dropped
		if strings.Contains(errStr, "connection reset") {
			return true
		}
		// Closed network connection
		if strings.Contains(errStr, "use of closed network connection") {
			return true
		}
	}

	return false
}

// retryWithBackoff executes a function with exponential backoff retry logic
func retryWithBackoff(ctx context.Context, operation func() error) error {
	return retryWithBackoffAndConfig(ctx, operation, retryConfig{
		maxRetries: maxRetries,
		maxBackoff: defaultMaxBackoff,
	})
}

// retryWithBackoffAndConfig executes a function with exponential backoff retry logic
// using the provided retry configuration
func retryWithBackoffAndConfig(ctx context.Context, operation func() error, config retryConfig) error {
	var lastErr error

	for attempt := 0; attempt <= config.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s, 8s, 16s, etc.
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			if backoff > config.maxBackoff {
				backoff = config.maxBackoff
			}

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if error is not retryable
		if !isRetryableError(err) {
			return fmt.Errorf("non-retryable error: %w", err)
		}

		// Don't retry on last attempt
		if attempt == config.maxRetries {
			break
		}
	}

	return fmt.Errorf("max retries (%d) exceeded, last error: %w", config.maxRetries, lastErr)
}

func NewAdobeImsOAuthBearer(clientID, clientSecret, clientCode, tokenEndpoint string, logger *zap.Logger) (*AdobeImsOAuthBearer, error) {
	client, err := ims.NewClient(&ims.ClientConfig{
		URL: tokenEndpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create IMS client: %w", err)
	}

	bearer := &AdobeImsOAuthBearer{
		clientID:      clientID,
		clientSecret:  clientSecret,
		clientCode:    clientCode,
		tokenEndpoint: tokenEndpoint,
		client:        client,
		logger:        logger.Named("adobe_ims_oauth"),
	}

	// Fetch initial token with retry logic
	ctx, cancel := context.WithTimeout(context.Background(), initialTokenTimeout)
	defer cancel()

	err = retryWithBackoff(ctx, func() error {
		token, err := client.Token(&ims.TokenRequest{
			Code:         clientCode,
			ClientID:     clientID,
			ClientSecret: clientSecret,
		})
		if err != nil {
			return err
		}

		bearer.mu.Lock()
		bearer.accessToken = token.AccessToken
		bearer.refreshToken = token.RefreshToken
		bearer.expiresAt = time.Now().Add(token.ExpiresIn)
		bearer.mu.Unlock()

		refreshAt := bearer.expiresAt.Add(-tokenRefreshThreshold)
		bearer.logger.Info("initial refresh token retrieved",
			zap.Time("expires_at", bearer.expiresAt),
			zap.Duration("expires_in", token.ExpiresIn),
			zap.Time("refresh_at", refreshAt))

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("unable to get initial token from IMS after retries: %w", err)
	}

	return bearer, nil
}

// refreshTokenIfNeeded checks if the token needs refresh and refreshes it if necessary.
// It refreshes the token when it expires in less than 15 minutes.
// Uses adaptive retry logic: the closer to expiration, the more aggressive the retries.
func (a *AdobeImsOAuthBearer) refreshTokenIfNeeded(ctx context.Context) error {
	a.mu.RLock()
	timeUntilExpiry := time.Until(a.expiresAt)
	needsRefresh := timeUntilExpiry < tokenRefreshThreshold
	currentRefreshToken := a.refreshToken
	a.mu.RUnlock()

	if !needsRefresh {
		return nil
	}

	// Upgrade to write lock for refresh
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine might have refreshed)
	timeUntilExpiry = time.Until(a.expiresAt)
	if timeUntilExpiry >= tokenRefreshThreshold {
		return nil
	}

	// Calculate retry configuration based on urgency
	config := calculateRetryConfig(timeUntilExpiry)

	// Refresh the token with adaptive retry logic
	err := retryWithBackoffAndConfig(ctx, func() error {
		refreshResp, err := a.client.RefreshTokenWithContext(ctx, &ims.RefreshTokenRequest{
			RefreshToken: currentRefreshToken,
			ClientID:     a.clientID,
			ClientSecret: a.clientSecret,
		})
		if err != nil {
			return err
		}

		// Update token state
		a.accessToken = refreshResp.AccessToken
		refreshTokenRenewed := false
		if refreshResp.RefreshToken != "" {
			a.refreshToken = refreshResp.RefreshToken
			refreshTokenRenewed = true
		}
		a.expiresAt = time.Now().Add(refreshResp.ExpiresIn)

		// Log token refresh
		refreshAt := a.expiresAt.Add(-tokenRefreshThreshold)
		if refreshTokenRenewed {
			a.logger.Info("refresh token renewed",
				zap.Time("new_expires_at", a.expiresAt),
				zap.Duration("expires_in", refreshResp.ExpiresIn),
				zap.Time("refresh_at", refreshAt))
		} else {
			a.logger.Info("access token refreshed",
				zap.Time("new_expires_at", a.expiresAt),
				zap.Duration("expires_in", refreshResp.ExpiresIn),
				zap.Time("refresh_at", refreshAt))
		}

		return nil
	}, config)

	if err != nil {
		return fmt.Errorf("failed to refresh IMS token after retries: %w", err)
	}

	return nil
}

func (a *AdobeImsOAuthBearer) Opt() kgo.Opt {
	return kgo.SASL(oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		// Refresh token if needed
		if err := a.refreshTokenIfNeeded(ctx); err != nil {
			return oauth.Auth{}, err
		}

		// Return current access token
		a.mu.RLock()
		defer a.mu.RUnlock()
		return oauth.Auth{
			Token: a.accessToken,
		}, nil
	}))
}
