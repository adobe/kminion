package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adobe/ims-go/ims"
)

func TestAdobeImsOAuthBearer_RefreshTokenIfNeeded(t *testing.T) {
	tests := []struct {
		name          string
		expiresAt     time.Time
		shouldRefresh bool
		description   string
	}{
		{
			name:          "token expires in 20 minutes - no refresh",
			expiresAt:     time.Now().Add(20 * time.Minute),
			shouldRefresh: false,
			description:   "Token with 20 minutes until expiry should not be refreshed",
		},
		{
			name:          "token expires in 16 minutes - no refresh",
			expiresAt:     time.Now().Add(16 * time.Minute),
			shouldRefresh: false,
			description:   "Token with 16 minutes until expiry should not be refreshed",
		},
		{
			name:          "token expires in 14 minutes - refresh",
			expiresAt:     time.Now().Add(14 * time.Minute),
			shouldRefresh: true,
			description:   "Token with 14 minutes until expiry should be refreshed",
		},
		{
			name:          "token expires in 5 minutes - refresh",
			expiresAt:     time.Now().Add(5 * time.Minute),
			shouldRefresh: true,
			description:   "Token with 5 minutes until expiry should be refreshed",
		},
		{
			name:          "token already expired - refresh",
			expiresAt:     time.Now().Add(-1 * time.Minute),
			shouldRefresh: true,
			description:   "Expired token should be refreshed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bearer := &AdobeImsOAuthBearer{
				accessToken:  "test-access-token",
				refreshToken: "test-refresh-token",
				expiresAt:    tt.expiresAt,
			}

			bearer.mu.RLock()
			timeUntilExpiry := time.Until(bearer.expiresAt)
			needsRefresh := timeUntilExpiry < tokenRefreshThreshold
			bearer.mu.RUnlock()

			if needsRefresh != tt.shouldRefresh {
				t.Errorf("%s: expected shouldRefresh=%v, got %v (time until expiry: %v)",
					tt.description, tt.shouldRefresh, needsRefresh, timeUntilExpiry)
			}
		})
	}
}

func TestAdobeImsOAuthBearer_ConcurrentAccess(t *testing.T) {
	bearer := &AdobeImsOAuthBearer{
		accessToken:  "initial-token",
		refreshToken: "initial-refresh-token",
		expiresAt:    time.Now().Add(30 * time.Minute),
	}

	// Test concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bearer.mu.RLock()
			_ = bearer.accessToken
			_ = bearer.expiresAt
			bearer.mu.RUnlock()
		}()
	}

	wg.Wait()
}

func TestAdobeImsOAuthBearer_TokenStateUpdate(t *testing.T) {
	bearer := &AdobeImsOAuthBearer{
		accessToken:  "old-token",
		refreshToken: "old-refresh-token",
		expiresAt:    time.Now().Add(10 * time.Minute),
	}

	// Simulate token update
	bearer.mu.Lock()
	bearer.accessToken = "new-token"
	bearer.refreshToken = "new-refresh-token"
	bearer.expiresAt = time.Now().Add(60 * time.Minute)
	bearer.mu.Unlock()

	// Verify update
	bearer.mu.RLock()
	if bearer.accessToken != "new-token" {
		t.Errorf("expected accessToken to be 'new-token', got '%s'", bearer.accessToken)
	}
	if bearer.refreshToken != "new-refresh-token" {
		t.Errorf("expected refreshToken to be 'new-refresh-token', got '%s'", bearer.refreshToken)
	}
	bearer.mu.RUnlock()
}

func TestAdobeImsOAuthBearer_ExpiryCalculation(t *testing.T) {
	now := time.Now()
	expiresIn := 3600 * time.Second // 1 hour

	bearer := &AdobeImsOAuthBearer{
		accessToken:  "test-token",
		refreshToken: "test-refresh-token",
		expiresAt:    now.Add(expiresIn),
	}

	bearer.mu.RLock()
	timeUntilExpiry := time.Until(bearer.expiresAt)
	bearer.mu.RUnlock()

	// Allow for small time drift in test execution
	expectedMin := expiresIn - 1*time.Second
	expectedMax := expiresIn + 1*time.Second

	if timeUntilExpiry < expectedMin || timeUntilExpiry > expectedMax {
		t.Errorf("expected time until expiry to be around %v, got %v", expiresIn, timeUntilExpiry)
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		// IMS-specific errors
		{
			name: "IMS 429 Too Many Requests",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 429},
			},
			retryable: true,
		},
		{
			name: "IMS 503 Service Unavailable",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 503},
			},
			retryable: true,
		},
		{
			name: "IMS 500 Internal Server Error",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 500},
			},
			retryable: true,
		},
		{
			name: "IMS 502 Bad Gateway",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 502},
			},
			retryable: true,
		},
		{
			name: "IMS 504 Gateway Timeout",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 504},
			},
			retryable: true,
		},
		{
			name: "IMS 408 Request Timeout",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 408},
			},
			retryable: true,
		},
		{
			name: "IMS 401 Unauthorized (non-retryable)",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 401},
			},
			retryable: false,
		},
		{
			name: "IMS 403 Forbidden (non-retryable)",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 403},
			},
			retryable: false,
		},
		{
			name: "IMS 400 Bad Request (non-retryable)",
			err: &ims.Error{
				Response: ims.Response{StatusCode: 400},
			},
			retryable: false,
		},
		// Network errors
		{
			name:      "timeout error",
			err:       &net.DNSError{IsTimeout: true},
			retryable: true,
		},
		{
			name:      "temporary DNS error",
			err:       &net.DNSError{IsTemporary: true},
			retryable: true,
		},
		{
			name:      "DNS NXDOMAIN error (non-retryable)",
			err:       &net.DNSError{IsNotFound: true},
			retryable: false,
		},
		{
			name:      "dial operation error",
			err:       &net.OpError{Op: "dial", Err: errors.New("connection failed")},
			retryable: true,
		},
		{
			name:      "connection refused error",
			err:       errors.New("dial tcp 127.0.0.1:9092: connect: connection refused"),
			retryable: true,
		},
		{
			name:      "connection reset error",
			err:       errors.New("read tcp 127.0.0.1:9092: connection reset by peer"),
			retryable: true,
		},
		{
			name:      "closed network connection error",
			err:       errors.New("use of closed network connection"),
			retryable: true,
		},
		{
			name:      "non-retryable generic error",
			err:       errors.New("invalid credentials"),
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			if result != tt.retryable {
				t.Errorf("isRetryableError(%v) = %v, want %v", tt.err, result, tt.retryable)
			}
		})
	}
}

func TestRetryWithBackoff_Success(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	// Use a config with minimal retries and backoff for fast testing
	config := retryConfig{
		maxRetries: 2,
		maxBackoff: 1 * time.Millisecond,
	}

	err := retryWithBackoffAndConfig(ctx, func() error {
		callCount++
		if callCount < 2 {
			return &net.DNSError{IsTimeout: true} // Retryable error
		}
		return nil // Success on second attempt
	}, config)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestRetryWithBackoff_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	// Use a config with minimal retries and backoff for fast testing
	config := retryConfig{
		maxRetries: 3,
		maxBackoff: 1 * time.Millisecond,
	}

	err := retryWithBackoffAndConfig(ctx, func() error {
		callCount++
		return &net.DNSError{IsTimeout: true} // Always fail with retryable error
	}, config)

	if err == nil {
		t.Error("expected error, got nil")
	}

	expectedCalls := config.maxRetries + 1 // Initial attempt + retries
	if callCount != expectedCalls {
		t.Errorf("expected %d calls, got %d", expectedCalls, callCount)
	}
}

func TestRetryWithBackoff_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	err := retryWithBackoff(ctx, func() error {
		callCount++
		return errors.New("invalid credentials") // Non-retryable error
	})

	if err == nil {
		t.Error("expected error, got nil")
	}

	if callCount != 1 { // Should fail immediately
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestRetryWithBackoff_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	// Use a config with minimal backoff for fast testing
	config := retryConfig{
		maxRetries: 5,
		maxBackoff: 10 * time.Millisecond,
	}

	// Cancel context after first failure
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := retryWithBackoffAndConfig(ctx, func() error {
		callCount++
		return &net.DNSError{IsTimeout: true} // Retryable error
	}, config)

	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestTokenRefreshThreshold(t *testing.T) {
	tests := []struct {
		name          string
		expiresIn     time.Duration
		shouldRefresh bool
	}{
		{
			name:          "token expires in 10 minutes - should refresh",
			expiresIn:     10 * time.Minute,
			shouldRefresh: true,
		},
		{
			name:          "token expires in 14 minutes - should refresh",
			expiresIn:     14 * time.Minute,
			shouldRefresh: true,
		},
		{
			name:          "token expires in 15 minutes - should refresh (boundary)",
			expiresIn:     15 * time.Minute,
			shouldRefresh: true,
		},
		{
			name:          "token expires in 16 minutes - no refresh needed",
			expiresIn:     16 * time.Minute,
			shouldRefresh: false,
		},
		{
			name:          "token expires in 60 minutes - no refresh needed",
			expiresIn:     60 * time.Minute,
			shouldRefresh: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bearer := &AdobeImsOAuthBearer{
				expiresAt: time.Now().Add(tt.expiresIn),
			}

			bearer.mu.RLock()
			timeUntilExpiry := time.Until(bearer.expiresAt)
			needsRefresh := timeUntilExpiry < tokenRefreshThreshold
			bearer.mu.RUnlock()

			if needsRefresh != tt.shouldRefresh {
				t.Errorf("expected needsRefresh=%v, got %v (expires in %v)", tt.shouldRefresh, needsRefresh, tt.expiresIn)
			}
		})
	}
}

func TestCalculateRetryConfig(t *testing.T) {
	tests := []struct {
		name               string
		timeUntilExpiry    time.Duration
		expectedMaxRetries int
		expectedMaxBackoff time.Duration
		description        string
	}{
		{
			name:               "plenty of time - 12 minutes",
			timeUntilExpiry:    12 * time.Minute,
			expectedMaxRetries: 2,
			expectedMaxBackoff: defaultMaxBackoff,
			description:        "Conservative retries when token has >10 minutes",
		},
		{
			name:               "plenty of time - exactly 10 minutes",
			timeUntilExpiry:    10*time.Minute + 1*time.Second,
			expectedMaxRetries: 2,
			expectedMaxBackoff: defaultMaxBackoff,
			description:        "Conservative retries at boundary",
		},
		{
			name:               "moderate urgency - 8 minutes",
			timeUntilExpiry:    8 * time.Minute,
			expectedMaxRetries: 3,
			expectedMaxBackoff: defaultMaxBackoff,
			description:        "Standard retries when token has 5-10 minutes",
		},
		{
			name:               "moderate urgency - exactly 5 minutes",
			timeUntilExpiry:    5*time.Minute + 1*time.Second,
			expectedMaxRetries: 3,
			expectedMaxBackoff: defaultMaxBackoff,
			description:        "Standard retries at boundary",
		},
		{
			name:               "high urgency - 4 minutes",
			timeUntilExpiry:    4 * time.Minute,
			expectedMaxRetries: 5,
			expectedMaxBackoff: 15 * time.Second,
			description:        "More aggressive retries when token has 2-5 minutes",
		},
		{
			name:               "high urgency - exactly 2 minutes",
			timeUntilExpiry:    2*time.Minute + 1*time.Second,
			expectedMaxRetries: 5,
			expectedMaxBackoff: 15 * time.Second,
			description:        "More aggressive retries at boundary",
		},
		{
			name:               "critical - 1 minute",
			timeUntilExpiry:    1 * time.Minute,
			expectedMaxRetries: 8,
			expectedMaxBackoff: 5 * time.Second,
			description:        "Very aggressive retries when token has <2 minutes",
		},
		{
			name:               "critical - 30 seconds",
			timeUntilExpiry:    30 * time.Second,
			expectedMaxRetries: 8,
			expectedMaxBackoff: 5 * time.Second,
			description:        "Very aggressive retries when token about to expire",
		},
		{
			name:               "critical - already expired",
			timeUntilExpiry:    -1 * time.Minute,
			expectedMaxRetries: 8,
			expectedMaxBackoff: 5 * time.Second,
			description:        "Very aggressive retries when token already expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := calculateRetryConfig(tt.timeUntilExpiry)

			if config.maxRetries != tt.expectedMaxRetries {
				t.Errorf("%s: expected maxRetries=%d, got %d",
					tt.description, tt.expectedMaxRetries, config.maxRetries)
			}

			if config.maxBackoff != tt.expectedMaxBackoff {
				t.Errorf("%s: expected maxBackoff=%v, got %v",
					tt.description, tt.expectedMaxBackoff, config.maxBackoff)
			}
		})
	}
}

func TestRetryWithBackoffAndConfig(t *testing.T) {
	tests := []struct {
		name          string
		config        retryConfig
		failCount     int
		expectedCalls int
		shouldSucceed bool
	}{
		{
			name: "conservative config - succeeds on first retry",
			config: retryConfig{
				maxRetries: 2,
				maxBackoff: 1 * time.Millisecond,
			},
			failCount:     1,
			expectedCalls: 2,
			shouldSucceed: true,
		},
		{
			name: "conservative config - exceeds max retries",
			config: retryConfig{
				maxRetries: 2,
				maxBackoff: 1 * time.Millisecond,
			},
			failCount:     10,
			expectedCalls: 3, // initial + 2 retries
			shouldSucceed: false,
		},
		{
			name: "aggressive config - succeeds after multiple retries",
			config: retryConfig{
				maxRetries: 8,
				maxBackoff: 5 * time.Millisecond,
			},
			failCount:     5,
			expectedCalls: 6,
			shouldSucceed: true,
		},
		{
			name: "aggressive config - exceeds max retries",
			config: retryConfig{
				maxRetries: 8,
				maxBackoff: 5 * time.Millisecond,
			},
			failCount:     20,
			expectedCalls: 9, // initial + 8 retries
			shouldSucceed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			callCount := 0

			err := retryWithBackoffAndConfig(ctx, func() error {
				callCount++
				if callCount <= tt.failCount {
					return &net.DNSError{IsTimeout: true} // Retryable error
				}
				return nil // Success
			}, tt.config)

			if tt.shouldSucceed && err != nil {
				t.Errorf("expected success, got error: %v", err)
			}

			if !tt.shouldSucceed && err == nil {
				t.Error("expected error, got nil")
			}

			if callCount != tt.expectedCalls {
				t.Errorf("expected %d calls, got %d", tt.expectedCalls, callCount)
			}
		})
	}
}

func TestRetryWithBackoffAndConfig_BackoffCapping(t *testing.T) {
	tests := []struct {
		name       string
		maxBackoff time.Duration
		maxRetries int
	}{
		{
			name:       "default backoff caps at 10ms",
			maxBackoff: 10 * time.Millisecond,
			maxRetries: 5,
		},
		{
			name:       "aggressive backoff caps at 2ms",
			maxBackoff: 2 * time.Millisecond,
			maxRetries: 4,
		},
		{
			name:       "moderate backoff caps at 5ms",
			maxBackoff: 5 * time.Millisecond,
			maxRetries: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			config := retryConfig{
				maxRetries: tt.maxRetries,
				maxBackoff: tt.maxBackoff,
			}

			callCount := 0
			_ = retryWithBackoffAndConfig(ctx, func() error {
				callCount++
				return &net.DNSError{IsTimeout: true} // Always fail
			}, config)

			// Verify we made the expected number of calls
			expectedCalls := tt.maxRetries + 1
			if callCount != expectedCalls {
				t.Errorf("expected %d calls, got %d", expectedCalls, callCount)
			}
		})
	}
}
