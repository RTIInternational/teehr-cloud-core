package org.teehr.iceberg.auth;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.DefaultAuthSession;

final class BrokerBackedAuthSession implements AuthSession {

  private final BrokerTokenClient tokenClient;
  private final String brokerUrl;
  private final String userId;
  private final String sessionId;
  private final String realm;
  private final String catalog;
  private final String audience;
  private final Supplier<String> subjectTokenSupplier;
  private final Duration timeout;
  private final long requestedTtlSeconds;
  private final long refreshSkewSeconds;

  private final AtomicReference<BrokerToken> cachedToken = new AtomicReference<>();

  BrokerBackedAuthSession(
      BrokerTokenClient tokenClient,
      String brokerUrl,
      String userId,
      String sessionId,
      String realm,
      String catalog,
      String audience,
      Supplier<String> subjectTokenSupplier,
      Duration timeout,
      long requestedTtlSeconds,
      long refreshSkewSeconds) {
    this.tokenClient = tokenClient;
    this.brokerUrl = brokerUrl;
    this.userId = userId;
    this.sessionId = sessionId;
    this.realm = realm;
    this.catalog = catalog;
    this.audience = audience;
    this.subjectTokenSupplier = subjectTokenSupplier;
    this.timeout = timeout;
    this.requestedTtlSeconds = requestedTtlSeconds;
    this.refreshSkewSeconds = refreshSkewSeconds;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    BrokerToken token = currentToken();
    AuthSession delegate =
        DefaultAuthSession.of(HTTPHeaders.of(Map.of("Authorization", "Bearer " + token.accessToken())));
    return delegate.authenticate(request);
  }

  private BrokerToken currentToken() {
    BrokerToken token = cachedToken.get();
    Instant now = Instant.now();

    if (token != null && !token.expiresWithinSeconds(refreshSkewSeconds, now)) {
      return token;
    }

    synchronized (this) {
      token = cachedToken.get();
      now = Instant.now();
      if (token != null && !token.expiresWithinSeconds(refreshSkewSeconds, now)) {
        return token;
      }

      try {
        String subjectToken = subjectTokenSupplier.get();
        if (subjectToken == null || subjectToken.isBlank()) {
          throw new IllegalStateException("Subject token supplier returned an empty token");
        }

        BrokerToken refreshed =
            tokenClient.mintToken(
                brokerUrl,
                userId,
                sessionId,
                realm,
                catalog,
                audience,
                subjectToken,
                requestedTtlSeconds,
                timeout);
        cachedToken.set(refreshed);
        return refreshed;
      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("Unable to acquire broker-backed Polaris token", e);
      }
    }
  }

  @Override
  public void close() {
    cachedToken.set(null);
  }
}
