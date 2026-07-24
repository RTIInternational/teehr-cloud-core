package org.teehr.iceberg.auth;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype Iceberg AuthManager that sources short-lived bearer tokens from a broker endpoint.
 */
public class TeehrBrokerAuthManager implements AuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(TeehrBrokerAuthManager.class);

  private final String name;
  private volatile AuthSession catalogSession;

  public TeehrBrokerAuthManager(String managerName) {
    this.name = managerName;
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    AuthSession existing = catalogSession;
    if (existing != null) {
      return existing;
    }

    synchronized (this) {
      existing = catalogSession;
      if (existing != null) {
        return existing;
      }

      String brokerUrl = required(properties, TeehrAuthProperties.BROKER_URL);
      String userId = required(properties, TeehrAuthProperties.USER_ID);
      String sessionId = required(properties, TeehrAuthProperties.SESSION_ID);
      String realm = required(properties, TeehrAuthProperties.REALM);
      String catalog = properties.getOrDefault(TeehrAuthProperties.CATALOG, "iceberg");
        String audience =
          properties.getOrDefault(TeehrAuthProperties.AUDIENCE, TeehrAuthProperties.DEFAULT_AUDIENCE);
        Supplier<String> subjectTokenSupplier = subjectTokenSupplier(properties);

      int timeoutMs =
          PropertyUtil.propertyAsInt(
              properties,
              TeehrAuthProperties.REQUEST_TIMEOUT_MS,
              TeehrAuthProperties.DEFAULT_REQUEST_TIMEOUT_MS);
      long refreshSkewSeconds =
          PropertyUtil.propertyAsLong(
              properties,
              TeehrAuthProperties.REFRESH_SKEW_SECONDS,
              TeehrAuthProperties.DEFAULT_REFRESH_SKEW_SECONDS);
      long requestedTtlSeconds =
          PropertyUtil.propertyAsLong(
              properties,
              TeehrAuthProperties.REQUESTED_TTL_SECONDS,
              TeehrAuthProperties.DEFAULT_REQUESTED_TTL_SECONDS);

      BrokerTokenClient tokenClient = new BrokerTokenClient(HttpClient.newHttpClient());

      catalogSession =
          new BrokerBackedAuthSession(
              tokenClient,
              brokerUrl,
              userId,
              sessionId,
              realm,
              catalog,
              audience,
              subjectTokenSupplier,
              Duration.ofMillis(timeoutMs),
              requestedTtlSeconds,
              refreshSkewSeconds);

      LOG.info("Initialized TeehrBrokerAuthManager for catalog {}", name);
      return catalogSession;
    }
  }

  @Override
  public void close() {
    AuthSession session = catalogSession;
    this.catalogSession = null;
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("Error closing broker auth session", e);
      }
    }
  }

  private static String required(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return value;
  }

  private static Supplier<String> subjectTokenSupplier(Map<String, String> properties) {
    String explicitToken = properties.get(TeehrAuthProperties.SUBJECT_TOKEN);
    if (explicitToken != null && !explicitToken.isBlank()) {
      return () -> explicitToken;
    }

    String tokenEnv =
        properties.getOrDefault(
            TeehrAuthProperties.SUBJECT_TOKEN_ENV, TeehrAuthProperties.DEFAULT_SUBJECT_TOKEN_ENV);

    return () -> {
      String token = System.getenv(tokenEnv);
      if (token == null || token.isBlank()) {
        throw new IllegalStateException(
            "Missing subject token: set "
                + TeehrAuthProperties.SUBJECT_TOKEN
                + " or export env var "
                + tokenEnv);
      }
      return token;
    };
  }
}
