package org.teehr.iceberg.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

final class BrokerTokenClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpClient httpClient;

  BrokerTokenClient(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  BrokerToken mintToken(
      String brokerUrl,
      String userId,
      String sessionId,
      String realm,
      String catalog,
      String audience,
      String brokerSessionToken,
      String subjectToken,
      long requestedTtlSeconds,
      Duration timeout)
      throws IOException, InterruptedException {

    Map<String, Object> body = new HashMap<>();
    body.put("user_id", userId);
    body.put("session_id", sessionId);
    body.put("realm", realm);
    body.put("catalog", catalog);
    body.put("requested_ttl_seconds", requestedTtlSeconds);
    body.put("audience", audience);

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(brokerUrl))
            .header("Content-Type", "application/json")
            .timeout(timeout)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)));

    if (brokerSessionToken != null && !brokerSessionToken.isBlank()) {
      requestBuilder.header("X-Broker-Session-Token", brokerSessionToken);
    } else {
      requestBuilder.header("Authorization", "Bearer " + subjectToken);
    }

    HttpRequest request = requestBuilder.build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (shouldFallbackToSubjectExchange(response, brokerUrl, brokerSessionToken, subjectToken)) {
      String directBrokerUrl = brokerUrl.replace("/auth/polaris-token/session", "/auth/polaris-token");
      HttpRequest directRequest =
          HttpRequest.newBuilder(URI.create(directBrokerUrl))
              .header("Content-Type", "application/json")
              .header("Authorization", "Bearer " + subjectToken)
              .timeout(timeout)
              .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
              .build();

      response = httpClient.send(directRequest, HttpResponse.BodyHandlers.ofString());
    }

    if (response.statusCode() != 200) {
      String bodySnippet = response.body() == null ? "" : response.body();
      if (bodySnippet.length() > 500) {
        bodySnippet = bodySnippet.substring(0, 500);
      }
      String authMode =
          brokerSessionToken != null && !brokerSessionToken.isBlank()
              ? "broker-session-header"
              : "subject-authorization-header";
      throw new IOException(
          "Broker token request failed with status "
              + response.statusCode()
              + " against "
              + brokerUrl
              + " using "
              + authMode
              + ": "
              + bodySnippet);
    }

    JsonNode payload = MAPPER.readTree(response.body());
    String accessToken = payload.path("access_token").asText(null);
    long expiresAtEpoch = payload.path("expires_at_epoch_seconds").asLong(0);

    if (accessToken == null || expiresAtEpoch <= 0) {
      throw new IOException("Broker response missing access_token or expires_at_epoch_seconds");
    }

    return new BrokerToken(accessToken, Instant.ofEpochSecond(expiresAtEpoch));
  }

  private static boolean shouldFallbackToSubjectExchange(
      HttpResponse<String> response,
      String brokerUrl,
      String brokerSessionToken,
      String subjectToken) {
    if (response.statusCode() != 401) {
      return false;
    }
    if (brokerSessionToken == null || brokerSessionToken.isBlank()) {
      return false;
    }
    if (subjectToken == null || subjectToken.isBlank()) {
      return false;
    }
    if (!brokerUrl.endsWith("/auth/polaris-token/session")) {
      return false;
    }

    String body = response.body();
    if (body == null || body.isBlank()) {
      return false;
    }

    return body.contains("Delegated broker session not found")
        || body.contains("Delegated broker session expired");
  }
}
