package org.teehr.iceberg.auth;

import java.time.Instant;

record BrokerToken(String accessToken, Instant expiresAt) {

  boolean expiresWithinSeconds(long seconds, Instant now) {
    return !expiresAt.isAfter(now.plusSeconds(seconds));
  }
}
