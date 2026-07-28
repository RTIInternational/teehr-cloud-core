package org.teehr.iceberg.auth;

final class TeehrAuthProperties {

  private TeehrAuthProperties() {}

  static final String PREFIX = "rest.auth.teehr.";

  static final String BROKER_URL = PREFIX + "broker.url";
  static final String BROKER_SESSION_TOKEN = PREFIX + "broker-session-token";
  static final String BROKER_SESSION_TOKEN_ENV = PREFIX + "broker-session-token-env";
  static final String USER_ID = PREFIX + "user-id";
  static final String SESSION_ID = PREFIX + "session-id";
  static final String REALM = PREFIX + "realm";
  static final String CATALOG = PREFIX + "catalog";
  static final String AUDIENCE = PREFIX + "audience";
  static final String SUBJECT_TOKEN = PREFIX + "subject-token";
  static final String SUBJECT_TOKEN_ENV = PREFIX + "subject-token-env";
  static final String REQUESTED_TTL_SECONDS = PREFIX + "requested-ttl-seconds";
  static final String REQUEST_TIMEOUT_MS = PREFIX + "request-timeout-ms";
  static final String REFRESH_SKEW_SECONDS = PREFIX + "refresh-skew-seconds";

  static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
  static final long DEFAULT_REFRESH_SKEW_SECONDS = 60L;
  static final long DEFAULT_REQUESTED_TTL_SECONDS = 600L;
  static final String DEFAULT_AUDIENCE = "account";
  static final String DEFAULT_SUBJECT_TOKEN_ENV = "POLARIS_USER_TOKEN";
  static final String DEFAULT_BROKER_SESSION_TOKEN_ENV = "POLARIS_BROKER_SESSION_TOKEN";
}
