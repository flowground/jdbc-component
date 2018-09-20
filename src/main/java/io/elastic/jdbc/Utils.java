package io.elastic.jdbc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.sql.Connection;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static final String CFG_DATABASE_NAME = "databaseName";
  public static final String CFG_PASSWORD = "password";
  public static final String CFG_PORT = "port";
  public static final String CFG_DB_ENGINE = "dbEngine";
  public static final String CFG_HOST = "host";
  public static final String CFG_USER = "user";

  public static Connection getConnection(final com.google.gson.JsonObject config) {
    final String engine = getRequiredNonEmptyString(config, CFG_DB_ENGINE, "Engine is required")
        .toLowerCase();
    final String host = getRequiredNonEmptyString(config, CFG_HOST, "Host is required");
    final String user = getRequiredNonEmptyString(config, CFG_USER, "User is required");
    final Engines engineType = Engines.valueOf(engine.toUpperCase());
    final Integer port = getPort(config, engineType);
    final String password = getPassword(config, engineType);
    final String databaseName = getRequiredNonEmptyString(config, CFG_DATABASE_NAME,
        "Database name is required");
    logger.info("DB type {}", engineType);
    engineType.loadDriverClass();
    final String connectionString = engineType.getConnectionString(host, port, databaseName);
    logger.info("Connecting to {}", connectionString);
    try {
      return DriverManager.getConnection(connectionString, user, password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getPassword(final JsonObject config, final Engines engineType) {
    final String password = getNonNullString(config, CFG_PASSWORD);

    if (password.isEmpty() && engineType != Engines.HSQLDB) {
      throw new RuntimeException("Password is required");
    }

    return password;
  }

  private static String getRequiredNonEmptyString(final JsonObject config, final String key,
      final String message) {
    final JsonElement value = config.get(key);
    if (value == null || value.isJsonNull() || value.getAsString().isEmpty()) {
      throw new RuntimeException(message);
    }
    return value.getAsString();
  }


  private static String getNonNullString(final JsonObject config, final String key) {
    final JsonElement value = config.get(key);
    if (value != null && !value.isJsonNull()) {
      return value.getAsString();
    }
    return "";
  }

  private static Integer getPort(final JsonObject config, final Engines engineType) {
    final JsonElement value = config.get(CFG_PORT);
    if (value != null && !value.isJsonNull() && !value.getAsString().isEmpty()) {
      return value.getAsInt();
    }
    return engineType.defaultPort();
  }

}
