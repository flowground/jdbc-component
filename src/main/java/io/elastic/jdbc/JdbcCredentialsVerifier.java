package io.elastic.jdbc;

import io.elastic.api.CredentialsVerifier;
import io.elastic.api.InvalidCredentialsException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCredentialsVerifier implements CredentialsVerifier {

  private static final Logger logger = LoggerFactory.getLogger(JdbcCredentialsVerifier.class);

  @Override
  public void verify(JsonObject configuration) throws InvalidCredentialsException {

    logger.info("About to connect to database using given credentials");

    Connection connection = null;

    try {
      com.google.gson.JsonObject transformedConfig = SailorVersionsAdapter
          .javaxToGson(configuration);
      connection = Utils.getConnection(transformedConfig);
      logger.info("Successfully connected to database. Credentials verified.");
    } catch (Exception e) {
      throw new InvalidCredentialsException("Failed to connect to database", e);
    } finally {
      if (connection != null) {
        logger.info("Closing database connection");
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error("Failed to closed database connection", e);
        }
      }
    }
  }
}
