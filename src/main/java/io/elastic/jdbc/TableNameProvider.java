package io.elastic.jdbc;

import io.elastic.api.SelectModelProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableNameProvider implements SelectModelProvider {

  private static final Logger logger = LoggerFactory.getLogger(TableNameProvider.class);

  public JsonObject getSelectModel(JsonObject configuration) {
    logger.info("About to retrieve table name");

    JsonObjectBuilder result = Json.createObjectBuilder();
    Connection connection = null;
    ResultSet rs = null;

    com.google.gson.JsonObject transformedConfig = SailorVersionsAdapter
        .javaxToGson(configuration);

    try {
      connection = Utils.getConnection(transformedConfig);
      logger.info("Successfully connected to DB");

      // get metadata
      DatabaseMetaData md = connection.getMetaData();

      // get table names
      String[] types = {"TABLE", "VIEW"};
      rs = md.getTables(null, "%", "%", types);

      // put table names to result
      String tableName;
      String schemaName;
      Boolean isEmpty = true;

      while (rs.next()) {
        tableName = rs.getString("TABLE_NAME");
        schemaName = rs.getString("TABLE_SCHEM");
        if (configuration.get("dbEngine").toString().toLowerCase().equals("oracle")
            && isOracleServiceSchema(schemaName)) {
          continue;
        }
        if (schemaName != null) {
          tableName = schemaName + "." + tableName;
        }
        result.add(tableName, tableName);
        isEmpty = false;
      }
      if (isEmpty) {
        result.add("", "no tables");
      }
    } catch (SQLException e) {
      logger.error("Unexpected error", e);
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          logger.error(e.toString());
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.toString());
        }
      }
    }
    return result.build();
  }

  private boolean isOracleServiceSchema(String schema) {
    List<String> schemas = Arrays
        .asList("APPQOSSYS", "CTXSYS", "DBSNMP", "DIP", "OUTLN", "RDSADMIN", "SYS", "SYSTEM");
    return schemas.indexOf(schema) > -1;
  }
}
