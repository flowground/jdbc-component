package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnNamesProvider implements DynamicMetadataProvider, SelectModelProvider {

  private static final Logger logger = LoggerFactory.getLogger(ColumnNamesProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObject properties = getColumns(configuration);
    for (Map.Entry<String, JsonValue> entry : properties.entrySet()) {
      JsonValue field = entry.getValue();
      result.add(entry.getKey(), field.toString());
    }
    return result.build();
  }

  /**
   * Returns Columns list as metadata
   */

  public JsonObject getMetaModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObjectBuilder inMetadata = Json.createObjectBuilder();
    JsonObject properties = getColumns(configuration);
    inMetadata.add("type", "object")
        .add("properties", properties);
    result.add("out", inMetadata);
    result.add("in", inMetadata);
    return result.build();
  }

  public JsonObject getColumns(JsonObject configuration) {
    if (configuration.get("tableName") == null ||
        configuration.get("tableName").toString().isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
    String tableName = configuration.get("tableName").toString();
    JsonObjectBuilder properties = Json.createObjectBuilder();
    Connection connection = null;
    ResultSet rs = null;
    String schemaName = null;
    Boolean isEmpty = true;

    com.google.gson.JsonObject transformedConfig = SailorVersionsAdapter
        .javaxToGson(configuration);
    try {
      connection = Utils.getConnection(transformedConfig);
      DatabaseMetaData dbMetaData = connection.getMetaData();
      if (tableName.contains(".")) {
        schemaName = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
      }
      rs = dbMetaData.getColumns(null, schemaName, tableName, "%");
      while (rs.next()) {
        JsonObjectBuilder field = Json.createObjectBuilder();
        String name = rs.getString("COLUMN_NAME");
        Boolean isRequired =
            rs.getInt("NULLABLE") == 0 && !rs.getString("IS_AUTOINCREMENT").equals("YES");
        field.add("required", isRequired);
        field.add("title", name);
        field.add("type", convertType(rs.getInt("DATA_TYPE")));
        properties.add(name, field);
        isEmpty = false;
      }
      if (isEmpty) {
        properties.add("", "no columns");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          logger.error("Failed to close result set", e.toString());
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error("Failed to close connection", e.toString());
        }
      }
    }
    return properties.build();
  }

  /**
   * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
   *
   * @param sqlType JDBC column type
   * @url http://db.apache.org/ojb/docu/guides/jdbc-types.html
   */
  private String convertType(Integer sqlType) {
    if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
        || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
        || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
      return "number";
    }
    if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
      return "boolean";
    }
    return "string";
  }
}
