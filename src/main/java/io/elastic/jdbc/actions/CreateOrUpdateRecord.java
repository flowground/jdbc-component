package io.elastic.jdbc.actions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.Engines;
import io.elastic.jdbc.SailorVersionsAdapter;
import io.elastic.jdbc.Utils;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateOrUpdateRecord implements Module {

  private static final Logger logger = LoggerFactory.getLogger(CreateOrUpdateRecord.class);

  private Connection connection = null;
  private Map<String, String> columnTypes = null;
  private boolean isOracle = false;

  @Override
  public void execute(ExecutionParameters parameters) {
    final javax.json.JsonObject configuration = parameters.getConfiguration();
    final javax.json.JsonObject body = parameters.getMessage().getBody();
    if (!configuration.containsKey("tableName") || configuration.get("tableName") == null
        || configuration.get("tableName").toString().isEmpty()) {
      throw new RuntimeException("Table name is required field");
    }
    if (!configuration.containsKey("idColumn") || configuration.get("idColumn") == null
        || configuration.get("idColumn").toString().isEmpty()) {
      throw new RuntimeException("ID column is required field");
    }
    String tableName = configuration.get("tableName").toString();
    String idColumn = configuration.get("idColumn").toString();
    String idColumnValue = null;
    if (!(!body.containsKey(idColumn) || body.get(idColumn) == null
        || body.get(idColumn).toString().isEmpty())) {
      idColumnValue = body.get(idColumn).toString();
    }
    logger.info("ID column value: {}", idColumnValue);
    String db = configuration.get(Utils.CFG_DB_ENGINE).toString();
    isOracle = db.equals(Engines.ORACLE.name().toLowerCase());

    com.google.gson.JsonObject transformedConfig = SailorVersionsAdapter
        .javaxToGson(configuration);

    try {
      connection = Utils.getConnection(transformedConfig);
      columnTypes = getColumnTypes(tableName);
      logger.info("Detected column types: " + columnTypes);
      if (recordExists(tableName, idColumn, idColumnValue)) {
        makeUpdate(tableName, idColumn, idColumnValue, body);
      } else {
        makeInsert(tableName, body);
      }
      parameters.getEventEmitter().emitData(new Message.Builder().body(body).build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.toString());
        }
      }
    }
  }

  private Map<String, String> getColumnTypes(String tableName) {
    DatabaseMetaData md;
    ResultSet rs = null;
    Map<String, String> columnTypes = new HashMap<String, String>();
    String schemaName = null;
    try {
      md = connection.getMetaData();
      if (tableName.contains(".")) {
        schemaName = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
      }
      if (isOracle) {
        tableName = tableName.toUpperCase();
      }
      rs = md.getColumns(null, schemaName, tableName, "%");
      while (rs.next()) {
        String name = rs.getString("COLUMN_NAME").toLowerCase();
        String type = detectColumnType(rs.getInt("DATA_TYPE"));
        columnTypes.put(name, type);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (Exception e) {
          logger.error(e.toString());
        }
      }
    }
    return columnTypes;
  }

  private String detectColumnType(Integer sqlType) {
    if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
        || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
        || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
      return "number";
    }
    if (sqlType == Types.TIMESTAMP) {
      return "timestamp";
    }
    if (sqlType == Types.DATE) {
      return "date";
    }
    if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
      return "boolean";
    }
    return "string";
  }

  private String getColumnType(String columnName) {
    return columnTypes.get(columnName.toLowerCase());
  }

  private boolean isNumeric(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("number");
  }

  private boolean isTimestamp(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("timestamp");
  }

  private boolean isDate(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("date");
  }

  private void setStatementParam(PreparedStatement statement, int paramNumber, String colName,
      String colValue) throws SQLException {
    if (isNumeric(colName)) {
      statement.setBigDecimal(paramNumber, new BigDecimal(colValue));
    } else if (isTimestamp(colName)) {
      statement.setTimestamp(paramNumber, Timestamp.valueOf(colValue));
    } else if (isDate(colName)) {
      statement.setDate(paramNumber, Date.valueOf(colValue));
    } else {
      statement.setString(paramNumber, colValue);
    }
  }

  private boolean recordExists(String tableName, String idColumn, String idValue)
      throws SQLException {
    String query = "SELECT COUNT(*) FROM " + tableName + " WHERE " + idColumn + " = ?";
    PreparedStatement statement = connection.prepareStatement(query);
    setStatementParam(statement, 1, idColumn, idValue);
    logger.info("{}", statement);
    ResultSet rs = statement.executeQuery();
    rs.next();
    return rs.getInt(1) > 0;
  }

  private void makeInsert(String tableName, javax.json.JsonObject body) throws SQLException {
    JsonObject transformedBody = SailorVersionsAdapter.javaxToGson(body);

    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (Map.Entry<String, JsonElement> entry : transformedBody.entrySet()) {
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
    }
    String sql =
        "INSERT INTO " + tableName + " (" + keys.toString() + ") VALUES (" + values.toString()
            + ")";
    PreparedStatement statement = connection.prepareStatement(sql);
    int i = 1;
    for (Map.Entry<String, JsonElement> entry : transformedBody.entrySet()) {
      setStatementParam(statement, i, entry.getKey(), entry.getValue().getAsString());
      i++;
    }
    logger.debug("{}", statement);
    statement.execute();
  }

  private void makeUpdate(String tableName, String idColumn, String idValue,
      javax.json.JsonObject body) throws SQLException {
    JsonObject transformedBody = SailorVersionsAdapter.javaxToGson(body);

    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonElement> entry : transformedBody.entrySet()) {
      if (setString.length() > 0) {
        setString.append(",");
      }
      setString.append(entry.getKey()).append(" = ?");
    }
    String sql =
        "UPDATE " + tableName + " SET " + setString.toString() + " WHERE " + idColumn + " = ?";
    PreparedStatement statement = connection.prepareStatement(sql);
    int i = 1;
    for (Map.Entry<String, JsonElement> entry : transformedBody.entrySet()) {
      setStatementParam(statement, i, entry.getKey(), entry.getValue().getAsString());
      i++;
    }
    setStatementParam(statement, i, idColumn, idValue);
    statement.execute();
  }
}