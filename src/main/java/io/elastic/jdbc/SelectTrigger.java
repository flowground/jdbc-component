package io.elastic.jdbc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import javax.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectTrigger implements Module {

  private static final Logger logger = LoggerFactory.getLogger(SelectTrigger.class);
  public static final String PROPERTY_TABLE_NAME = "tableName";
  public static final String PROPERTY_ORDER_FIELD = "orderField";

  @Override
  public final void execute(ExecutionParameters parameters) {

    logger.info("About to execute select action");
    JsonArray rows = new JsonArray();
    javax.json.JsonObject config = parameters.getConfiguration();

    com.google.gson.JsonObject transformedConfig = SailorVersionsAdapter
        .javaxToGson(config);

    Connection connection = Utils.getConnection(transformedConfig);
    checkConfig(transformedConfig);

    String dbEngine = transformedConfig.get("dbEngine").getAsString();
    String tableName = transformedConfig.get(PROPERTY_TABLE_NAME).getAsString();
    String orderField = transformedConfig.get("orderField").getAsString();

    javax.json.JsonObject snapshot = parameters.getSnapshot();
    Integer skipNumber = 0;
    if (snapshot.get("skipNumber") != null) {
      skipNumber = snapshot.getInt("skipNumber");
    }

    if (snapshot.get(PROPERTY_TABLE_NAME) != null && !snapshot.getString(PROPERTY_TABLE_NAME)
        .equals(tableName)) {
      skipNumber = 0;
    }
    ResultSet rs = null;
    logger.info("Executing select action");
    try {
      TriggerQueryFactory queryFactory = new TriggerQueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      query.from(tableName).skip(skipNumber).orderBy(orderField);
      rs = query.execute(connection);
      ResultSetMetaData metaData = rs.getMetaData();
      while (rs.next()) {
        JsonObject row = new JsonObject();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          String rsString = rs.getString(metaData.getColumnName(i));
          if (metaData.getColumnType(i) == Types.TIMESTAMP) {
            rsString = rs.getTimestamp(metaData.getColumnName(i)).toString();
          } else if (metaData.getColumnType(i) == Types.DATE) {
            rsString = rs.getDate(metaData.getColumnName(i)).toString();
          } else if (metaData.getColumnType(i) == Types.TIME) {
            rsString = rs.getTime(metaData.getColumnName(i)).toString();
          }
          row.addProperty(metaData.getColumnName(i), rsString);
        }
        rows.add(row);
        logger.info("Emitting data");
        logger.info(row.toString());
        javax.json.JsonObject data = SailorVersionsAdapter.gsonToJavax(row);
        parameters.getEventEmitter().emitData(new Message.Builder().body(data).build());
      }

      snapshot = Json.createObjectBuilder()
          .add("skipNumber", skipNumber + rows.size())
          .add(PROPERTY_TABLE_NAME, tableName)
          .build();
      logger.info("Emitting new snapshot {}", snapshot.toString());
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      logger.error("Failed to make request", e.toString());
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
  }

  private void checkConfig(JsonObject config) {
    final JsonElement tableName = config.get(PROPERTY_TABLE_NAME);
    final JsonElement orderField = config.get(PROPERTY_ORDER_FIELD);

    if (tableName == null || tableName.isJsonNull() || tableName.getAsString().isEmpty()) {
      throw new RuntimeException("Table name is required field");
    }

    if (orderField == null || orderField.isJsonNull() || orderField.getAsString().isEmpty()) {
      throw new RuntimeException("Order column is required field");
    }
  }
}
