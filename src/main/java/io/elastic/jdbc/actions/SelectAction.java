package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectAction implements Module {

  private static final Logger LOGGER = LoggerFactory.getLogger(SelectAction.class);
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_NULLABLE_RESULT = "nullableResult";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";
  private static final String PROPERTY_EMIT_AS_ARRAY = "emitAsArray";

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    checkConfig(configuration);
    String dbEngine = configuration.getString("dbEngine");
    String sqlQuery = configuration.getString("sqlQuery");
    Integer skipNumber = 0;
    Boolean nullableResult = false;
    Boolean emitAsArray = false;

    if (Utils.getNonNullString(configuration, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    } else if (Utils.getNonNullString(snapshot, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    }

    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null) {
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    }

    if (Utils.getNonNullString(configuration, PROPERTY_EMIT_AS_ARRAY).equals("true")) {
      emitAsArray = true;
    } else if (Utils.getNonNullString(snapshot, PROPERTY_EMIT_AS_ARRAY).equals("true")) {
      emitAsArray = true;
    }

    Utils.columnTypes = Utils.getVariableTypes(sqlQuery);
    LOGGER.info("Detected column types: " + Utils.columnTypes);
    LOGGER.info("Executing select action");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      LOGGER.info("SQL Query: {}", sqlQuery);
      ArrayList<JsonObject> resultList;
      try(Connection connection = Utils.getConnection(configuration)){
        resultList = query.executeSelectQuery(connection, sqlQuery, body);
      }


      if(emitAsArray) {

        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (int i=0; i < resultList.size(); i++) {
          builder.add(resultList.get(i));
        }

        JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
        objectBuilder.add("resultSet", builder);
        JsonObject object = objectBuilder.build();

        LOGGER.info("Emitting data {}", object.toString());
        parameters.getEventEmitter()
                .emitData(new Message.Builder().body(object).build());
      } else {
        for (int i = 0; i < resultList.size(); i++) {
          LOGGER.info("Columns count: {} from {}", i + 1, resultList.size());
          LOGGER.info("Emitting data {}", resultList.get(i).toString());
          parameters.getEventEmitter()
                  .emitData(new Message.Builder().body(resultList.get(i)).build());
        }
      }

      if (resultList.size() == 0 && nullableResult) {
        resultList.add(Json.createObjectBuilder()
            .add("empty dataset", "no data")
            .build());
        LOGGER.info("Emitting data {}", resultList.get(0));
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(0)).build());
      } else if (resultList.size() == 0 && !nullableResult) {
        LOGGER.info("Empty response. Error message will be returned");
        throw new RuntimeException("Empty response");
      }

      snapshot = Json.createObjectBuilder()
          .add(PROPERTY_SKIP_NUMBER, skipNumber + resultList.size())
          .add(SQL_QUERY_VALUE, sqlQuery)
          .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
      LOGGER.info("Emitting new snapshot {}", snapshot.toString());
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonString sqlQuery = config.getJsonString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.toString().isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }
  }
}
