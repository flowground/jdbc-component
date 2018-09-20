package io.elastic.jdbc;

import com.google.gson.JsonParser;
import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonReader;

public class SailorVersionsAdapter {

  public static javax.json.JsonObject gsonToJavax(com.google.gson.JsonObject json) {
    JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
    javax.json.JsonObject jsonObject = jsonReader.readObject();
    jsonReader.close();

    return jsonObject;
  }

  public static com.google.gson.JsonObject javaxToGson(javax.json.JsonObject json) {
    return new JsonParser().parse(json.toString()).getAsJsonObject();
  }

}
