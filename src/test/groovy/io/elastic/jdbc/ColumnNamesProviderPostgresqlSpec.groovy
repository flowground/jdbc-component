package io.elastic.jdbc

import com.google.gson.JsonObject
import spock.lang.Ignore
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ColumnNamesProviderPostgresqlSpec extends Specification {

    def setup() {
        Connection connection = DriverManager.getConnection("", "", "");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE stars (id SERIAL NOT NULL, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)";
        connection.createStatement().execute(sql);
        connection.close();
    }

    def cleanup() {
        Connection connection = DriverManager.getConnection("", "", "");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
        connection.close();
    }

    def "get metadata model, given table name"() {

        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "")
        config.addProperty("dbEngine", "postgresql")
        config.addProperty("user", "")
        config.addProperty("password", "")
        config.addProperty("tableName", "stars")
        config.addProperty("host", "")
        ColumnNamesProvider provider = new ColumnNamesProvider()

        expect:
        provider.getMetaModel((config)).toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isdead\":{\"required\":false,\"title\":\"isdead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isdead\":{\"required\":false,\"title\":\"isdead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}}}"
    }
}
