package io.elastic.jdbc

import com.google.gson.JsonObject
import spock.lang.Ignore
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ColumnNamesProviderSpec extends Specification {

    def setup() {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:tests", "sa", "");
        String sql = "DROP TABLE IF EXISTS STARS";
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE STARS (id int IDENTITY NOT NULL, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)";
        connection.createStatement().execute(sql);
    }

    def cleanup() {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:tests", "sa", "");
        String sql = "DROP TABLE IF EXISTS STARS";
        connection.createStatement().execute(sql);
    }

    def "get metadata model, given table name"() {

        JsonObject config = new JsonObject()
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("user", "sa")
        config.addProperty("tableName", "STARS")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "tests")
        ColumnNamesProvider provider = new ColumnNamesProvider()

        expect:
        provider.getMetaModel((config)).toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"ISDEAD\":{\"required\":false,\"title\":\"ISDEAD\",\"type\":\"boolean\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"ISDEAD\":{\"required\":false,\"title\":\"ISDEAD\",\"type\":\"boolean\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}}}"
    }

    def "should throw the exception when tableName is not set"() {
        JsonObject config = new JsonObject()
        config.addProperty("engine", "hsqldb")
        config.addProperty("user", " ")
        config.addProperty("password", " ")
        when:
        ColumnNamesProvider provider = new ColumnNamesProvider()
        provider.getColumns(config)
        then:
        def e = thrown(RuntimeException)
        e.message == "Table name is required"
    }
}
