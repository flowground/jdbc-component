package io.elastic.jdbc

import com.google.gson.JsonObject
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Ignore
class TableNameProviderSpec extends Specification {

    @Shared
    def connectionString = "jdbc:hsqldb:mem:tests"
    @Shared
    def user = "sa"
    @Shared
    def password = ""

    @Shared
    Connection connection

    @Shared
    JsonObject config

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
        config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "mem:tests")
    }

    def cleanup() {
        connection.createStatement().execute("DROP TABLE users IF EXISTS");
        connection.createStatement().execute("DROP TABLE products IF EXISTS");
        connection.createStatement().execute("DROP TABLE orders IF EXISTS");
    }

    def "get selectbox values, successful"() {


        String sql1 = "CREATE TABLE users (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql2 = "CREATE TABLE products (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql3 = "CREATE TABLE orders (id int, name varchar(255) NOT NULL, radius int, destination int)";

        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);


        TableNameProvider provider = new TableNameProvider();

        JsonObject model = provider.getSelectModel(config);

        expect:
        model.toString() == '{"PUBLIC.ORDERS":"PUBLIC.ORDERS","PUBLIC.PRODUCTS":"PUBLIC.PRODUCTS","PUBLIC.USERS":"PUBLIC.USERS"}'
    }


    def "get selectbox values, no tables"() {

        TableNameProvider provider = new TableNameProvider();

        JsonObject model = provider.getSelectModel(config);

        expect:
        model.toString() == '{"":"no tables"}'
    }
}
