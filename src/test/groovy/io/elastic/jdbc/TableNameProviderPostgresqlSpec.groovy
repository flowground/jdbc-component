package io.elastic.jdbc

import com.google.gson.JsonObject
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Ignore
class TableNameProviderPostgresqlSpec extends Specification {

    @Shared
    def connectionString = ""
    @Shared
    def user = ""
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
        config.addProperty("dbEngine", "postgresql")
        config.addProperty("host", "")
        config.addProperty("databaseName", "")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS users");
        connection.createStatement().execute("DROP TABLE IF EXISTS products");
        connection.createStatement().execute("DROP TABLE IF EXISTS orders");
        connection.close()
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
        model.toString() == '{"public.decimals":"public.decimals","public.orders":"public.orders","public.products":"public.products","public.tetstable":"public.tetstable","public.users":"public.users","public.pg_stat_statements":"public.pg_stat_statements"}'
    }
}
