package io.elastic.jdbc.actions

import com.google.gson.JsonObject
import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Ignore
class CreateOrUpdateRecordPostgresqlSpec extends Specification {

    @Shared
    def connectionString = ""
    @Shared
    def user = ""
    @Shared
    def password = ""

    @Shared
    Connection connection

    @Shared
    EventEmitter.Callback errorCallback
    @Shared
    EventEmitter.Callback snapshotCallback
    @Shared
    EventEmitter.Callback dataCallback
    @Shared
    EventEmitter.Callback reboundCallback
    @Shared
    EventEmitter emitter
    @Shared
    CreateOrUpdateRecord action

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
    }

    def setup() {
        createAction()
    }

    def createAction() {
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback).onRebound(reboundCallback).build()
        action = new CreateOrUpdateRecord(emitter)
    }

    def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
        action.execute(params);
    }

    def getStarsConfig() {
        return getTableConfig('stars', 'id');
    }

    def getTableConfig(tableName, idColumn) {
        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "")
        config.addProperty("dbEngine", "postgresql")
        config.addProperty("idColumn", idColumn)
        config.addProperty("user", "")
        config.addProperty("password", "")
        config.addProperty("tableName", tableName)
        config.addProperty("host", "")

        return config;
    }

    def prepareStarsTable() {
        connection.createStatement().execute("DROP TABLE IF EXISTS stars");
        connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, date timestamp, radius int, destination int)");
    }

    def prepareTableWithDecimals() {
        connection.createStatement().execute("DROP TABLE IF EXISTS decimals");
        connection.createStatement().execute("CREATE TABLE decimals (id int, name varchar(255) NOT NULL, decimal_value numeric(30,9) NOT NULL)");
    }

    def getRecords(tableName) {
        ArrayList<String> records = new ArrayList<String>();
        String sql = "SELECT * FROM " + tableName;
        ResultSet rs = connection.createStatement().executeQuery(sql);
        while (rs.next()) {
            records.add(rs.toRowResult().toString());
        }
        rs.close();
        return records;
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS persons")
        connection.createStatement().execute("DROP TABLE IF EXISTS stars")
        connection.close()
    }

    def "one insert"() {

        prepareStarsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body = new JsonObject()
        body.addProperty("id", "1")
        body.addProperty("name", "Taurus")
        body.addProperty("date", "2015-02-19 12:12:12")
        body.addProperty("radius", "123")

        runAction(getStarsConfig(), body, snapshot)

        ArrayList<String> records = getRecords("stars")

        expect:
        records.size() == 1
        records.get(0) == '{id=1, name=Taurus, date=2015-02-19 12:12:12.0, radius=123, destination=null}'
    }

    def "one insert, incorrect value: string in integer field"() {

        prepareStarsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body = new JsonObject()
        body.addProperty("id", "1")
        body.addProperty("name", "Taurus")
        body.addProperty("radius", "test")

        String exceptionClass = "";

        try {
            runAction(getStarsConfig(), body, snapshot)
        } catch (Exception e) {
            exceptionClass = e.getClass().getName();
        }

        expect:
        exceptionClass.contains('Exception')
    }

    def "two inserts"() {

        prepareStarsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body1 = new JsonObject()
        body1.addProperty("id", "1")
        body1.addProperty("name", "Taurus")
        body1.addProperty("radius", "123")

        runAction(getStarsConfig(), body1, snapshot)

        JsonObject body2 = new JsonObject()
        body2.addProperty("id", "2")
        body2.addProperty("name", "Eridanus")
        body2.addProperty("radius", "456")

        runAction(getStarsConfig(), body2, snapshot)

        ArrayList<String> records = getRecords("stars")

        expect:
        records.size() == 2
        records.get(0) == '{id=1, name=Taurus, date=null, radius=123, destination=null}'
        records.get(1) == '{id=2, name=Eridanus, date=null, radius=456, destination=null}'
    }

    def "one insert, one update by ID"() {

        prepareStarsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body1 = new JsonObject()
        body1.addProperty("id", "1")
        body1.addProperty("name", "Taurus")
        body1.addProperty("radius", "123")

        runAction(getStarsConfig(), body1, snapshot)

        JsonObject body2 = new JsonObject()
        body2.addProperty("id", "1")
        body2.addProperty("name", "Eridanus")

        runAction(getStarsConfig(), body2, snapshot)

        ArrayList<String> records = getRecords("stars")

        expect:
        records.size() == 1
        records.get(0) == '{id=1, name=Eridanus, date=null, radius=123, destination=null}'
    }


    def getPersonsConfig() {
        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "")
        config.addProperty("dbEngine", "postgresql")
        config.addProperty("idColumn", "email")
        config.addProperty("user", "")
        config.addProperty("password", "")
        config.addProperty("tableName", "persons")
        config.addProperty("host", "")
        return config;
    }

    def preparePersonsTable() {
        connection.createStatement().execute("DROP TABLE IF EXISTS persons");
        connection.createStatement().execute("CREATE TABLE persons (id int, name varchar(255) NOT NULL, email varchar(255) NOT NULL)");
    }

    def "one insert, name with quote"() {

        preparePersonsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body1 = new JsonObject()
        body1.addProperty("id", "1")
        body1.addProperty("name", "O'Henry")
        body1.addProperty("email", "ohenry@elastic.io")
        runAction(getPersonsConfig(), body1, snapshot)

        ArrayList<String> records = getRecords("persons")

        expect:
        records.size() == 1
        records.get(0) == '{id=1, name=O\'Henry, email=ohenry@elastic.io}'
    }

    def "two inserts, one update by email"() {

        preparePersonsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body1 = new JsonObject()
        body1.addProperty("id", "1")
        body1.addProperty("name", "User1")
        body1.addProperty("email", "user1@elastic.io")
        runAction(getPersonsConfig(), body1, snapshot)

        JsonObject body2 = new JsonObject()
        body2.addProperty("id", "2")
        body2.addProperty("name", "User2")
        body2.addProperty("email", "user2@elastic.io")
        runAction(getPersonsConfig(), body2, snapshot)

        JsonObject body3 = new JsonObject()
        body3.addProperty("id", "3")
        body3.addProperty("name", "User3")
        body3.addProperty("email", "user2@elastic.io")
        runAction(getPersonsConfig(), body3, snapshot)

        ArrayList<String> records = getRecords("persons")

        expect:
        records.size() == 2
        records.get(0) == '{id=1, name=User1, email=user1@elastic.io}'
        records.get(1) == '{id=3, name=User3, email=user2@elastic.io}'
    }


    def "insert decimals"() {

        prepareTableWithDecimals();

        JsonObject snapshot = new JsonObject()

        JsonObject body = new JsonObject()
        body.addProperty("id", "1")
        body.addProperty("name", "Igor")
        body.addProperty("decimal_value", "2100.000000000")

        runAction(getTableConfig('decimals', 'id'), body, snapshot)

        ArrayList<String> records = getRecords("decimals")

        expect:
        records.size() == 1
        //records.get(0) == '{id=1, name=Taurus, date=2015-02-19 12:12:12.0, radius=123, destination=null}'
    }


}
