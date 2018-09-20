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
class CreateOrUpdateRecordSpec extends Specification {

    @Shared
    def connectionString = "jdbc:hsqldb:mem:tests"
    @Shared
    def user = "sa"
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
        JsonObject config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("host", "localhost")
        config.addProperty("tableName", "STARS")
        config.addProperty("idColumn", "id")
        config.addProperty("databaseName", "mem:tests")
        return config;
    }

    def prepareStarsTable() {
        connection.createStatement().execute("DROP TABLE stars IF EXISTS");
        connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, radius int, destination int)");
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

    def "one insert"() {

        prepareStarsTable();

        JsonObject snapshot = new JsonObject()

        JsonObject body = new JsonObject()
        body.addProperty("id", "1")
        body.addProperty("name", "Taurus")
        body.addProperty("radius", "123")

        runAction(getStarsConfig(), body, snapshot)

        ArrayList<String> records = getRecords("stars")

        expect:
        records.size() == 1
        records.get(0) == '{ID=1, NAME=Taurus, RADIUS=123, DESTINATION=null}'
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
        records.get(0) == '{ID=1, NAME=Taurus, RADIUS=123, DESTINATION=null}'
        records.get(1) == '{ID=2, NAME=Eridanus, RADIUS=456, DESTINATION=null}'
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
        records.get(0) == '{ID=1, NAME=Eridanus, RADIUS=123, DESTINATION=null}'
    }


    def getPersonsConfig() {
        JsonObject config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "mem:tests")
        config.addProperty("tableName", "PERSONS")
        config.addProperty("idColumn", "email")
        return config;
    }

    def preparePersonsTable() {
        connection.createStatement().execute("DROP TABLE persons IF EXISTS");
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
        records.get(0) == '{ID=1, NAME=O\'Henry, EMAIL=ohenry@elastic.io}'
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
        records.get(0) == '{ID=1, NAME=User1, EMAIL=user1@elastic.io}'
        records.get(1) == '{ID=3, NAME=User3, EMAIL=user2@elastic.io}'
    }


}
