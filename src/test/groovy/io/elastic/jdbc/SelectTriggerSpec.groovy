package io.elastic.jdbc

import com.google.gson.JsonObject
import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import spock.lang.Ignore
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Ignore
class SelectTriggerSpec extends Specification {

    def setup() {
        String connectionString = "jdbc:hsqldb:tests"
        String user = "sa"
        String password = ""

        Connection connection;
        connection = DriverManager.getConnection(connectionString, user, password)

        String sql = "DROP TABLE IF EXISTS stars"
        connection.createStatement().execute(sql)
        sql = "DROP TABLE IF EXISTS stars2"
        connection.createStatement().execute(sql)

        sql = "CREATE TABLE stars (id int, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)"
        connection.createStatement().execute(sql)

        sql = "CREATE TABLE stars2 (id int, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)"
        connection.createStatement().execute(sql)

        sql = "INSERT INTO stars (id, isDead, name, radius, destination) VALUES (1, false, 'Sun', 50, 170), (2, false, 'Shit', 90, 90000)"
        connection.createStatement().execute(sql)
    }

    def "make a SELECT request"() {

        Callback errorCallback = Mock(Callback)
        Callback snapshotCallback = Mock(Callback)
        Callback dataCallback = Mock(Callback)
        Callback onreboundCallback = Mock(Callback)

        EventEmitter emitter = new EventEmitter.Builder()
                .onData(dataCallback)
                .onSnapshot(snapshotCallback)
                .onError(errorCallback)
                .onRebound(onreboundCallback).build();

        SelectTrigger selectAction = new SelectTrigger(emitter);

        given:
        Message msg = new Message.Builder().build();

        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "tests")
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("orderField", "id")
        config.addProperty("user", "sa")
        config.addProperty("password", "")
        config.addProperty("tableName", "stars")
        config.addProperty("host", "localhost")

        JsonObject snapshot = new JsonObject()
        snapshot.addProperty("skipNumber", 0)

        when:
        ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
        selectAction.execute(params)

        then:
        0 * errorCallback.receive(_)
        1 * dataCallback.receive({
            it.toString() == '{"body":{"ID":"1","ISDEAD":"FALSE","NAME":"Sun","RADIUS":"50","DESTINATION":"170.0E0"},"attachments":{}}'
        })
        1 * dataCallback.receive({
            it.toString() == '{"body":{"ID":"2","ISDEAD":"FALSE","NAME":"Shit","RADIUS":"90","DESTINATION":"90000.0E0"},"attachments":{}}'
        })
        1 * snapshotCallback.receive({ it.toString() == '{"skipNumber":2,"tableName":"stars"}' })
    }

    def "reset a snapshot when table was changed"() {

        Callback errorCallback = Mock(Callback)
        Callback snapshotCallback = Mock(Callback)
        Callback dataCallback = Mock(Callback)
        Callback onreboundCallback = Mock(Callback)

        EventEmitter emitter = new EventEmitter.Builder()
                .onData(dataCallback)
                .onSnapshot(snapshotCallback)
                .onError(errorCallback)
                .onRebound(onreboundCallback).build();

        SelectTrigger selectAction = new SelectTrigger(emitter);

        given:
        Message msg = new Message.Builder().build();

        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "tests")
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("orderField", "id")
        config.addProperty("user", "sa")
        config.addProperty("password", "")
        config.addProperty("tableName", "stars")
        config.addProperty("host", "localhost")

        JsonObject snapshot = new JsonObject()
        snapshot.addProperty("skipNumber", 0)

        when:
        ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
        selectAction.execute(params)

        then:
        1 * snapshotCallback.receive({ it.toString() == '{"skipNumber":2,"tableName":"stars"}' })

        config.addProperty("tableName", "stars2")

        when:
        params = new ExecutionParameters(msg, config, snapshot)
        selectAction.execute(params)

        then:
        1 * snapshotCallback.receive({ it.toString() == '{"skipNumber":0,"tableName":"stars2"}' })

    }
}
