package io.elastic.jdbc

import com.google.gson.JsonObject
import io.elastic.api.InvalidCredentialsException
import spock.lang.Ignore
import spock.lang.Specification

@Ignore
class JdbcCredentialsVerifierSpec extends Specification {

    def "should verify successfully when connection succeeds"() {
        setup:
        JsonObject config = new JsonObject()
        config.addProperty("user", "sa")
        config.addProperty("password", "")
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "mem:tests")

        when:
        new JdbcCredentialsVerifier().verify(config)

        then:
        notThrown(Throwable.class)
    }

    def "should not verify when connection fails"() {
        setup:
        JsonObject config = new JsonObject()
        config.addProperty("user", "admin")
        config.addProperty("password", "secret")
        config.addProperty("dbEngine", "mysql")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "testdb")

        when:
        new JdbcCredentialsVerifier().verify(config)

        then:
        def e = thrown(InvalidCredentialsException.class)
        e.message == "Failed to connect to database"
    }
}
