package com.traderepublic.mrschyzo.portfoliovaluation.perf

import com.traderepublic.mrschyzo.portfoliovaluation.utilities.DataPoint
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.Resolution
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@Testcontainers
class PostgresTest: PerformanceTest() {
    companion object {
        @Container
        private val postgres = PostgreSQLContainer("postgres:15-alpine3.18")
            .withUsername("user")
            .withPassword("password")
            .withDatabaseName("portfolio-valuation")
            .withInitScript("postgres/init.sql")
    }

    override fun setup() = Unit

    override fun writeAllDataPoints(dataPoints: Sequence<Pair<DataPoint, Resolution>>): Duration =
        DriverManager.getConnection(
            "jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/${postgres.databaseName}",
            "user",
            "password"
        ).use { conn ->
            stopwatch {
                dataPoints.chunked(32*1024).forEach { chunk ->
                    conn.prepareStatement("insert into portfolio_valuation values (?,?,?,?)").use { batchStatement ->
                        chunk.forEach { (dataPoint, resolution) ->
                            batchStatement.setObject(1, dataPoint.userId)
                            batchStatement.setBigDecimal(2, dataPoint.amount)
                            batchStatement.setTimestamp(3, Timestamp.from(dataPoint.timestamp))
                            batchStatement.setInt(4, resolution.id)
                            batchStatement.addBatch()
                        }
                        batchStatement.executeBatch()
                    }
                }
            }.first
        }

    override fun readAllUsersResolutions(users: Collection<UUID>, resolutions: Collection<Resolution>): Duration =
        DriverManager.getConnection(
            "jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/${postgres.databaseName}",
            "user",
            "password"
        ).use { conn ->
            val inputs = users.flatMap { resolutions.map(it::to) }
            val now = Instant.now().plus(30, ChronoUnit.DAYS)
            stopwatch {
                inputs.forEach { (user, resolution) ->
                    conn.prepareStatement("select \"timestamp\", amount from portfolio_valuation where user_id = ? and resolution = ? and \"timestamp\" between ? and ?").use { statement ->
                        statement.setObject(1, user)
                        statement.setInt(2, resolution.id)
                        statement.setTimestamp(3, now.minusNanos(resolution.duration.toNanos() * 150).let(Timestamp::from))
                        statement.setTimestamp(4, Timestamp.from(now))
                        statement.executeQuery().takeIf(ResultSet::next)?.use {
                            it.getBigDecimal(2)
                        }
                    }
                }
            }.first
        }
}
