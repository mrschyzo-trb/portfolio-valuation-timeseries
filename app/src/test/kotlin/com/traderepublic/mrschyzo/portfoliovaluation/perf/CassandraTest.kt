package com.traderepublic.mrschyzo.portfoliovaluation.perf

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder
import com.datastax.oss.driver.api.core.cql.BatchType
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.DataPoint
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.Resolution
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.net.InetSocketAddress
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.random.Random

@Testcontainers
class CassandraTest: PerformanceTest() {
    companion object {

        @Container
        private val cassandra = CassandraContainer("cassandra:4.1.2")
            .withInitScript("cassandra/init.cql")
    }

    private lateinit var session: CqlSession

    @BeforeEach
    fun setMeUp() {
        System.setProperty("datastax-java-driver.basic.request.timeout", "10 seconds")

        session = listOf(cassandra)
            .fold(CqlSession.builder()) { builder, cass ->
                builder.addContactPoint(InetSocketAddress(cass.host, cass.firstMappedPort))
            }
            .withLocalDatacenter("datacenter1")
            .build()
    }

    @AfterEach
    fun tearDown() {
        session.close()
    }

    override fun setup() = Unit

    override fun writeAllDataPoints(dataPoints: Sequence<Pair<DataPoint, Resolution>>): Duration =
        stopwatch {
            val statement = session.prepare("insert into pv.portfolio_valuation (user_id, resolution, time, amount) values (:user, :res, :time, :amount)")
            dataPoints.chunked(768).forEach { chunk ->
                val start = BatchStatement.builder(BatchType.UNLOGGED)
                chunk.fold(start) { builder, (point, res) ->
                    statement.bind(
                        point.userId,
                        res.label,
                        point.timestamp,
                        point.amount
                    ).let(builder::addStatement)
                }.build().let { session.execute(it) }
            }
        }.first

    override fun readAllUsersResolutions(users: Collection<UUID>, resolutions: Collection<Resolution>): Duration {
        val inputs = users.flatMap { resolutions.map(it::to) }
        val now = Instant.now().plus(30, ChronoUnit.DAYS)
        return stopwatch {
            val select = session.prepareAsync("select time, amount from pv.portfolio_valuation where user_id = :user and resolution = :res and time >= :start and time <= :end")
            inputs.forEach { (user, resolution) ->
                select.thenApply {
                    it.bind(
                        user,
                        resolution.label,
                        now.minusNanos(resolution.duration.toNanos() * 150),
                        now
                    )
                }.thenCompose(session::executeAsync)
                .thenApply {
                    it.currentPage().first().toString()
                }.toCompletableFuture().get()
            }
        }.first
    }
}
