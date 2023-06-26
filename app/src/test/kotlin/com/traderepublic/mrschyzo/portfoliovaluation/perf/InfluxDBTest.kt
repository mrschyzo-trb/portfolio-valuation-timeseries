package com.traderepublic.mrschyzo.portfoliovaluation.perf

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.domain.Query
import com.influxdb.client.domain.WritePrecision.NS
import com.influxdb.client.write.Point
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.DataPoint
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.Resolution
import org.testcontainers.containers.InfluxDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@Testcontainers
class InfluxDBTest : PerformanceTest() {
    companion object {
        @Container
        private val influx = InfluxDBContainer(DockerImageName.parse("influxdb:2.7.1-alpine"))
            .withUsername("user")
            .withPassword("password")
            .withOrganization("org")
            .withBucket("bucket")
            .withRetention("0")
    }

    private lateinit var client: InfluxDBClient

    override fun setup() {
        client = InfluxDBClientOptions
            .builder()
            .url(influx.url)
            .authenticate(influx.username, influx.password.toCharArray())
            .bucket(influx.bucket)
            .org(influx.organization)
            .build()
            .let(InfluxDBClientFactory::create)
    }

    override fun writeAllDataPoints(dataPoints: Sequence<Pair<DataPoint, Resolution>>): Duration =
        with(client.writeApiBlocking) {
            stopwatch {
                dataPoints.chunked(16 * 1024).forEach { chunk ->
                    chunk.map { (point, res) ->
                        Point.measurement("portfolio")
                            .time(point.timestamp, NS)
                            .addTag("user", point.userId.toString())
                            .addTag("resolution", res.label)
                            .addField("amount", point.amount)
                    }.also(this::writePoints)
                }
            }.first
        }

    override fun readAllUsersResolutions(users: Collection<UUID>, resolutions: Collection<Resolution>): Duration =
        stopwatch {
            val now = Instant.now().plus(300, ChronoUnit.DAYS)

            with(client.queryApi) {
                val inputs = users.flatMap { resolutions.map(it::to) }
                inputs.forEach { (user, res) ->
                    val prev = now.minusNanos(res.duration.toNanos() * 150)
                    """
                        from(bucket: "bucket")
                        |> range(start: ${prev.epochSecond}, stop: ${now.epochSecond})
                        |> filter(fn: (r) => r.user == "$user")
                        |> aggregateWindow(every: ${res.label}, fn: last)
                    """.trimIndent()
                        .let(this::query)

                }
            }
        }.first

}
