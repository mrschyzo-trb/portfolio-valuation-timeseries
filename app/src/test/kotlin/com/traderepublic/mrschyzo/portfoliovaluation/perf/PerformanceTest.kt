package com.traderepublic.mrschyzo.portfoliovaluation.perf

import com.traderepublic.mrschyzo.portfoliovaluation.utilities.DataPoint
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.Resolution
import com.traderepublic.mrschyzo.portfoliovaluation.utilities.Resolution.Companion.resolution
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.math.RoundingMode.HALF_UP
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.DAYS
import java.util.UUID
import java.util.concurrent.TimeUnit.MINUTES
import kotlin.random.Random

abstract class PerformanceTest {
    abstract fun setup()
    abstract fun writeAllDataPoints(dataPoints: Sequence<Pair<DataPoint, Resolution>>): Duration
    abstract fun readAllUsersResolutions(users: Collection<UUID>, resolutions: Collection<Resolution>): Duration

    @Test
    @Timeout(10, unit = MINUTES)
    fun `performance test`() {
        val dataPoints = logActivity("💡Generating datapoints for users", ::generateDataPoints)
        val users = dataPoints.map { (point, _) -> point.userId }.distinct()
        val resolutions = Resolution.values().toList()

        logActivity("🛠️Setting up the test", ::setup)

        val writesPerSecond = logActivity("🤮Vomiting ${dataPoints.size} datapoints") {
            writeAllDataPoints(dataPoints = dataPoints.asSequence())
                .toNanos()
                .let { dataPoints.size.toDouble() / it * 1_000_000_000 }
                .toBigDecimal()
                .setScale(2, HALF_UP)
        }
        val readsPerSecond = logActivity("😋Ingesting user datapoints (${users.size * resolutions.size} combinations)") {
            val combos = users.size * resolutions.size
            readAllUsersResolutions(users, resolutions)
                .toNanos()
                .let { combos.toDouble() / it * 1_000_000_000 }
                .toBigDecimal()
                .setScale(2, HALF_UP)
        }

        println(
"""📊Results:
🖋️ $writesPerSecond w/s
📖 $readsPerSecond r/s
""".trimIndent()
        )
    }

    private fun generateDataPoints(): List<Pair<DataPoint, Resolution>> {
        val now = Instant.now().truncatedTo(DAYS)
        val userCount = 128
        val dataPointCount = 365 * 24 * 6
        val dataPoints = (0 until userCount)
            .map { UUID.randomUUID() }
            .flatMap { user ->
                (0 until dataPointCount).map { iteration ->
                    val timestamp = now.plus(iteration * 10L, ChronoUnit.MINUTES)
                    val resolution = timestamp.atOffset(ZoneOffset.UTC).resolution()
                    DataPoint(
                        userId = user,
                        timestamp = timestamp,
                        amount = Random.nextDouble(1e0, 1e20).toBigDecimal()
                    ) to resolution
                }
            }

        return dataPoints
    }

    protected inline fun <T: Any> logActivity(message: String, block: () -> T): T {
        print("$message...")
        System.out.flush()
        try {
            return block().also {
                println("✅")
            }
        } catch (ex: Throwable) {
            println("❌$ex")
            throw ex
        }
    }
    protected inline fun <T: Any> stopwatch(block: () -> T): Pair<Duration, T> =
        System.nanoTime().let { start ->
            block().let(Duration.ofNanos(System.nanoTime() - start)::to)
        }
}
