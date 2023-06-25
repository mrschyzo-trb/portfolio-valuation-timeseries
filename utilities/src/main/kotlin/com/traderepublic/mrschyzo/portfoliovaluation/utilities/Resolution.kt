package com.traderepublic.mrschyzo.portfoliovaluation.utilities

import java.time.DayOfWeek.MONDAY
import java.time.Duration
import java.time.OffsetDateTime

enum class Resolution(val id: Int, val duration: Duration, val label: String) {
    TEN_MINUTES(1, Duration.ofMinutes(10L), "10m"),
    ONE_HOUR(2, Duration.ofHours(1L), "1h"),
    FOUR_HOURS(3, Duration.ofHours(4L), "4h"),
    ONE_DAY(4, Duration.ofHours(24L), "1d"),
    ONE_WEEK(5, Duration.ofDays(7L), "1w");

    companion object {
        fun OffsetDateTime.resolution() =
            when {
                minute != 0 -> TEN_MINUTES
                hour % 4 != 0 -> ONE_HOUR
                hour != 0 -> FOUR_HOURS
                dayOfWeek != MONDAY -> ONE_DAY
                else -> ONE_WEEK
            }
    }
}
