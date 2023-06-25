package com.traderepublic.mrschyzo.portfoliovaluation.utilities

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class DataPoint(
    val userId: UUID,
    val amount: BigDecimal,
    val timestamp: Instant,
)
