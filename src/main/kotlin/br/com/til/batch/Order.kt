package br.com.til.batch

import java.math.BigDecimal
import java.util.Date
import javax.validation.constraints.Pattern

open class Order(
    open val orderId: Long,
    open  val firstName: String,
    open val lastName: String,
    @field:Pattern(regexp = ".*\\.gov")open  val email: String,
    open  val cost: BigDecimal,
    open val itemId: String,
    open val itemName: String,
    open val shipDate: Date
)
