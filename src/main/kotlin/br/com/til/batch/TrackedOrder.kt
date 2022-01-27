package br.com.til.batch

import java.math.BigDecimal
import java.util.*

class TrackedOrder(
    val trackingNumber : String,
    var freeShipping: Boolean = false,
    orderId: Long,
    firstName: String,
    lastName: String,
    email: String,
    cost: BigDecimal,
    itemId: String,
    itemName: String,
    shipDate: Date,

    ) : Order(orderId, firstName, lastName, email, cost, itemId, itemName, shipDate)
