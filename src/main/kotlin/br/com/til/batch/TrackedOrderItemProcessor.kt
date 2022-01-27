package br.com.til.batch

import org.springframework.batch.item.ItemProcessor
import java.util.*

class TrackedOrderItemProcessor : ItemProcessor<Order, TrackedOrder> {
    override fun process(order: Order): TrackedOrder {
        println("Processing order with id: ${order.orderId}")
        return TrackedOrder(
            trackingNumber = getTrackingNumber(),
            orderId =order.orderId,
            firstName =order.firstName,
            lastName =order.lastName,
            email =order.email,
            cost =order.cost,
            itemId =order.itemId,
            itemName =order.itemName,
            shipDate =order.shipDate,
        )
    }

    private fun getTrackingNumber() = if (Math.random() < .1) throw OrderProcessingException() else UUID.randomUUID().toString()

}
