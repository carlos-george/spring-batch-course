package br.com.til.batch

import org.springframework.batch.core.SkipListener

class CustomSkipListener : SkipListener<Order, TrackedOrder>{
    override fun onSkipInRead(p0: Throwable) {
    }
    override fun onSkipInWrite(p0: TrackedOrder, p1: Throwable) {
    }
    override fun onSkipInProcess(order: Order, p1: Throwable) {
        println("Skipping processing of item with id: ${order.orderId}")
    }

}
