package br.com.til.batch

import org.springframework.batch.item.ItemProcessor
import java.math.BigDecimal

class FreeShippingItemProcessor : ItemProcessor<TrackedOrder, TrackedOrder> {

    override fun process(item: TrackedOrder): TrackedOrder? {

        item.freeShipping = item.cost.compareTo(BigDecimal("80")) == 1

        return if(item.freeShipping) item else null
    }
}