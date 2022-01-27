package br.com.til.batch

import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.transform.FieldSet

class OrderFieldSetMapper : FieldSetMapper<Order> {
    override fun mapFieldSet(fieldSet: FieldSet): Order {
        return Order(
            orderId = fieldSet.readLong("order_id"),
            firstName= fieldSet.readString("first_name"),
            lastName= fieldSet.readString("last_name"),
            email= fieldSet.readString("email"),
            cost= fieldSet.readBigDecimal("cost"),
            itemId= fieldSet.readString("item_id"),
            itemName= fieldSet.readString("item_name"),
            shipDate= fieldSet.readDate("ship_date"),
        )
    }

}
