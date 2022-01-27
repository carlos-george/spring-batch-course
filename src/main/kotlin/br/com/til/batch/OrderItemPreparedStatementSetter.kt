package br.com.til.batch

import org.springframework.batch.item.database.ItemPreparedStatementSetter
import java.sql.Date
import java.sql.PreparedStatement

class OrderItemPreparedStatementSetter : ItemPreparedStatementSetter<Order> {

    override fun setValues(order: Order, ps: PreparedStatement) {
        ps.setLong(1, order.orderId)
        ps.setString(2, order.firstName)
        ps.setString(3, order.lastName)
        ps.setString(4, order.email)
        ps.setString(5, order.itemId)
        ps.setString(6, order.itemName)
        ps.setBigDecimal(7, order.cost)
        ps.setDate(8, Date(order.shipDate.time))
    }

}
