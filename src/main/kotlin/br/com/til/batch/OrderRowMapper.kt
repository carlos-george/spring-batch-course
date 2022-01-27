package br.com.til.batch

import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet


class OrderRowMapper : RowMapper<Order> {
    override fun mapRow(rs: ResultSet, rowNum: Int): Order {
        return Order(
            orderId = rs.getLong("order_id"),
            firstName= rs.getString("first_name"),
            lastName= rs.getString("last_name"),
            email= rs.getString("email"),
            cost= rs.getBigDecimal("cost"),
            itemId= rs.getString("item_id"),
            itemName= rs.getString("item_name"),
            shipDate= rs.getDate("ship_date"),
        )
    }

}
