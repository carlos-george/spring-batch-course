package br.com.til.batch

import org.springframework.batch.item.ItemReader

class SimpleItemReader : ItemReader<String> {
    val dataSet = mutableListOf<String>("1","2","3","4","5")

    val iterator = dataSet.iterator()

    override fun read(): String? {
        return if(iterator.hasNext()) iterator.next() else null
    }

}
