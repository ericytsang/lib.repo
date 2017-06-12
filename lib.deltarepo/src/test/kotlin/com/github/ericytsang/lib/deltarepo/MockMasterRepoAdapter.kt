package com.github.ericytsang.lib.deltarepo

class MockMasterRepoAdapter:MasterRepo.Adapter
{
    val records = mutableMapOf<Long,MockItem>()
    override val BATCH_SIZE:Int = 3
    override val MAX_DELETED_ROWS_TO_RETAIN:Int = 3
    override var minimumIsDeletedStart:Long = Long.MIN_VALUE

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<Item>
    {
        return records
            .values
            .filter {it.isDeleted == isDeleted?:it.isDeleted}
            .filter {order.isAfterOrEqual(start,it.updateSequence)}
            .sortedBy {it.updateSequence}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun select(item:Item):Item?
    {
        item as MockItem
        return records[item.pk]
    }

    override fun insertOrReplace(item:Item)
    {
        item as MockItem
        records[item.pk] = item
    }

    override fun delete(item:Item)
    {
        item as MockItem
        records.remove(item.pk)
    }
}
