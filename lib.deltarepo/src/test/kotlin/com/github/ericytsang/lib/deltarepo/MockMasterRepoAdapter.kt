package com.github.ericytsang.lib.deltarepo

class MockMasterRepoAdapter:SimpleMasterRepo.Adapter<MockItem.Pk,MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

    override val BATCH_SIZE:Int = 3

    override val MAX_DELETED_ITEMS_TO_RETAIN:Long = 3

    override var deleteCount:Int = 0

    override fun <R> read(block:()->R):R
    {
        return block()
    }

    override fun <R> write(block:()->R):R
    {
        return block()
    }

    override fun selectByPk(pk:MockItem.Pk):MockItem?
    {
        return records[pk]
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>,isDeleted:Boolean?):List<MockItem>
    {
        return records.values
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!)}
            .filter {isDeleted == null || it.isDeleted == isDeleted}
            .filter {it.syncStatus in syncStatus}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun insertOrReplace(items:Iterable<MockItem>)
    {
        items.forEach {records[it.pk] = it}
    }

    override fun merge(local:MockItem,remote:MockItem):MockItem
    {
        return remote.copy(string = local.string+remote.string)
    }

    override fun deleteByPk(pks:Set<MockItem.Pk>)
    {
        records.values.removeAll {it.pk in pks}
    }

    private var prevId = Long.MIN_VALUE

    override fun computeNextPk():MockItem.Pk
    {
        return MockItem.Pk(DeltaRepo.LOCAL_NODE_ID,DeltaRepo.ItemPk(prevId++))
    }

    private var prevUpdateStamp = 0L//todo Long.MIN_VALUE

    override fun computeNextUpdateStamp():Long
    {
        return prevUpdateStamp++
    }
}
