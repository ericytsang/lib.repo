package com.github.ericytsang.lib.deltarepo

class MockMasterRepoAdapter:SimpleMasterRepo.Adapter<MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

    override val BATCH_SIZE:Int = 3

    override val MAX_DELETED_ITEMS_TO_RETAIN:Long = 3

    override var deleteCount:Int = 0

    override fun selectByPk(pk:DeltaRepo.Item.Pk):MockItem?
    {
        return records[MockItem.Pk(pk)]
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<MockItem>
    {
        return records.values
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!)}
            .filter {isDeleted == null || it.isDeleted == isDeleted}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun insertOrReplace(items:Iterable<MockItem>)
    {
        items.forEach {records[it.pk] = it}
    }

    override fun merge(local:MockItem?,remote:MockItem):MockItem
    {
        return if (local == null) remote else remote.copy(string = local.string+remote.string)
    }

    override fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
    {
        records.values.removeAll {it.pk.value in pks}
    }

    private var prevUpdateStamp = 0L//todo Long.MIN_VALUE

    override fun computeNextUpdateStamp():Long
    {
        return prevUpdateStamp++
    }

    override val MockItem.metadata:DeltaRepo.Item.Metadata get()
    {
        return metadata(DeltaRepo.Item.Companion)
    }

    override fun MockItem.copy(newMetadata:DeltaRepo.Item.Metadata):MockItem
    {
        return copy(DeltaRepo.Item.Companion,newMetadata.pk,newMetadata.updateStamp,newMetadata.syncStatus,newMetadata.isDeleted)
    }
}
