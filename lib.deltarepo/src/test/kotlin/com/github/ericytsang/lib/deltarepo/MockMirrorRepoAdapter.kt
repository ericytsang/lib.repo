package com.github.ericytsang.lib.deltarepo

class MockMirrorRepoAdapter:SimpleMirrorRepo.Adapter<MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

    override val BATCH_SIZE:Int = 500

    override var deleteCount:Int = 0

    override fun selectByPk(pk:DeltaRepo.Item.Pk):MockItem?
    {
        return records[MockItem.Pk(pk)]
    }

    override fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int):List<MockItem>
    {
        return records.values
            .asSequence()
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!)}
            .filter {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED}
            .toList()
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun insertOrReplace(item:MockItem)
    {
        records[item.pk] = item
    }

    override fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
    {
        records.values.removeAll {it.pk.value in pks}
    }

    override fun setAllPulledToPushed()
    {
        records.values.filter {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED}.forEach()
        {
            records[it.pk] = it.copy(syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)
        }
    }

    override fun merge(dirtyLocalItem:MockItem?,pulledRemoteItem:MockItem):MockItem
    {
        return if (dirtyLocalItem == null) pulledRemoteItem else pulledRemoteItem.copy(string = dirtyLocalItem.string+pulledRemoteItem.string)
    }

    override var nextId = Long.MIN_VALUE

    override fun selectDirtyItemsToPush(limit:Int):List<MockItem>
    {
        return records.values.filter {it.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY}.take(limit)
    }

    override fun deleteAllPushed()
    {
        records.values.removeAll {it.syncStatus == DeltaRepo.Item.SyncStatus.PUSHED}
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
