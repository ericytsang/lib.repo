package com.github.ericytsang.lib.deltarepo

class MockMirrorRepoAdapter:SimpleMirrorRepo.Adapter<MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

    override val rowsWhereIsDeletedCount:Int get()
    {
        return records.values.count {it.isDeleted}
    }

    override val BATCH_SIZE:Int = 3

    override var destructiveDeleteCount:Int = 0

    override fun selectByPk(pk:DeltaRepo.Item.Pk):MockItem?
    {
        return records[MockItem.Pk(pk)]
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,pulledOnly:Boolean,isDeleted:Boolean?):List<MockItem>
    {
        return records.values
            .asSequence()
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {it.isDeleted == isDeleted?:it.isDeleted}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!)}
            .filter {!pulledOnly || it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED}
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
}
