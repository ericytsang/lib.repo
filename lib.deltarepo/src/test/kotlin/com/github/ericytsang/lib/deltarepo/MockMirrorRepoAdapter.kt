package com.github.ericytsang.lib.deltarepo

class MockMirrorRepoAdapter:SimpleMirrorRepo.Adapter<MockItem.Pk,MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

    override val BATCH_SIZE:Int = 500

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
        return records[pk]?.takeIf {!it.isDeleted}
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<MockItem>
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

    override fun deleteByPk(pks:Set<MockItem.Pk>)
    {
        records.values.removeAll {it.pk in pks}
    }

    override fun setAllPulledToPushed()
    {
        records.values.filter {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED}.forEach()
        {
            records[it.pk] = it.copy(syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)
        }
    }

    override fun merge(dirtyLocalItem:MockItem,pulledRemoteItem:MockItem):MockItem
    {
        return pulledRemoteItem.copy(string = dirtyLocalItem.string+pulledRemoteItem.string)
    }

    private var prevId = Long.MIN_VALUE

    override fun computeNextPk():MockItem.Pk
    {
        return MockItem.Pk(DeltaRepo.LOCAL_NODE_ID,DeltaRepo.ItemPk(prevId++))
    }

    override fun selectNextUnsyncedToSync(limit:Int):List<MockItem>
    {
        return records.values.filter {it.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY}.take(limit)
    }
}
