package com.github.ericytsang.lib.deltarepo

class MockMirrorRepoAdapter:MirrorRepoAdapter<MockItem.Pk,MockItem>
{
    val records = mutableMapOf<MockItem.Pk,MockItem>()

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

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>):List<MockItem>
    {
        return records.values
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!) && !it.isDeleted}
            .filter {it.syncStatus in syncStatus}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun insertOrReplace(item:MockItem)
    {
        records[item.pk] = item
    }

    override fun delete(minUpdateStampToKeep:Long)
    {
        records.values.removeAll()
        {
            val deleteStamp = it.updateStamp ?: return@removeAll false
            deleteStamp < minUpdateStampToKeep
        }
    }

    private var prevId = Long.MIN_VALUE

    override fun computeNextPk():MockItem.Pk
    {
        return MockItem.Pk(DeltaRepo.LOCAL_NODE_ID,RepoItemPk(prevId++))
    }

    override fun selectNextUnsyncedToSync(limit:Int):List<MockItem>
    {
        return records.values.filter {it.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY}.take(limit)
    }
}
