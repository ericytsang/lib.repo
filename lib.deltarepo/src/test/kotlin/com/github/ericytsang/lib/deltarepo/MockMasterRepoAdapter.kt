package com.github.ericytsang.lib.deltarepo

class MockMasterRepoAdapter:MasterRepoAdapter<MockItem.Pk,MockItem>
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
        return records[pk]
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<MockItem>
    {
        return records.values
            .filter {it.updateStamp != null}
            .sortedBy {it.updateStamp}
            .filter {order.isAfterOrEqual(start,it.updateStamp!!)}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<MockItem>
    {
        return records.values
            .filter {it.deleteStamp != null}
            .sortedBy {it.deleteStamp}
            .filter {order.isAfterOrEqual(start,it.deleteStamp!!)}
            .let {if (order == Order.DESC) it.asReversed() else it}
            .take(limit)
    }

    override fun insertOrReplace(item:MockItem)
    {
        records[item.pk] = item
    }

    override fun delete(minDeleteStampToKeep:Long)
    {
        records.values.removeAll()
        {
            val deleteStamp = it.deleteStamp ?: return@removeAll false
            deleteStamp < minDeleteStampToKeep
        }
    }

    private var prevId = 0L

    override fun computeNextPk():MockItem.Pk
    {
        return MockItem.Pk(DeltaRepo.LOCAL_NODE_ID,RepoItemPk(prevId++))
    }

    private var prevUpdateStamp = 0L

    override fun computeNextUpdateStamp():Long
    {
        return prevUpdateStamp++
    }

    private var prevDeleteStamp = 0L

    override fun computeNextDeleteStamp():Long
    {
        return prevDeleteStamp++
    }
}
