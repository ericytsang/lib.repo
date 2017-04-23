package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

class MockMirrorRepoAdapter:MirrorRepoAdapter<MockMirrorRepoAdapter.MockItem.Pk,MockMirrorRepoAdapter.MockItem>
{
    data class MockItem(
        override val pk:Pk,
        override val updateStamp:Long?,
        override val deleteStamp:Long?,
        override val isSynced:Boolean,
        override val isDeleted:Boolean)
        :DeltaRepo.Item<MockItem.Pk,MockItem>
    {
        data class Pk(
            override val nodePk:DeltaRepo.Pk,
            override val pk:Repo.Item.Pk)
            :DeltaRepo.Item.Pk
        override fun copy(
            updateStamp:Long?,
            deleteStamp:Long?,
            isSynced:Boolean,
            isDeleted:Boolean)
            :MockItem
        {
            return copy(pk,updateStamp,deleteStamp,isSynced,isDeleted)
        }
    }

    private val records = mutableMapOf<MockItem.Pk,MockItem>()

    override fun <R> read(block:()->R):R
    {
        return block()
    }

    override fun <R> write(block:()->R):R
    {
        return block()
    }

    override val pk:DeltaRepo.Pk = DeltaRepoPk(1)

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
        return MockItem.Pk(pk,RepoItemPk(prevId++))
    }

    override fun selectNextUnsyncedToSync(limit:Int):List<MockItem>
    {
        return records.values.filter {!it.isSynced}
    }
}
