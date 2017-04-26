package com.github.ericytsang.lib.deltarepo

class SimpleMirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:MirrorRepoAdapter<ItemPk,Item>):BaseRepo(),MutableMirrorRepo<ItemPk,Item>
{
    override fun synchronizeWith(source:MasterRepo<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)
    {
        checkCanWrite()

        // synchronize updates
        run {
            do
            {
                val maxUpdateStampItem = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
                val maxUpdateStamp = maxUpdateStampItem?.updateStamp ?: Long.MIN_VALUE
                val newUpdates = source.pageByUpdateStamp(maxUpdateStamp,Order.ASC,DeltaRepo.BATCH_SIZE)
                    .asSequence()
                    .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                    .filter {it.pk != maxUpdateStampItem?.pk}
                    .toList()
                newUpdates.forEach {adapter.insertOrReplace(it)}
            }
            while (newUpdates.isNotEmpty())
        }

        // synchronize delete stamps
        run {
            do
            {
                val maxDeleteStampItem = pageByDeleteStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
                val maxDeleteStamp = maxDeleteStampItem?.deleteStamp ?: Long.MIN_VALUE
                val newUpdates = source.pageByDeleteStamp(maxDeleteStamp,Order.ASC,DeltaRepo.BATCH_SIZE)
                    .asSequence()
                    .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                    .filter {it.pk != maxDeleteStampItem?.pk}
                    .toList()
                newUpdates.forEach {adapter.insertOrReplace(it)}
            }
            while (newUpdates.isNotEmpty())
        }

        // delete items whose delete stamps are less than source's minimum
        run {
            val minDeleteStampItem = source.pageByDeleteStamp(Long.MIN_VALUE,Order.ASC,1)
                .asSequence()
                .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                .singleOrNull()
            val minDeleteStamp = minDeleteStampItem?.deleteStamp ?: Long.MAX_VALUE
            adapter.delete(minDeleteStamp)
        }
    }

    override fun insertOrReplace(item:Item)
    {
        checkCanWrite()
        adapter.insertOrReplace(item.copy(Unit,isSynced = false))
    }

    override fun markAsSynced(pk:ItemPk)
    {
        checkCanWrite()
        val item = adapter.selectByPk(pk) ?: return
        adapter.insertOrReplace(item.copy(Unit,isSynced = true))
    }

    override fun deleteByPk(pk:ItemPk)
    {
        checkCanWrite()
        val item = selectByPk(pk) ?: return
        insertOrReplace(item.copy(Unit,isDeleted = true))
    }

    override fun computeNextPk():ItemPk
    {
        checkCanWrite()
        return adapter.computeNextPk()
    }

    override fun selectByPk(pk:ItemPk):Item?
    {
        checkCanRead()
        return adapter.selectByPk(pk)
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.pageByUpdateStamp(start,order,limit)
    }

    override fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.pageByDeleteStamp(start,order,limit)
    }

    override fun selectNextUnsyncedToSync(limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.selectNextUnsyncedToSync(limit)
    }

    override fun <R> read(block:()->R):R
    {
        return super.read {adapter.read(block)}
    }

    override fun <R> write(block:()->R):R
    {
        return super.write {adapter.write(block)}
    }
}
