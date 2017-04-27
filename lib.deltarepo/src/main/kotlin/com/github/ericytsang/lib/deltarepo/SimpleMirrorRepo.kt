package com.github.ericytsang.lib.deltarepo

class SimpleMirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:MirrorRepoAdapter<ItemPk,Item>):BaseRepo(),MutableMirrorRepo<ItemPk,Item>
{
    override fun push(remote:Pushable<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)
    {
        checkCanWrite()

        // push updates
        run {
            do
            {
                val toPush = adapter.selectNextUnsyncedToSync(DeltaRepo.BATCH_SIZE)
                val pushed = remote.insertOrReplace(toPush,remoteRepoInterRepoId,localRepoInterRepoId)
                    .asSequence()
                    .map {it.copy(Unit,syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)}
                    .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                    .toList()
                pushed.forEach {adapter.insertOrReplace(it)}
            }
            while (toPush.isNotEmpty())
        }
    }

    override fun pull(remote:Pullable<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)
    {
        checkCanWrite()

        // pull updates
        run {
            do
            {
                val maxUpdateStampItem = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,setOf(DeltaRepo.Item.SyncStatus.PULLED)).singleOrNull()
                val maxUpdateStamp = maxUpdateStampItem?.updateStamp ?: Long.MIN_VALUE
                val newUpdates = remote.pageByUpdateStamp(maxUpdateStamp,Order.ASC,DeltaRepo.BATCH_SIZE,setOf(DeltaRepo.Item.SyncStatus.PULLED))
                    .asSequence()
                    .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                    .filter {it.pk != maxUpdateStampItem?.pk}
                    .toList()
                newUpdates.forEach {adapter.insertOrReplace(it)}
            }
            while (newUpdates.isNotEmpty())
        }

        // delete items whose delete stamps are less than source's minimum
        run {
            val minDeleteStampItem = remote.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,1,setOf(DeltaRepo.Item.SyncStatus.PULLED))
                .asSequence()
                .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                .singleOrNull()
            val minDeleteStamp = minDeleteStampItem?.updateStamp ?: Long.MAX_VALUE
            adapter.delete(minDeleteStamp)
        }
    }

    override fun insertOrReplace(items:Iterable<Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk):Set<Item>
    {
        checkCanWrite()
        val itemsToInsert = items
            .asSequence()
            .localized(localRepoInterRepoId,remoteRepoInterRepoId)
            .map {
                val existing = adapter.selectByPk(it.pk)
                it.copy(Unit,
                    syncStatus = DeltaRepo.Item.SyncStatus.DIRTY,
                    updateStamp = existing?.updateStamp)
            }
            .toSet()
        itemsToInsert.forEach {adapter.insertOrReplace(it)}
        return itemsToInsert
    }

    override fun deleteByPk(pk:ItemPk)
    {
        checkCanWrite()
        val item = selectByPk(pk) ?: return
        insertOrReplace(listOf(item.copy(Unit,isDeleted = true)))
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

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>):List<Item>
    {
        checkCanRead()
        return adapter.pageByUpdateStamp(start,order,limit,syncStatus)
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
