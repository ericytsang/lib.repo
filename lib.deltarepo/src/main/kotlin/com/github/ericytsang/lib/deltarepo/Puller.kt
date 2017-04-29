package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class Puller<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:Adapter<ItemPk,Item>)
{
    interface Remote<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
    {
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Result<ItemPk,Item>

        data class Result<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(val items:List<Item>,val deleteCount:Int):Serializable
    }

    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
    {
        /**
         * size of batch to use when doing incremental operations.
         */
        val BATCH_SIZE:Int

        /**
         * persistent, mutable integer initialized to 0. used by context to count
         * the number of records that are deleted by [deleteByPk].
         */
        var deleteCount:Int

        /**
         * returns the [Item] whose [DeltaRepo.Item.pk] == [pk]; null if not exists.
         */
        fun selectByPk(pk:ItemPk):Item?

        /**
         * selects the first [limit] records where [DeltaRepo.Item.updateStamp] is
         * equal to or after [start] when records are sorted in [order] order.
         * [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.PULLED]
         */
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>

        /**
         * takes care of merging [pulledRemoteItem] into [dirtyLocalItem].
         */
        fun merge(dirtyLocalItem:Item,pulledRemoteItem:Item):Item

        /**
         * inserts [item] into the repo. replaces any existing record with the same
         * [DeltaRepo.Item.pk].
         */
        fun insertOrReplace(item:Item)

        /**
         * deletes all records where [DeltaRepo.Item.pk] in [pks].
         */
        fun deleteByPk(pks:Set<ItemPk>)

        /**
         * update
         * [DeltaRepo.Item.syncStatus] = [DeltaRepo.Item.SyncStatus.PUSHED]
         * where [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.PULLED]
         */
        fun setAllPulledToPushed()

        /**
         * delete all where
         * [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.PUSHED].
         */
        fun deleteAllPushed()
    }

    private fun _pull(remote:Remote<ItemPk,Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Int
    {
        var remoteDeleteCount:Int

        // pull updates
        do
        {
            val maxUpdateStampItem = adapter.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
            val maxUpdateStamp = maxUpdateStampItem?.updateStamp ?: Long.MIN_VALUE
            val (items,deleteCount) = remote.pageByUpdateStamp(maxUpdateStamp,Order.ASC,adapter.BATCH_SIZE)
            remoteDeleteCount = deleteCount
            val (toDelete,toInsert) = items
                .asSequence()
                // localize updates
                .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                // only process the new updates
                .filter {it.updateStamp!! > maxUpdateStamp}
                // lookup the existing item...
                .map {adapter.selectByPk(it.pk) to it}
                // ignore redundant deletes
                .filter {
                    (existing,update) ->
                    !update.isDeleted || (update.isDeleted && existing?.isDeleted == false)
                }
                // merge if needed
                .map {
                    (existing,update) ->
                    when (existing?.syncStatus)
                    {
                        DeltaRepo.Item.SyncStatus.DIRTY ->
                        {
                            if (existing.updateStamp!! == update.updateStamp!!)
                            {
                                existing
                            }
                            else
                            {
                                adapter.merge(existing,update).copy(
                                    DeltaRepo.Item.Companion,
                                    updateStamp = update.updateStamp!!,
                                    syncStatus = DeltaRepo.Item.SyncStatus.DIRTY)
                            }
                        }
                        DeltaRepo.Item.SyncStatus.PUSHED,
                        DeltaRepo.Item.SyncStatus.PULLED,
                        null -> update
                    }
                }
                .partition {it.isDeleted}

            // insert items
            toInsert.forEach {adapter.insertOrReplace(it)}

            // delete items and do book keeping
            toDelete
                .map {it.pk}
                .toSet()
                .let {
                    adapter.deleteByPk(it)
                    adapter.deleteCount += it.size
                }
        }
        while (toInsert.size+toDelete.size >= adapter.BATCH_SIZE)

        return remoteDeleteCount
    }

    fun pull(remote:Remote<ItemPk,Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        // pull data
        val remoteDeleteCount = _pull(remote,localRepoInterRepoId,remoteRepoInterRepoId)

        // pull data and make sure deletes are in sync. get in sync if deletes are not in sync.
        if (adapter.deleteCount != remoteDeleteCount)
        {
            // set status of all PULLED items to PUSHED, and sync them one batch at a time.
            adapter.setAllPulledToPushed()

            // pull updates
            adapter.deleteCount = _pull(remote,localRepoInterRepoId,remoteRepoInterRepoId)

            // delete all pushed records (they did not exist on the master)
            adapter.deleteAllPushed()
        }
    }
}
