package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class Puller<Item:DeltaRepo.Item<Item>>(private val adapter:Adapter<Item>)
{
    interface Remote<Item:DeltaRepo.Item<Item>>
    {
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Result<Item>

        data class Result<Item:DeltaRepo.Item<Item>>(val items:List<Item>,val deleteCount:Int):Serializable
    }

    interface Adapter<Item:DeltaRepo.Item<Item>>
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
         * returns the [Item] whose [DeltaRepo.Item.Metadata.pk] == [pk]; null if not exists.
         */
        fun selectByPk(pk:DeltaRepo.Item.Pk):Item?

        /**
         * selects the first [limit] records where [DeltaRepo.Item.Metadata.updateStamp] is
         * equal to or after [start] when records are sorted in [order] order.
         * [DeltaRepo.Item.Metadata.syncStatus] == [DeltaRepo.Item.SyncStatus.PULLED]
         */
        fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>

        /**
         * takes care of merging [pulledRemoteItem] into [dirtyLocalItem].
         */
        fun merge(dirtyLocalItem:Item?,pulledRemoteItem:Item):Item

        /**
         * inserts [item] into the repo. replaces any existing record with the same
         * [DeltaRepo.Item.Metadata.pk].
         */
        fun insertOrReplace(item:Item)

        /**
         * deletes all records where [DeltaRepo.Item.Metadata.pk] in [pks].
         */
        fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)

        /**
         * update
         * [DeltaRepo.Item.Metadata.syncStatus] = [DeltaRepo.Item.SyncStatus.PUSHED]
         * where [DeltaRepo.Item.Metadata.syncStatus] == [DeltaRepo.Item.SyncStatus.PULLED]
         */
        fun setAllPulledToPushed()

        /**
         * delete all where
         * [DeltaRepo.Item.Metadata.syncStatus] == [DeltaRepo.Item.SyncStatus.PUSHED].
         */
        fun deleteAllPushed()

        /**
         * true if dirty rows exist; false otherwise.
         */
        fun hasDirtyRows():Boolean
    }

    private fun merge(old:Item?,new:Item):Item
    {
        return when (old?.metadata?.syncStatus)
        {
            null,
            DeltaRepo.Item.SyncStatus.PUSHED,
            DeltaRepo.Item.SyncStatus.PULLED ->
            {
                adapter.merge(old,new).let {it.copy(
                    it.metadata.copy(
                        pk = new.metadata.pk,
                        updateStamp = new.metadata.updateStamp!!,
                        syncStatus = DeltaRepo.Item.SyncStatus.PULLED))
                }
            }
            DeltaRepo.Item.SyncStatus.DIRTY ->
            {
                throw IllegalStateException("no dirty rows in repo allowed when pulling.")
            }
        }
    }

    data class BatchResult(val itemsProcessed:Int,val remoteDeleteCount:Int)

    private fun _pullBatch(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):BatchResult
    {
        val maxUpdateStampItem = adapter.pagePulledByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
        val maxUpdateStamp = maxUpdateStampItem?.metadata?.updateStamp ?: Long.MIN_VALUE
        val (items,deleteCount) = remote.pageByUpdateStamp(maxUpdateStamp,Order.ASC,adapter.BATCH_SIZE)
        val remoteDeleteCount = deleteCount
        val (toDelete,toInsert) = items
            .asSequence()
            // only process the new updates
            .filter {it.metadata.updateStamp!! > maxUpdateStamp}
            // count the number of records deleted on master
            .map {if (it.metadata.isDeleted) adapter.deleteCount++;it}
            // localize updates
            .localized(localRepoInterRepoId,remoteRepoInterRepoId)
            // lookup the existing item...
            .map {adapter.selectByPk(it.metadata.pk) to it}
            // merge...
            .map {(existing,update) -> merge(existing,update)}
            // partition...
            .partition {it.metadata.isDeleted && it.metadata.syncStatus == DeltaRepo.Item.SyncStatus.PULLED}

        // insert items
        toInsert.forEach {adapter.insertOrReplace(it)}

        // delete items and do book keeping
        adapter.deleteByPk(toDelete.map {it.metadata.pk}.toSet())

        return BatchResult(toInsert.size+toDelete.size,remoteDeleteCount)
    }

    fun pullAll(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        while (pullBatch(remote,localRepoInterRepoId,remoteRepoInterRepoId));
    }

    fun pullBatch(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Boolean
    {
        // make sure there are no dirty rows
        check(!adapter.hasDirtyRows()) {"no dirty rows in repo allowed when pulling."}

        // if this is synchronizing from the very beginning, we should adopt the
        // remoteDeleteCount since there is no way we will miss any "delete"
        // updates when synchronizing starting from nothing.
        val shouldAdoptRemoteDeleteCount = adapter.pagePulledByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).isEmpty()

        // pull data
        val (itemsProcessed,remoteDeleteCount) = _pullBatch(remote,localRepoInterRepoId,remoteRepoInterRepoId)

        // set adapter.deleteCount after _pullBatch because _pullBatch modifies it
        if (shouldAdoptRemoteDeleteCount) adapter.deleteCount = remoteDeleteCount

        return when
        {
        // when there are more items to process, return true
            itemsProcessed >= adapter.BATCH_SIZE ->
            {
                true
            }
        // set status of all PULLED items to PUSHED, and sync them one batch at a time.
            adapter.deleteCount != remoteDeleteCount ->
            {
                adapter.setAllPulledToPushed()
                true
            }
        // delete all pushed records (they did not exist on the master)
            adapter.deleteCount == remoteDeleteCount ->
            {
                adapter.deleteAllPushed()
                false
            }
            else -> throw RuntimeException("else branch executed")
        }
    }
}
