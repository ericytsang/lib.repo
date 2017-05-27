package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class Puller<Item:DeltaRepo.Item<Item>>(private val adapter:Adapter<Item>)
{
    interface Remote<Item:DeltaRepo.Item<Item>>
    {
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Result<Item>

        data class Result<Item:DeltaRepo.Item<Item>>(val items:List<Item>,val deleteCount:Int,val remoteExistingDeletedItems:Int):Serializable
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
        var destructiveDeleteCount:Int

        /**
         * returns the [Item] whose [DeltaRepo.Item.Metadata.pk] == [pk]; null if not exists.
         */
        fun selectByPk(pk:DeltaRepo.Item.Pk):Item?

        /**
         * selects the first [limit] records where [DeltaRepo.Item.Metadata.updateStamp] is
         * equal to or after [start] when records are sorted in [order] order.
         * [DeltaRepo.Item.Metadata.syncStatus] == [DeltaRepo.Item.SyncStatus.PULLED]
         */
        fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<Item>

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

    fun pullAll(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        while (pullBatch(remote,localRepoInterRepoId,remoteRepoInterRepoId));
    }

    fun pullBatch(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Boolean
    {
        return pullBatch1(remote,localRepoInterRepoId,remoteRepoInterRepoId)
            .let {pullBatch2(it)}
            .let {pullBatch3(it)}
    }

    fun pullBatch1(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):PullBatch1Result<Item>
    {
        // make sure there are no dirty rows
        check(!adapter.hasDirtyRows()) {"no dirty rows in repo allowed when pulling."}

        // pull data
        val maxUpdateStampItem = adapter.pagePulledByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,null).singleOrNull()
        val maxUpdateStamp = maxUpdateStampItem?.metadata?.updateStamp?.plus(1) ?: Long.MIN_VALUE
        return PullBatch1Result(remote,localRepoInterRepoId,remoteRepoInterRepoId,maxUpdateStamp)
    }

    fun pullBatch2(prevResult:PullBatch1Result<Item>):PullBatch2Result<Item>
    {
        val (remote,localRepoInterRepoId,remoteRepoInterRepoId,maxUpdateStamp)
            = prevResult
        val (pulledItems,remoteDeleteCount,remoteExistingDeletedItemsCount)
            = remote.pageByUpdateStamp(maxUpdateStamp,Order.ASC,adapter.BATCH_SIZE)
        return PullBatch2Result(localRepoInterRepoId,remoteRepoInterRepoId,
            pulledItems,remoteDeleteCount,remoteExistingDeletedItemsCount)
    }

    fun pullBatch3(prevResult:PullBatch2Result<Item>):Boolean
    {
        val (localRepoInterRepoId,remoteRepoInterRepoId,pulledItems,
            remoteDeleteCount,remoteExistingDeletedItemsCount)
            = prevResult

        // if this is synchronizing from the very beginning, we should adopt the
        // remoteDeleteCount since there is no way we will miss any "delete"
        // updates when synchronizing starting from nothing.
        val shouldAdoptRemoteDeleteCount = adapter.pagePulledByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,null).isEmpty()
        if (shouldAdoptRemoteDeleteCount) adapter.destructiveDeleteCount = remoteDeleteCount

        // process remote items and insert into db
        pulledItems
            .asSequence()
            // localize updates
            .localized(localRepoInterRepoId,remoteRepoInterRepoId)
            // lookup the existing item...
            .map {adapter.selectByPk(it.metadata.pk) to it}
            // merge...
            .map {(existing,update) -> merge(existing,update)}
            // insert items
            .forEach {adapter.insertOrReplace(it)}

        // delete oldest items whose isDeleted flag is set from db until only n
        // items in the db with the isDeleted flag set
        var numDeletedItemsToRetainCount = remoteExistingDeletedItemsCount
        var start = Long.MAX_VALUE
        do
        {
            val deletedItems = adapter
                .pagePulledByUpdateStamp(start,Order.DESC,adapter.BATCH_SIZE,true)
                .filter {it.metadata.updateStamp != start}
            start = deletedItems.lastOrNull()?.metadata?.updateStamp ?: break

            val numItemsToRetain = Math.min(numDeletedItemsToRetainCount,deletedItems.size)
            numDeletedItemsToRetainCount = Math.max(numDeletedItemsToRetainCount-numItemsToRetain,0)
            val itemsToDelete = deletedItems.drop(numItemsToRetain).map {it.metadata.pk}.toSet()
            if (itemsToDelete.isNotEmpty())
            {
                adapter.destructiveDeleteCount += itemsToDelete.size
                adapter.deleteByPk(itemsToDelete)
            }
        }
        while (true)

        return when
        {
        // when there are more items to process, return true
            pulledItems.size >= adapter.BATCH_SIZE ->
            {
                true
            }
        // set status of all PULLED items to PUSHED, and sync them one batch at a time.
            adapter.destructiveDeleteCount != remoteDeleteCount ->
            {
                adapter.setAllPulledToPushed()
                true
            }
        // delete all pushed records (they did not exist on the master)
            adapter.destructiveDeleteCount == remoteDeleteCount ->
            {
                adapter.deleteAllPushed()
                false
            }
            else -> throw RuntimeException("else branch executed")
        }
    }

    data class PullBatch1Result<Item:DeltaRepo.Item<Item>>
    internal constructor(
        internal val remote:Remote<Item>,
        internal val localRepoInterRepoId:DeltaRepo.RepoPk,
        internal val remoteRepoInterRepoId:DeltaRepo.RepoPk,
        internal val maxUpdateStamp:Long)
    data class PullBatch2Result<Item:DeltaRepo.Item<Item>>
    internal constructor(
        internal val localRepoInterRepoId:DeltaRepo.RepoPk,
        internal val remoteRepoInterRepoId:DeltaRepo.RepoPk,
        internal val pulledItems:List<Item>,
        internal val remoteDeleteCount:Int,
        internal val remoteExistingDeletedItemsCount:Int)
}
