package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class Puller<Item:Any>(private val adapter:Adapter<Item>)
{
    interface Remote<out Item:Any>
    {
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Result<Item>

        data class Result<out Item>(val items:List<Item>,val deleteCount:Int):Serializable
    }

    interface Adapter<Item:Any>
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

        val Item.metadata:DeltaRepo.Item.Metadata

        fun Item.copy(newMetadata:DeltaRepo.Item.Metadata):Item
    }

    private val itemAdapter = object:ItemAdapter<Item>
    {
        override val Item.metadata:DeltaRepo.Item.Metadata get()
        {
            return with(adapter) {metadata}
        }

        override fun Item.copy(newMetadata:DeltaRepo.Item.Metadata):Item
        {
            return with(adapter) {copy(newMetadata)}
        }
    }

    val Item.metadata:DeltaRepo.Item.Metadata get()
    {
        return with(adapter) {metadata}
    }

    fun Item.copy(
        pk:DeltaRepo.Item.Pk = metadata.pk,
        updateStamp:Long? = metadata.updateStamp,
        syncStatus:DeltaRepo.Item.SyncStatus = metadata.syncStatus,
        isDeleted:Boolean = metadata.isDeleted)
        :Item
    {
        return with(adapter) {copy(DeltaRepo.Item.Metadata(pk,updateStamp,syncStatus,isDeleted))}
    }

    private fun merge(old:Item?,new:Item):Item
    {
        return when (old?.metadata?.syncStatus)
        {
            null,
            DeltaRepo.Item.SyncStatus.PUSHED,
            DeltaRepo.Item.SyncStatus.PULLED ->
            {
                adapter.merge(old,new).copy(
                    pk = new.metadata.pk,
                    updateStamp = new.metadata.updateStamp!!,
                    syncStatus = DeltaRepo.Item.SyncStatus.PULLED)
            }
            DeltaRepo.Item.SyncStatus.DIRTY ->
            {
                throw IllegalStateException("no dirty rows in repo allowed when pulling.")
            }
        }
    }

    private fun _pull(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Int
    {
        var remoteDeleteCount:Int

        // pull updates
        do
        {
            val maxUpdateStampItem = adapter.pagePulledByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
            val maxUpdateStamp = maxUpdateStampItem?.metadata?.updateStamp ?: Long.MIN_VALUE
            val (items,deleteCount) = remote.pageByUpdateStamp(maxUpdateStamp,Order.ASC,adapter.BATCH_SIZE)
            remoteDeleteCount = deleteCount
            val (toDelete,toInsert) = items
                .asSequence()
                // only process the new updates
                .filter {it.metadata.updateStamp!! > maxUpdateStamp}
                // count the number of records deleted on master
                .map {if (it.metadata.isDeleted) adapter.deleteCount++;it}
                // localize updates
                .localized(itemAdapter,localRepoInterRepoId,remoteRepoInterRepoId)
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
        }
        while (toInsert.size+toDelete.size >= adapter.BATCH_SIZE)

        return remoteDeleteCount
    }

    fun pull(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        // make sure there are no dirty rows
        check(!adapter.hasDirtyRows()) {"no dirty rows in repo allowed when pulling."}

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
