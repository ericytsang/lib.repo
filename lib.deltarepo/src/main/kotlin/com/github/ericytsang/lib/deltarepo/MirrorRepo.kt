package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class MirrorRepo(private val adapter:Adapter)
{
    interface Remote
    {
        fun push(items:List<Item>)
        fun pull(isNotDeletedStart:Long,isDeletedStart:Long,limit:Int):Result
        data class Result(val items:List<Item>,val minimumIsDeletedStart:Long):Serializable
    }

    interface Adapter
    {
        /**
         * size of batch to use when doing incremental operations.
         */
        val BATCH_SIZE:Int

        /**
         * selects the first [limit] records where [Item.updateSequence] is
         * equal to or after [start] when records are sorted in [order] order.
         * [Item.syncStatus] == [Item.SyncStatus.PULLED]
         */
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Item.SyncStatus,isDeleted:Boolean?):List<Item>

        fun selectByPk(pk:Item.Pk):Item

        /**
         * inserts [item] into the repo. replaces any existing record with the same
         * [Item.pk].
         */
        fun insertOrReplace(item:Item)

        /**
         * deletes all records where [Item.pk] in [pks].
         */
        fun deleteByPk(pk:Item.Pk)

        fun selectDirtyItemsToPush(limit:Int):List<Item>
        {
            return pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,limit,Item.SyncStatus.DIRTY,null)
        }

        /**
         * update
         * [Item.syncStatus] = [Item.SyncStatus.PUSHED]
         * where [Item.syncStatus] == [Item.SyncStatus.PULLED]
         */
        fun setAllPulledToPushed()
        {
            do
            {
                val items = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,BATCH_SIZE,Item.SyncStatus.PULLED,null)
                items.forEach {insertOrReplace(it._copy(syncStatus = Item.SyncStatus.PUSHED))}
            }
            while (items.size == BATCH_SIZE)
        }

        /**
         * delete all where
         * [Item.syncStatus] == [Item.SyncStatus.PUSHED].
         */
        fun deleteAllPushed()
        {
            do
            {
                val items = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,BATCH_SIZE,Item.SyncStatus.PUSHED,null)
                items.forEach {deleteByPk(it.pk)}
            }
            while (items.size == BATCH_SIZE)
        }
    }

    fun insertOrReplace(item:Item)
    {
        // insert the item with updated metadata
        adapter.insertOrReplace(item._copy(
            updateSequence = computeNextUpdateSequence(),
            syncStatus = Item.SyncStatus.DIRTY))
    }

    fun deleteByPk(pk:Item.Pk)
    {
        // flag the item as deleted
        insertOrReplace(adapter.selectByPk(pk)._copy(
            isDeleted = true))
    }

    fun pullAll(remote:Remote,localRepoInterRepoId:Item.Pk.RepoPk,remoteRepoInterRepoId:Item.Pk.RepoPk)
    {
        while (pullBatch(remote,localRepoInterRepoId,remoteRepoInterRepoId));
    }

    fun pullBatch(remote:Remote,localRepoInterRepoId:Item.Pk.RepoPk,remoteRepoInterRepoId:Item.Pk.RepoPk):Boolean
    {
        // push data
        run {
            val itemsToPush = adapter
                // query for the next items to push
                .selectDirtyItemsToPush(adapter.BATCH_SIZE)
                // localize updates for remote
                .asSequence().localized(remoteRepoInterRepoId,localRepoInterRepoId)
            if (itemsToPush.iterator().hasNext())
            {
                remote.push(itemsToPush.toList())
                return true
            }
        }

        // pull data
        val isNotDeletedStart = adapter
            .pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,Item.SyncStatus.PULLED,false)
            .singleOrNull()?.updateSequence?.plus(1) ?: Long.MIN_VALUE
        val isDeletedStart = adapter
            .pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,Item.SyncStatus.PULLED,true)
            .singleOrNull()?.updateSequence?.plus(1) ?: Long.MIN_VALUE
        val (pulledItems,minimumIsDeletedStart) = remote.pull(isNotDeletedStart,isDeletedStart,adapter.BATCH_SIZE)

        // if we didn't miss any deletes, insert rows into db
        return if (isDeletedStart >= minimumIsDeletedStart
            || (isDeletedStart == Long.MIN_VALUE && isNotDeletedStart == Long.MIN_VALUE))
        {
            // insert rows into db
            pulledItems
                .asSequence()
                // localize updates
                .localized(localRepoInterRepoId,remoteRepoInterRepoId)
                // insert items
                .forEach {
                    if (!it.isDeleted)
                        adapter.insertOrReplace(it)
                    else
                        adapter.deleteByPk(it.pk)
                }

            // if we're done syncing, do this because this might be the end
            // of an "intense sync" where the remaining unsynced rows should
            // be deleted as they were deleted on remote.
            if (pulledItems.size < adapter.BATCH_SIZE)
            {
                adapter.deleteAllPushed()
                false
            }
            else
            {
                true
            }
        }

        // else deletes were missed, prepare for more intense syncing
        else
        {
            adapter.setAllPulledToPushed()
            true
        }
    }

    // helper functions

    private fun computeNextUpdateSequence():Long
    {
        return adapter
            .pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,Item.SyncStatus.DIRTY,null)
            .singleOrNull()?.updateSequence
            ?.let {
                if (it == Long.MAX_VALUE)
                {
                    throw IllegalStateException("used up all sequence numbers")
                }
                else
                {
                    it+1
                }
            }
            ?: Long.MIN_VALUE
    }
}
