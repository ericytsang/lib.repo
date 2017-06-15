package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

class MirrorRepo<E:Item<E>>(private val adapter:Adapter<E>)
{
    interface Remote<E:Item<E>>
    {
        fun pull(isNotDeletedStart:Long,isDeletedStart:Long,limit:Int):Result<E>
        data class Result<E:Item<E>>(val items:List<E>,val minimumIsDeletedStart:Long):Serializable
    }

    interface Adapter<E:Item<E>>
    {
        /**
         * size of batch to use when doing incremental operations.
         */
        val BATCH_SIZE:Int

        /**
         * should be initialized to [Long.MIN_VALUE].
         */
        var latestIsDeletedUpdateSequence:Long

        /**
         * should be initialized to [Long.MIN_VALUE].
         */
        var latestIsNotDeletedUpdateSequence:Long

        /**
         * inserts [items] into the repo.
         */
        fun merge(items:List<E>)

        /**
         * invoked when it is detected that repo has missed some delete updates
         * from the master repo and must perform a total re-sync to recover.
         * entries should be flagged as un-synced...multiple calls to [merge]
         * shall follow calls to [prepareForResync] ending with a call to
         * [completeResync].
         */
        fun prepareForResync()

        /**
         * invoked upon completing a total re-sync. the first call to this
         * method that takes place after a call to [prepareForResync] should
         * take into account that all items that have not been updated or
         * created in a call to [merge] since the last call to
         * [prepareForResync] has been deleted from the remote repo.
         */
        fun completeResync()
    }

    fun forceResync()
    {
        adapter.latestIsDeletedUpdateSequence = Long.MIN_VALUE
        adapter.latestIsNotDeletedUpdateSequence = Long.MIN_VALUE
        adapter.prepareForResync()
    }

    fun pullAll(remote:Remote<E>)
    {
        while (pullBatch(remote));
    }

    fun pullBatch(remote:Remote<E>):Boolean
    {
        return pullBatchPhase3(pullBatchPhase2(pullBatchPhase1(remote)))
    }

    fun pullBatchPhase1(remote:Remote<E>):PullBatchPhase1Result<E>
    {
        val isNotDeletedStart = adapter.latestIsNotDeletedUpdateSequence
        val isDeletedStart = adapter.latestIsDeletedUpdateSequence
        return PullBatchPhase1Result(remote,isNotDeletedStart,isDeletedStart)
    }

    data class PullBatchPhase1Result<E:Item<E>>(
        val remote:Remote<E>,
        val isNotDeletedStart:Long,
        val isDeletedStart:Long)

    fun pullBatchPhase2(result:PullBatchPhase1Result<E>):PullBatchPhase2Result<E>
    {
        val (remote,isNotDeletedStart,isDeletedStart) = result
        val (pulledItems,minimumIsDeletedStart) = remote.pull(isNotDeletedStart,isDeletedStart,adapter.BATCH_SIZE)
        return PullBatchPhase2Result(isNotDeletedStart,isDeletedStart,pulledItems,minimumIsDeletedStart)
    }

    data class PullBatchPhase2Result<E:Item<E>>(
        val isNotDeletedStart:Long,
        val isDeletedStart:Long,
        val pulledItems:List<E>,
        val minimumIsDeletedStart:Long)

    fun pullBatchPhase3(result:PullBatchPhase2Result<E>):Boolean
    {
        val (isNotDeletedStart,isDeletedStart,pulledItems,minimumIsDeletedStart) = result

        // if we didn't miss any deletes, insert rows into db
        return if (adapter.latestIsDeletedUpdateSequence >= minimumIsDeletedStart
            || (isDeletedStart == Long.MIN_VALUE && isNotDeletedStart == Long.MIN_VALUE))
        {
            // update db from pulled items
            adapter.merge(pulledItems)

            adapter.latestIsNotDeletedUpdateSequence = pulledItems
                .asSequence()
                .filter {!it.isDeleted}
                .lastOrNull()
                ?.updateSequence
                ?.plus(1)
                ?:adapter.latestIsNotDeletedUpdateSequence
            adapter.latestIsDeletedUpdateSequence = pulledItems
                .asSequence()
                .filter {it.isDeleted}
                .lastOrNull()
                ?.updateSequence
                ?.plus(1)
                ?:
                run {
                    // if this is the first pull (repo is empty or re-syncing) adopt remote's minimum delete start
                    if (isDeletedStart == Long.MIN_VALUE && isNotDeletedStart == Long.MIN_VALUE)
                    {
                        minimumIsDeletedStart
                    }
                    // don't update latest pulled delete stamp otherwise.
                    else
                    {
                        adapter.latestIsDeletedUpdateSequence
                    }
                }

            // if we're done syncing, do this because this might be the end
            // of an "intense sync" where the remaining unsynced rows should
            // be deleted as they were deleted on remote.
            if (pulledItems.size < adapter.BATCH_SIZE)
            {
                adapter.completeResync()
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
            forceResync()
            true
        }
    }
}
