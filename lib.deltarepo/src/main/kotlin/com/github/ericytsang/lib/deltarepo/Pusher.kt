package com.github.ericytsang.lib.deltarepo

class Pusher<Item:DeltaRepo.Item<Item>>(private val adapter:Pusher.Adapter<Item>)
{
    interface Remote<Item:DeltaRepo.Item<Item>>
    {
        /**
         * takes care of merging [items] into the local database (the database that
         * is being inserted into).
         */
        fun insertOrReplace(items:Iterable<Item>)
    }

    interface Adapter<Item:DeltaRepo.Item<Item>>
    {
        /**
         * size of batch to use when doing incremental operations.
         */
        val BATCH_SIZE:Int

        /**
         * selects the next [limit] [DeltaRepo.Item]s to sync to the [SimpleMasterRepo].
         * [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.DIRTY]
         */
        fun selectDirtyItemsToPush(limit:Int):List<Item>

        /**
         * inserts [item] into the repo. replaces any existing record with the same
         * [DeltaRepo.Item.pk].
         */
        fun insertOrReplace(item:Item)
    }

    fun pushAll(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        while (pushBatch(remote,localRepoInterRepoId,remoteRepoInterRepoId));
    }

    fun pushBatch(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Boolean
    {
        return pushBatch1(remote,localRepoInterRepoId,remoteRepoInterRepoId)
            .let {pushBatch2(it)}
            .let {pushBatch3(it)}
    }

    fun pushBatch1(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):PushBatch1Result<Item>
    {
        val toPush = adapter.selectDirtyItemsToPush(adapter.BATCH_SIZE)
            .map {it.copy(syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)}
        return PushBatch1Result(remote,localRepoInterRepoId,remoteRepoInterRepoId,toPush)
    }

    fun pushBatch2(prevResult:PushBatch1Result<Item>):PushBatch2Result<Item>
    {
        val (remote,localRepoInterRepoId,remoteRepoInterRepoId,toPush) = prevResult
        remote.insertOrReplace(toPush
            .asSequence()
            .localized(remoteRepoInterRepoId,localRepoInterRepoId)
            .toList())
        return PushBatch2Result(toPush)
    }

    fun pushBatch3(prevResult:PushBatch2Result<Item>):Boolean
    {
        val toPush = prevResult.toPush
        toPush
            .forEach {adapter.insertOrReplace(it)}
        return toPush.isNotEmpty()
    }

    data class PushBatch1Result<Item:DeltaRepo.Item<Item>>
    internal constructor(
        val remote:Remote<Item>,
        val localRepoInterRepoId:DeltaRepo.RepoPk,
        val remoteRepoInterRepoId:DeltaRepo.RepoPk,
        val toPush:List<Item>)
    data class PushBatch2Result<Item:DeltaRepo.Item<Item>>
    internal constructor(
        val toPush:List<Item>)
}
