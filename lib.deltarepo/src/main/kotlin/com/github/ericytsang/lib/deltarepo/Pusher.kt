package com.github.ericytsang.lib.deltarepo

class Pusher<Item:Any>(private val adapter:Pusher.Adapter<Item>)
{
    interface Remote<in Item:Any>
    {
        /**
         * takes care of merging [items] into the local database (the database that
         * is being inserted into).
         */
        fun insertOrReplace(items:Iterable<Item>)
    }

    interface Adapter<Item:Any>
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

    fun push(remote:Remote<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        // push updates
        do
        {
            val toPush = adapter.selectDirtyItemsToPush(adapter.BATCH_SIZE)
                .map {it.copy(
                    syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)}
            remote.insertOrReplace(toPush
                .asSequence()
                .localized(itemAdapter,remoteRepoInterRepoId,localRepoInterRepoId)
                .toList())
            toPush
                .forEach {adapter.insertOrReplace(it)}
        }
        while (toPush.isNotEmpty())
    }
}
