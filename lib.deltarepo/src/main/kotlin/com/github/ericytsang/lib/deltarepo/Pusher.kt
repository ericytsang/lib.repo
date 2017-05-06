package com.github.ericytsang.lib.deltarepo

class Pusher<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:Pusher.Adapter<ItemPk,Item>)
{
    interface Remote<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
    {
        /**
         * takes care of merging [items] into the local database (the database that
         * is being inserted into).
         */
        fun insertOrReplace(items:Iterable<Item>)
    }

    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
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

    fun push(remote:Remote<ItemPk,Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk)
    {
        // push updates
        do
        {
            val toPush = adapter.selectDirtyItemsToPush(adapter.BATCH_SIZE)
                .map {it.copy(DeltaRepo.Item.Companion,syncStatus = DeltaRepo.Item.SyncStatus.PUSHED)}
            remote.insertOrReplace(toPush
                .asSequence()
                .localized(remoteRepoInterRepoId,localRepoInterRepoId)
                .toList())
            toPush
                .forEach {adapter.insertOrReplace(it)}
        }
        while (toPush.isNotEmpty())
    }
}
