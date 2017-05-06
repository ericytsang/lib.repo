package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo
import com.github.ericytsang.lib.repo.SimpleRepo

open class SimpleMasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(protected val adapter:Adapter<ItemPk,Item>):SimpleRepo(),MasterRepo<ItemPk,Item>
{
    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo
    {
        /**
         * size of batch to use when doing incremental operations.
         */
        val BATCH_SIZE:Int

        /**
         * specifies the maximum number of deleted entries to keep so [SimpleMirrorRepo]s
         * can have a chance to sync up. if a [SimpleMirrorRepo] is out of sync for so
         * long so that it fails to sync deletes, it will have to execute a more
         * intensive routine to get back in sync with the master.
         */
        val MAX_DELETED_ITEMS_TO_RETAIN:Long

        /**
         * persistent, mutable integer initialized to 0. used by context to count
         * the number of records that are deleted by [deleteByPk].
         */
        var deleteCount:Int

        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<Item>

        /**
         * inserts all [items] into the repo and replaces any records with
         * conflicting [DeltaRepo.Item.pk].
         */
        fun insertOrReplace(items:Iterable<Item>)

        /**
         * returns the [Item] whose [DeltaRepo.Item.pk] == [pk]; null if not exists.
         */
        fun selectByPk(pk:ItemPk):Item?

        /**
         * returns the next unused update stamp.
         */
        fun computeNextUpdateStamp():Long

        /**
         * takes care of merging updates from mirrors into the master repo.
         */
        fun merge(local:Item?,remote:Item):Item

        /**
         * delete all where [DeltaRepo.Item.pk] in [pks].
         */
        fun deleteByPk(pks:Set<ItemPk>)

        /**
         * returns the next unused primary key.
         */
        fun computeNextPk():ItemPk
    }

    override val pushTarget = object:Pusher.Remote<ItemPk,Item>
    {
        override fun insertOrReplace(items:Iterable<Item>)
        {
            checkCanWrite()
            this@SimpleMasterRepo.insertOrReplace(items
                .map {
                    update ->
                    val existing = adapter.selectByPk(update.pk)
                    val merged = adapter.merge(existing,update)
                    check(merged.pk == update.pk)
                    merged
                })
        }
    }

    override val pullTarget = object:Puller.Remote<ItemPk,Item>
    {
        override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Puller.Remote.Result<ItemPk,Item>
        {
            checkCanRead()
            return Puller.Remote.Result(
                adapter.pageByUpdateStamp(start,order,limit,null),
                adapter.deleteCount)
        }
    }

    /**
     * inserts [items] into the repo. replaces any existing record with
     * conflicting [DeltaRepo.Item.pk].
     *
     * may be used by [SimpleMirrorRepo]s to merge new records into the [SimpleMasterRepo].
     *
     * if [DeltaRepo.Item.isDeleted] == false, then the record is deleted.
     *
     * automatically sets:
     * - [DeltaRepo.Item.syncStatus] = [DeltaRepo.Item.SyncStatus.PULLED]
     * - [DeltaRepo.Item.updateStamp] = [MasterRepoAdapter.computeNextUpdateStamp]
     */
    private fun insertOrReplace(items:Iterable<Item>):HashSet<Item>
    {
        checkCanWrite()
        val (toDelete,toInsert) = items
            .asSequence()
            .map {it.copy(DeltaRepo.Item.Companion,syncStatus = DeltaRepo.Item.SyncStatus.PULLED)}
            .partition {it.isDeleted}
        val _toInsert = toInsert
            .toSet()
            .asSequence()
            .map {it.copy(
                DeltaRepo.Item.Companion,
                updateStamp = adapter.computeNextUpdateStamp())}
            .toList()
        adapter.insertOrReplace(_toInsert)
        deleteByPk(toDelete.map {it.pk}.toSet())
        return (toDelete+_toInsert).toHashSet()
    }

    /**
     * delete all where [DeltaRepo.Item.pk] in [pks].
     */
    private fun deleteByPk(pks:Set<ItemPk>)
    {
        checkCanWrite()
        // sets the isDeleted flag of entries whose pk are in pks
        pks
            .mapNotNull {adapter.selectByPk(it)}
            .asSequence()
            .filter {!it.isDeleted}
            .map {

                // increment delete count
                adapter.deleteCount += 1

                // set deleted flag
                it.copy(
                    DeltaRepo.Item.Companion,
                    updateStamp = adapter.computeNextUpdateStamp(),
                    isDeleted = true)
            }
            .let {adapter.insertOrReplace(it.asIterable())}

        // delete oldest items whose isDeleted flag is set from db until only n
        // items in the db with the isDeleted flag set
        var numDeletedItemsToRetainCount = adapter.MAX_DELETED_ITEMS_TO_RETAIN
        var start = Long.MAX_VALUE
        do
        {
            val items = adapter
                .pageByUpdateStamp(start,Order.DESC,adapter.BATCH_SIZE,true)
                .filter {it.updateStamp != start}
            start = items.lastOrNull()?.updateStamp ?: break

            val numItemsToRetain = Math.min(numDeletedItemsToRetainCount,items.size.toLong()).toInt()
            numDeletedItemsToRetainCount = Math.max(numDeletedItemsToRetainCount-numItemsToRetain,0)
            val itemsToDelete = items.drop(numItemsToRetain).map {it.pk}.toSet()
            if (itemsToDelete.isNotEmpty())
            {
                adapter.deleteByPk(itemsToDelete)
            }
        }
        while (true)
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
