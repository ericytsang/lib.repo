package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo
import com.github.ericytsang.lib.repo.SimpleRepo

open class SimpleMasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:Adapter<ItemPk,Item>):SimpleRepo(),MasterRepo<ItemPk,Item>
{
    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo,Pusher.Remote<ItemPk,Item>,Puller.Remote<ItemPk,Item>
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

        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>,isDeleted:Boolean?):List<Item>

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
        fun merge(local:Item,remote:Item):Item

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
        override fun insertOrReplace(items:Iterable<Item>):HashSet<Item>
        {
            checkCanWrite()
            return adapter.insertOrReplace(items)
        }
    }

    override val pullTarget = object:Puller.Remote<ItemPk,Item>
    {
        override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Puller.Remote.Result<ItemPk,Item>
        {
            checkCanRead()
            return adapter.pageByUpdateStamp(start,order,limit)
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
    override fun insertOrReplace(items:Iterable<Item>):Set<Item>
    {
        checkCanWrite()
        val (toDelete,toInsert) = items
            .asSequence()
            .map {it.copy(Unit,syncStatus = DeltaRepo.Item.SyncStatus.PULLED)}
            .map {
                update ->
                val existing = adapter.selectByPk(update.pk)
                if (existing != null) adapter.merge(existing,update) else update
            }
            .map {check(it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED);it}
            .partition {it.isDeleted}
        val _toInsert = toInsert
            .toSet()
            .asSequence()
            .map {it.copy(
                Unit,
                updateStamp = adapter.computeNextUpdateStamp())}
            .toList()
        adapter.insertOrReplace(_toInsert)
        deleteByPk(toDelete.map {it.pk}.toSet())
        return (toDelete+_toInsert).toHashSet()
    }

    /**
     * delete all where [DeltaRepo.Item.pk] in [pks].
     */
    override fun deleteByPk(pks:Set<ItemPk>)
    {
        checkCanWrite()
        // sets the isDeleted flag of entries whose pk are in pks
        val recordsToDelete = pks
            .mapNotNull {adapter.selectByPk(it)}
            .asSequence()
        recordsToDelete
            .map {it.copy(
                Unit,
                updateStamp = adapter.computeNextUpdateStamp(),
                isDeleted = true)}
            .let {adapter.insertOrReplace(it.asIterable())}

        // delete oldest items whose isDeleted flag is set from db until only n
        // items in the db with the isDeleted flag set
        var numDeletedItemsToRetainCount = adapter.MAX_DELETED_ITEMS_TO_RETAIN
        var start = Long.MAX_VALUE
        do
        {
            val items = adapter
                .pageByUpdateStamp(start,Order.DESC,adapter.BATCH_SIZE,setOf(DeltaRepo.Item.SyncStatus.PULLED),true)
                .filter {it.updateStamp != start}
            start = items.lastOrNull()?.updateStamp ?: break

            val numItemsToRetain = Math.min(numDeletedItemsToRetainCount,items.size.toLong()).toInt()
            numDeletedItemsToRetainCount = Math.max(numDeletedItemsToRetainCount-numItemsToRetain,0)
            val itemsToDelete = items.drop(numItemsToRetain).map {it.pk}.toSet()
            if (itemsToDelete.isNotEmpty())
            {
                adapter.deleteByPk(itemsToDelete)
                adapter.deleteCount += itemsToDelete.size
            }
        }
        while (true)
    }

    /**
     * returns the next unused primary key.
     */
    override fun computeNextPk():ItemPk
    {
        checkCanWrite()
        return adapter.computeNextPk()
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
