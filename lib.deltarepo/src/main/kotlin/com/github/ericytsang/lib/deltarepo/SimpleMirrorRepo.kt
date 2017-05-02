package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo
import com.github.ericytsang.lib.repo.SimpleRepo

open class SimpleMirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(protected val adapter:Adapter<ItemPk,Item>):SimpleRepo(),MirrorRepo<ItemPk,Item>
{
    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo,Pusher.Adapter<ItemPk,Item>,Puller.Adapter<ItemPk,Item>
    {
        /**
         * returns the next unused primary key.
         */
        fun computeNextPk():ItemPk
    }

    override val pusher = Pusher(object:Pusher.Adapter<ItemPk,Item> by adapter
    {
        override fun selectNextUnsyncedToSync(limit:Int):List<Item>
        {
            checkCanRead()
            return adapter.selectNextUnsyncedToSync(limit)
        }

        override fun insertOrReplace(item:Item)
        {
            checkCanWrite()
            return adapter.insertOrReplace(item)
        }
    })

    override val puller = Puller(object:Puller.Adapter<ItemPk,Item> by adapter
    {
        override var deleteCount:Int
            get()
            {
                checkCanRead()
                return adapter.deleteCount
            }
            set(value)
            {
                checkCanWrite()
                adapter.deleteCount = value
            }

        override fun selectByPk(pk:ItemPk):Item?
        {
            checkCanRead()
            return adapter.selectByPk(pk)
        }

        override fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
        {
            checkCanRead()
            return adapter.pagePulledByUpdateStamp(start,order,limit)
        }

        override fun insertOrReplace(item:Item)
        {
            checkCanWrite()
            return adapter.insertOrReplace(item)
        }

        override fun deleteByPk(pks:Set<ItemPk>)
        {
            checkCanWrite()
            return adapter.deleteByPk(pks)
        }

        override fun setAllPulledToPushed()
        {
            checkCanWrite()
            return adapter.setAllPulledToPushed()
        }
    })

    /**
     * inserts [items] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk].
     *
     * automatically sets:
     * - [DeltaRepo.Item.syncStatus] = [DeltaRepo.Item.SyncStatus.DIRTY]
     * - if not previously exists [DeltaRepo.Item.updateStamp] = null
     * - if previously exists [DeltaRepo.Item.updateStamp] = previousRecord's
     *   [DeltaRepo.Item.updateStamp]
     */
    override fun insertOrReplace(items:Iterable<Item>):Set<Item>
    {
        checkCanWrite()
        val itemsToInsert = items
            .asSequence()
            .map {
                val existing = adapter.selectByPk(it.pk)
                it.copy(
                    DeltaRepo.Item.Companion,
                    syncStatus = DeltaRepo.Item.SyncStatus.DIRTY,
                    updateStamp = existing?.updateStamp)
            }
        itemsToInsert.forEach {adapter.insertOrReplace(it)}
        return itemsToInsert.toSet()
    }
    /**
     * convenience method for [insertOrReplace].
     *
     * sets:
     * - [DeltaRepo.Item.isDeleted] = true
     * where [DeltaRepo.Item.pk] in [pks].
     */
    override fun deleteByPk(pks:Set<ItemPk>)
    {
        checkCanWrite()
        val items = pks
            .mapNotNull {adapter.selectByPk(it)}
            .filter {!it.isDeleted}
            .map {it.copy(DeltaRepo.Item.Companion,isDeleted = true)}
        insertOrReplace(items)
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
