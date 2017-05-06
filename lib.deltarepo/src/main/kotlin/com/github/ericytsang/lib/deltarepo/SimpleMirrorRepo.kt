package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo
import com.github.ericytsang.lib.repo.SimpleRepo

open class SimpleMirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(_adapter:Adapter<ItemPk,Item>):SimpleRepo(),MirrorRepo<ItemPk,Item>
{
    interface Adapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo
    {
        val BATCH_SIZE:Int
        var deleteCount:Int
        fun computeNextPk():ItemPk
        fun insertOrReplace(item:Item)
        fun selectDirtyItemsToPush(limit:Int):List<Item>
        fun deleteByPk(pks:Set<ItemPk>)
        fun selectByPk(pk:ItemPk):Item?
        fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
        fun merge(dirtyLocalItem:Item?,pulledRemoteItem:Item):Item
        fun setAllPulledToPushed()
        fun deleteAllPushed()
    }

    private val adapter = object:Adapter<ItemPk,Item> by _adapter
    {
        override val BATCH_SIZE:Int get()
        {
            checkCanRead()
            return _adapter.BATCH_SIZE
        }

        override var deleteCount:Int
            get()
            {
                checkCanRead()
                return _adapter.deleteCount
            }
            set(value)
            {
                checkCanWrite()
                _adapter.deleteCount = value
            }

        override fun computeNextPk():ItemPk
        {
            checkCanWrite()
            return _adapter.computeNextPk()
        }

        override fun insertOrReplace(item:Item)
        {
            checkCanWrite()
            _adapter.insertOrReplace(item)
        }

        override fun selectDirtyItemsToPush(limit:Int):List<Item>
        {
            checkCanRead()
            return _adapter.selectDirtyItemsToPush(limit)
        }

        override fun deleteByPk(pks:Set<ItemPk>)
        {
            checkCanWrite()
            _adapter.deleteByPk(pks)
        }

        override fun selectByPk(pk:ItemPk):Item?
        {
            checkCanRead()
            return _adapter.selectByPk(pk)
        }
    }

    override val pusher = Pusher(object:Pusher.Adapter<ItemPk,Item>
    {
        override val BATCH_SIZE:Int get() = adapter.BATCH_SIZE

        override fun selectDirtyItemsToPush(limit:Int):List<Item>
        {
            return adapter.selectDirtyItemsToPush(limit)
        }

        override fun insertOrReplace(item:Item)
        {
            return adapter.insertOrReplace(item)
        }
    })

    override val puller = Puller(object:Puller.Adapter<ItemPk,Item>
    {
        override val BATCH_SIZE:Int get() = adapter.BATCH_SIZE

        override var deleteCount:Int
            get() = adapter.deleteCount
            set(value) { adapter.deleteCount = value }

        override fun selectByPk(pk:ItemPk):Item?
        {
            return adapter.selectByPk(pk)
        }

        override fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
        {
            return adapter.pagePulledByUpdateStamp(start,order,limit)
        }

        override fun merge(dirtyLocalItem:Item?,pulledRemoteItem:Item):Item
        {
            return adapter.merge(dirtyLocalItem,pulledRemoteItem)
        }

        override fun insertOrReplace(item:Item)
        {
            return adapter.insertOrReplace(item)
        }

        override fun deleteByPk(pks:Set<ItemPk>)
        {
            adapter.deleteByPk(pks)
        }

        override fun setAllPulledToPushed()
        {
            adapter.setAllPulledToPushed()
        }

        override fun deleteAllPushed()
        {
            adapter.deleteAllPushed()
        }

        override fun hasDirtyRows():Boolean
        {
            return adapter.selectDirtyItemsToPush(1).isNotEmpty()
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
    fun insertOrReplace(items:Iterable<Item>):Set<Item>
    {
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
    fun deleteByPk(pks:Set<ItemPk>)
    {
        val items = pks
            .mapNotNull {adapter.selectByPk(it)}
            .filter {!it.isDeleted}
            .map {it.copy(DeltaRepo.Item.Companion,isDeleted = true)}
        insertOrReplace(items)
    }

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
    {
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
