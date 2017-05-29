package com.github.ericytsang.lib.deltarepo

open class SimpleMirrorRepo<Item:DeltaRepo.Item<Item>>(private val adapter:Adapter<Item>):MirrorRepo<Item>
{
    interface Adapter<Item:DeltaRepo.Item<Item>>
    {
        val BATCH_SIZE:Int
        var destructiveDeleteCount:Int
        var nextId:Long
        fun insertOrReplace(item:Item)
        fun selectDirtyItemsToPush(limit:Int):List<Item>
        fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
        fun selectByPk(pk:DeltaRepo.Item.Pk):Item?
        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,pulledOnly:Boolean,isDeleted:Boolean?):List<Item>
        fun merge(dirtyLocalItem:Item?,pulledRemoteItem:Item):Item
        fun setAllPulledToPushed()
        fun deleteAllPushed()

        /**
         * number of rows in the database where
         * [DeltaRepo.Item.Metadata.isDeleted] == true.
         */
        val rowsWhereIsDeletedCount:Int
    }

    override val pusher = Pusher(object:Pusher.Adapter<Item>
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

    override val puller = Puller(object:Puller.Adapter<Item>
    {
        override val BATCH_SIZE:Int get() = adapter.BATCH_SIZE

        override var destructiveDeleteCount:Int
            get() = adapter.destructiveDeleteCount
            set(value) { adapter.destructiveDeleteCount = value }

        override fun selectByPk(pk:DeltaRepo.Item.Pk):Item?
        {
            return adapter.selectByPk(pk)
        }

        override fun pagePulledByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<Item>
        {
            return adapter.pageByUpdateStamp(start,order,limit,true,isDeleted)
        }

        override fun merge(dirtyLocalItem:Item?,pulledRemoteItem:Item):Item
        {
            return adapter.merge(dirtyLocalItem,pulledRemoteItem)
        }

        override fun insertOrReplace(item:Item)
        {
            return adapter.insertOrReplace(item)
        }

        override fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
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

    override val pullTarget = object:Puller.Remote<Item>
    {
        override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):Puller.Remote.Result<Item>
        {
            return Puller.Remote.Result(
                adapter.pageByUpdateStamp(start,order,limit,false,null),
                adapter.destructiveDeleteCount,
                adapter.rowsWhereIsDeletedCount)
        }
    }

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
                val existing = adapter.selectByPk(it.metadata.pk)
                val updateStamp = adapter.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,false,null).singleOrNull()?.metadata?.updateStamp?.plus(1) ?: Long.MIN_VALUE
                it.copy(
                    syncStatus = DeltaRepo.Item.SyncStatus.DIRTY,
                    updateStamp = updateStamp);//existing?.metadata?.updateStamp)
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
    fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
    {
        val items = pks
            .mapNotNull {adapter.selectByPk(it)}
            .filter {!it.metadata.isDeleted}
            .map {it.copy(isDeleted = true)}
        insertOrReplace(items)
    }

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():DeltaRepo.Item.Pk
    {
        return DeltaRepo.Item.Pk(DeltaRepo.LOCAL_NODE_ID,DeltaRepo.ItemPk(adapter.nextId++))
    }
}
