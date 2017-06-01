package com.github.ericytsang.lib.deltarepo

class MasterRepo(private val adapter:Adapter):MirrorRepo.Remote
{
    interface Adapter
    {
        val BATCH_SIZE:Int
        val MAX_DELETED_ROWS_TO_RETAIN:Int

        /**
         * should be initialized as [Long.MIN_VALUE] at database creation.
         */
        var minimumIsDeletedStart:Long

        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<Item>

        fun countDeletedRowsEqOrGtUpdateSequence(updateSequence:Long):Int
        {
            var start = updateSequence
            var count = 0
            do
            {
                val items = pageByUpdateStamp(start,Order.ASC,BATCH_SIZE,true)
                start = items.lastOrNull()?.updateSequence?.plus(1) ?: break
                count += items.size
            }
            while (true)
            return count
        }

        fun selectByPk(pk:Item.Pk):Item?

        /**
         * inserts [item] into the repo. replaces any existing record with the same
         * [DeltaRepo.Item.Metadata.pk].
         */
        fun insertOrReplace(item:Item)

        /**
         * deletes all records where [DeltaRepo.Item.Metadata.pk] in [pks].
         */
        fun deleteByPk(pk:Item.Pk)
    }

    fun insertOrReplace(item:Item)
    {
        // insert the item with updated metadata
        adapter.insertOrReplace(item._copy(
            updateSequence = computeNextUpdateSequence(),
            syncStatus = Item.SyncStatus.PULLED))

        // physically delete the oldest items that are flagged as delete until
        // there are only MAX_DELETED_ROWS_TO_RETAIN items flagged as deleted
        var itemsToPhysicallyDelete = adapter.MAX_DELETED_ROWS_TO_RETAIN-adapter.countDeletedRowsEqOrGtUpdateSequence(Long.MIN_VALUE)
        while (itemsToPhysicallyDelete > 0)
        {
            val selectionLimit = Math.min(adapter.BATCH_SIZE,itemsToPhysicallyDelete)
            itemsToPhysicallyDelete -= selectionLimit
            val toDelete = adapter.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,selectionLimit,true)
            adapter.minimumIsDeletedStart = toDelete.last().updateSequence+1
            toDelete.forEach {adapter.deleteByPk(it.pk)}
        }
    }

    fun deleteByPk(pk:Item.Pk)
    {
        // flag the item as deleted
        val existing = adapter.selectByPk(pk) ?: return
        insertOrReplace(existing._copy(
            isDeleted = true))
    }

    override fun push(items:List<Item>)
    {
        items.forEach {insertOrReplace(it)}
    }

    /**
     * pulls data from the data [adapter].
     *
     * @param isNotDeletedStart is the maximum [Item.updateSequence] where [Item.isDeleted] == false.
     * @param isDeletedStart is the maximum [Item.updateSequence] where [Item.isDeleted] == true.
     * @param limit is the maximum number of items to return from the data store.
     */
    override fun pull(isNotDeletedStart:Long,isDeletedStart:Long,limit:Int):MirrorRepo.Remote.Result
    {
        require(limit >= 0)
        val deletedItems = adapter.pageByUpdateStamp(isDeletedStart,Order.ASC,limit,true)
        check(deletedItems.size <= limit)

        val notDeletedItemsLimit = limit-deletedItems.size
        val notDeletedItems = adapter.pageByUpdateStamp(isNotDeletedStart,Order.ASC,notDeletedItemsLimit,false)
        check(notDeletedItems.size <= notDeletedItemsLimit)

        val items = deletedItems+notDeletedItems
        return MirrorRepo.Remote.Result(items,adapter.minimumIsDeletedStart)
    }

    // helper functions

    private fun computeNextUpdateSequence():Long
    {
        return adapter
            .pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,null)
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
