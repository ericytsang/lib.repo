package com.github.ericytsang.lib.deltarepo

class MasterRepo<E:Item<E>>(private val adapter:Adapter<E>):MirrorRepo.Remote<E>
{
    interface Adapter<E:Item<E>>
    {
        val BATCH_SIZE:Int
        val MAX_DELETED_ROWS_TO_RETAIN:Int

        /**
         * should be initialized as [Long.MIN_VALUE] at database creation. this
         * variable keeps track of the latest destructively deleted row.
         */
        var minimumIsDeletedStart:Long

        fun pageByUpdateStamp(start:Long,order:Order,limit:Int,isDeleted:Boolean?):List<E>

        /**
         * selects by [Item.updateSequence] the rows that are candidates to be
         * destructively deleted with a call to [delete]. useful for repos that
         * may have delete updates that need to be pushed up to a server.
         */
        fun pageRowsToDeleteByUpdateStamp(start:Long,order:Order,limit:Int):List<E>
        {
            return pageByUpdateStamp(start,order,limit,true)
        }

        /**
         * selects the record from persistent memory that is logically equivalent
         * to [item]; null if not exists.
         */
        fun select(item:E):E?

        /**
         * inserts [item] into persistent memory. replaces any existing record
         * if it represents the same item.
         */
        fun insertOrReplace(item:E)

        /**
         * deletes [item] from persistent memory.
         */
        fun delete(item:E)

        fun countDeletedRowsEqOrGtUpdateSequence(updateSequence:Long):Int
        {
            var start = updateSequence
            var count = 0
            do
            {
                val items = pageRowsToDeleteByUpdateStamp(start,Order.ASC,BATCH_SIZE)
                start = items.lastOrNull()?.updateSequence?.plus(1) ?: break
                count += items.size
            }
            while (true)
            return count
        }
    }

    fun insertOrReplace(item:E)
    {
        // insert the item with updated metadata
        adapter.insertOrReplace(item._copy(
            updateSequence = computeNextUpdateSequence()))

        // physically delete the oldest items that are flagged as delete until
        // there are only MAX_DELETED_ROWS_TO_RETAIN items flagged as deleted
        var itemsToPhysicallyDelete = adapter.countDeletedRowsEqOrGtUpdateSequence(Long.MIN_VALUE)-adapter.MAX_DELETED_ROWS_TO_RETAIN
        while (itemsToPhysicallyDelete > 0)
        {
            val selectionLimit = Math.min(adapter.BATCH_SIZE,itemsToPhysicallyDelete)
            itemsToPhysicallyDelete -= selectionLimit
            val toDelete = adapter.pageRowsToDeleteByUpdateStamp(Long.MIN_VALUE,Order.ASC,selectionLimit)
            adapter.minimumIsDeletedStart = toDelete.last().updateSequence+1
            toDelete.forEach {adapter.delete(it)}
        }
    }

    fun delete(item:E)
    {
        // flag the item as deleted
        val existing = adapter.select(item) ?: return
        insertOrReplace(existing._copy(
            isDeleted = true))
    }

    /**
     * pulls data from the data [adapter].
     *
     * @param isNotDeletedStart is the maximum [Item.updateSequence] where [Item.isDeleted] == false.
     * @param isDeletedStart is the maximum [Item.updateSequence] where [Item.isDeleted] == true.
     * @param limit is the maximum number of items to return from the data store.
     */
    override fun pull(isNotDeletedStart:Long,isDeletedStart:Long,limit:Int):MirrorRepo.Remote.Result<E>
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
