package com.github.ericytsang.lib.deltarepo

class SimpleMasterRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:MasterRepoAdapter<ItemPk,Item>):BaseRepo(),MutableMasterRepo<ItemPk,Item>
{
    override fun merge(items:Set<Item>)
    {
        checkCanWrite()
        val (toDelete,toInsert) = items.partition {it.isDeleted}
        deleteByPk(toDelete.map {it.pk}.toSet())
        toInsert.forEach {insertOrReplace(it)}
    }

    override fun insertOrReplace(item:Item)
    {
        checkCanWrite()
        require(!item.isDeleted)
        val _item = item
            .copy(
                updateStamp = adapter.computeNextUpdateStamp(),
                isSynced = true)
            .let()
            {
                if (it.deleteStamp == null)
                {
                    it.copy(deleteStamp = adapter.computeNextDeleteStamp())
                }
                else
                {
                    it
                }
            }
        adapter.insertOrReplace(_item)
    }

    override fun deleteByPk(pks:Set<ItemPk>)
    {
        checkCanWrite()
        val recordsToDelete = pks
            .mapNotNull {adapter.selectByPk(it)}
            .asSequence()
        val maxDeleteStampToDelete = recordsToDelete
            .map {it.deleteStamp ?: throw RuntimeException("master repo should not have null for this field")}
            .max() ?: return
        var pageStart = maxDeleteStampToDelete

        // update all where deleteStamp < maxDeleteStamp AND pk NOT IN pks
        do
        {
            // query for records and prepare for next query
            val records = pageByDeleteStamp(pageStart,Order.DESC,DeltaRepo.BATCH_SIZE)
                .filter {it.deleteStamp != pageStart}
            records.mapNotNull {it.deleteStamp}.min()?.let {pageStart = it}

            // update all where deleteStamp < maxDeleteStamp AND pk NOT IN pks
            records.filter {it.pk !in pks}.forEach {
                adapter.insertOrReplace(it.copy(deleteStamp = adapter.computeNextDeleteStamp()))
            }
        }
        while(records.isNotEmpty())

        // delete records...
        val minDeleteStampToKeep = pageByDeleteStamp(maxDeleteStampToDelete,Order.ASC,2)
            .drop(1).singleOrNull()?.deleteStamp ?: Long.MAX_VALUE
        adapter.delete(minDeleteStampToKeep)
    }

    override fun computeNextPk():ItemPk
    {
        checkCanWrite()
        return adapter.computeNextPk().apply {check(this.nodePk.id == this@SimpleMasterRepo.pk.id)}
    }

    override val pk:DeltaRepo.Pk get()
    {
        checkCanRead()
        return adapter.pk
    }

    override fun selectByPk(pk:ItemPk):Item?
    {
        checkCanRead()
        return adapter.selectByPk(pk)
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.pageByUpdateStamp(start,order,limit)
    }

    override fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.pageByDeleteStamp(start,order,limit)
    }
}
