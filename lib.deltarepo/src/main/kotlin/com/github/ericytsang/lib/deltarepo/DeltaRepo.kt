package com.github.ericytsang.lib.deltarepo

class SimpleMirrorRepo(private val adapter:MirrorRepoAdapter):MutableMirrorRepo,MirrorRepo by adapter
{
    override fun synchronizeWith(source:DeltaRepo)
    {
        // synchronize updates
        run {
            do
            {
                val maxUpdateStampItem:DeltaRepo.Item? = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
                val maxUpdateStamp = maxUpdateStampItem?.updateStamp ?: Long.MIN_VALUE
                val newUpdates = pageByUpdateStamp(maxUpdateStamp,Order.ASC,DeltaRepo.BATCH_SIZE).filter {it.pk == maxUpdateStampItem?.pk}
                newUpdates.forEach {insertOrReplace(it)}
            }
            while (newUpdates.isNotEmpty())
        }

        // synchronize delete stamps
        run {
            do
            {
                val maxDeleteStampItem:DeltaRepo.Item? = pageByDeleteStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
                val maxDeleteStamp = maxDeleteStampItem?.deleteStamp ?: Long.MIN_VALUE
                val newUpdates = pageByDeleteStamp(maxDeleteStamp,Order.ASC,DeltaRepo.BATCH_SIZE).filter {it.pk == maxDeleteStampItem?.pk}
                newUpdates.forEach {
                    adapter.insertOrReplace(it)
                }
            }
            while (newUpdates.isNotEmpty())
        }

        // delete items whose delete stamps are less than source's minimum
        run {
            val minDeleteStampItem:DeltaRepo.Item? = source.pageByDeleteStamp(Long.MIN_VALUE,Order.ASC,1).singleOrNull()
            val minDeleteStamp = minDeleteStampItem?.deleteStamp ?: Long.MAX_VALUE
            adapter.delete(minDeleteStamp)
        }
    }

    override fun insertOrReplace(item:DeltaRepo.Item)
    {
        adapter.insertOrReplace(item.copy(isSynced = false))
    }

    override fun deleteByPk(pk:DeltaRepo.Item.Pk)
    {
        val item = selectByPk(pk) ?: return
        adapter.insertOrReplace(item.copy(isDeleted = true))
    }
}

class SimpleMasterRepo(private val adapter:MasterRepoAdapter):MutableMasterRepo,MasterRepo by adapter
{
    override fun merge(items:Set<DeltaRepo.Item>)
    {
        val (toDelete,toInsert) = items.partition {it.isDeleted}
        deleteByPk(toDelete.map {it.pk}.toSet())
        toInsert.forEach {insertOrReplace(it)}
    }

    override fun insertOrReplace(item:DeltaRepo.Item)
    {
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

    override fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
    {
        val recordsToDelete = pks
            .mapNotNull {adapter.selectByPk(it)}
            .asSequence()
        var maxDeleteStampToDelete = recordsToDelete
            .map {it.deleteStamp ?: throw RuntimeException("master repo should not have null for this field")}
            .max() ?: return

        // update all where deleteStamp < maxDeleteStamp AND pk NOT IN pks
        do
        {
            // query for records and prepare for next query
            val records = pageByDeleteStamp(maxDeleteStampToDelete,Order.DESC,DeltaRepo.BATCH_SIZE)
                .filter {it.deleteStamp == maxDeleteStampToDelete}
            records.mapNotNull {it.deleteStamp}.min()?.let {maxDeleteStampToDelete = it}

            // update all where deleteStamp < maxDeleteStamp AND pk NOT IN pks
            records.filter {it.pk !in pks}.forEach {
                adapter.insertOrReplace(it.copy(deleteStamp = adapter.computeNextDeleteStamp()))
            }
        }
        while(records.isNotEmpty())

        // delete records...
        val minDeleteStampToKeep = pageByDeleteStamp(maxDeleteStampToDelete,Order.ASC,1)
            .singleOrNull()?.deleteStamp ?: Long.MAX_VALUE
        adapter.delete(minDeleteStampToKeep)
    }
}
