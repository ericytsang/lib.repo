package com.github.ericytsang.lib.deltarepo

class SimpleMasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:MasterRepoAdapter<ItemPk,Item>):BaseRepo(),MutableMasterRepo<ItemPk,Item>
{
    override fun insertOrReplace(items:Iterable<Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk):Set<Item>
    {
        checkCanWrite()

        val (toDelete,toInsert) = items
            .asSequence()
            .localized(localRepoInterRepoId,remoteRepoInterRepoId)
            .map {it.copy(Unit,syncStatus = DeltaRepo.Item.SyncStatus.PULLED)}
            .partition {it.isDeleted}
        val _toInsert = toInsert
            .toSet()
            .asSequence()
            .map {it.copy(Unit,updateStamp = adapter.computeNextUpdateStamp())}
            .toList()
        _toInsert.forEach {
                adapter.insertOrReplace(it)
            }
        deleteByPk(toDelete.map {it.pk}.toSet())
        return (toDelete+_toInsert).toSet()
    }

    override fun deleteByPk(pks:Set<ItemPk>)
    {
        checkCanWrite()
        val recordsToDelete = pks
            .mapNotNull {adapter.selectByPk(it)}
            .asSequence()
        val maxUpsateStampToDelete = recordsToDelete
            .map {it.updateStamp ?: throw RuntimeException("master repo should not have null for this field")}
            .max() ?: return
        var pageStart = maxUpsateStampToDelete

        // update all where deleteStamp < maxDeleteStamp AND pk NOT IN pks
        do
        {
            // query for records and prepare for next query
            val records = pageByUpdateStamp(pageStart,Order.DESC,DeltaRepo.BATCH_SIZE,setOf(DeltaRepo.Item.SyncStatus.PULLED))
                .filter {it.updateStamp != pageStart}
            records.mapNotNull {it.updateStamp}.min()?.let {pageStart = it}

            // update all where updateStamp < maxUpdateStamp AND pk NOT IN pks
            records.filter {it.pk !in pks}.forEach {
                adapter.insertOrReplace(it.copy(Unit,updateStamp = adapter.computeNextUpdateStamp()))
            }
        }
        while(records.isNotEmpty())

        // delete records...
        val minDeleteStampToKeep = pageByUpdateStamp(maxUpsateStampToDelete,Order.ASC,2,setOf(DeltaRepo.Item.SyncStatus.PULLED))
            .drop(1).singleOrNull()?.updateStamp ?: Long.MAX_VALUE
        adapter.delete(minDeleteStampToKeep)
    }

    override fun computeNextPk():ItemPk
    {
        checkCanWrite()
        return adapter.computeNextPk()
    }

    override fun selectByPk(pk:ItemPk):Item?
    {
        checkCanRead()
        return adapter.selectByPk(pk)
    }

    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>):List<Item>
    {
        checkCanRead()
        return adapter.pageByUpdateStamp(start,order,limit,syncStatus)
    }
}
