package com.github.ericytsang.lib.deltarepo

class SimpleMirrorRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>(private val adapter:MirrorRepoAdapter<ItemPk,Item>):BaseRepo(),MutableMirrorRepo<ItemPk,Item>
{
    override fun synchronizeWith(source:DeltaRepo<ItemPk,Item>)
    {
        checkCanWrite()

        // synchronize updates
        run {
            do
            {
                val maxUpdateStampItem = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
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
                val maxDeleteStampItem = pageByDeleteStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
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
            val minDeleteStampItem = source.pageByDeleteStamp(Long.MIN_VALUE,Order.ASC,1).singleOrNull()
            val minDeleteStamp = minDeleteStampItem?.deleteStamp ?: Long.MAX_VALUE
            adapter.delete(minDeleteStamp)
        }
    }

    override fun insertOrReplace(item:Item)
    {
        checkCanWrite()
        adapter.insertOrReplace(item.copy(isSynced = false))
    }

    override fun deleteByPk(pk:ItemPk)
    {
        checkCanWrite()
        val item = selectByPk(pk) ?: return
        adapter.insertOrReplace(item.copy(isDeleted = true))
    }

    override fun computeNextPk():ItemPk
    {
        checkCanWrite()
        return adapter.computeNextPk().apply {check(this.nodePk.id == this@SimpleMirrorRepo.pk.id)}
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

    override fun selectNextUnsyncedToSync(limit:Int):List<Item>
    {
        checkCanRead()
        return adapter.selectNextUnsyncedToSync(limit)
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
