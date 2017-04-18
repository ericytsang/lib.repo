package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

enum class Order {ASC,DESC}

interface DeltaRepo
{
    companion object
    {
        const val PAGE_SIZE:Int = 500
    }
    interface Pk
    {
        val id:Long
    }
    interface Item:Repo.Pk
    {
        interface Pk:com.github.ericytsang.lib.repo.Repo.Pk
        {
            val nodePk:DeltaRepo.Pk
        }
        val updateStamp:Long
        val deleteStamp:Long
        val isSynced:Boolean
    }
    val pk:Pk
    fun read(block:()->Unit)
    fun write(block:()->Unit)
    fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
    fun selectByPk(pk:Item.Pk)
    fun selectNextUnsyncedToSync()
}

interface MutableDeltaRepo:DeltaRepo
{
    fun computeNextPk():DeltaRepo.Item.Pk
    fun insertOrReplace(item:DeltaRepo.Item)
}

interface MasterRepo:MutableDeltaRepo
{
    fun conputeNextUpdateStamp():Long
    fun conputeNextDeleteStamp():Long
}

interface MirrorRepo:MutableDeltaRepo
{
    fun synchronizeWith(source:DeltaRepo)
    fun update(pk:DeltaRepo.Item.Pk,newDeleteStamp:Long)
    fun delte(minDeleteStamp:Long) // fixme cant delete MAX_LONG...nvm, better than not being able to delete nothing e.g. delete(Long.MIN_VALUE)
}

abstract class AbstractMirrorRepo:MirrorRepo
{
    override fun synchronizeWith(source:DeltaRepo)
    {
        val lastUpdatedItem:DeltaRepo.Item? = pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).singleOrNull()
        val lastUpdateStamp = lastUpdatedItem?.updateStamp ?: Long.MIN_VALUE
        val newUpdates = source.pageByUpdateStamp(lastUpdateStamp,Order.ASC,DeltaRepo.PAGE_SIZE,lastUpdateStamp).filter {it == lastUpdatedItem}
        newUpdates

    }
}
