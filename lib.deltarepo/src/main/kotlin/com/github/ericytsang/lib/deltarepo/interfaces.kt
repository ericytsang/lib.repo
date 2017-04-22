package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

enum class Order {ASC,DESC}

fun <T:Comparable<T>> Order.isAfterOrEqual(curr:T,next:T):Boolean
{
    return when(this)
    {
        Order.ASC -> next >= curr
        Order.DESC -> next <= curr
    }
}

// read-only delta repo

interface DeltaRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:Repo
{
    companion object
    {
        const val BATCH_SIZE:Int = 500
    }
    interface Pk
    {
        val id:Long
    }
    interface Item<out Pk:Item.Pk,SubClass:DeltaRepo.Item<Pk,SubClass>>
    {
        interface Pk
        {
            val nodePk:DeltaRepo.Pk
            val pk:Repo.Item.Pk
        }
        val pk:Pk
        val updateStamp:Long?
        val deleteStamp:Long?
        val isSynced:Boolean
        val isDeleted:Boolean
        fun copy(
            updateStamp:Long = updateStamp,
            deleteStamp:Long = deleteStamp,
            isSynced:Boolean = isSynced,
            isDeleted:Boolean = isDeleted)
            :SubClass
    }
    val pk:Pk
    fun selectByPk(pk:ItemPk):Item?
    fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
}

// mutable delta repo

interface MutableDeltaRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
{
    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk].
     */
    fun insertOrReplace(item:Item)

    /**
     * delete all where [DeltaRepo.Item.deleteStamp] < [minDeleteStampToKeep].
     */
    fun delete(minDeleteStampToKeep:Long)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}

// read-only master & mirror repos

interface MasterRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>

interface MirrorRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
{
    /**
     * selects the next [limit] [DeltaRepo.Item]s to sync to the [MasterRepo].
     */
    fun selectNextUnsyncedToSync(limit:Int):List<Item>
}

// master & mirror repo adapters

interface MasterRepoAdapter<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:MutableDeltaRepo<ItemPk,Item>,MasterRepo<ItemPk,Item>
{
    /**
     * returns the next unused update stamp.
     */
    fun computeNextUpdateStamp():Long

    /**
     * returns the next unused delete stamp.
     */
    fun computeNextDeleteStamp():Long
}

interface MirrorRepoAdapter<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:MutableDeltaRepo<ItemPk,Item>,MirrorRepo<ItemPk,Item>

// mutable master & mirror repos

interface MutableMasterRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:MasterRepo<ItemPk,Item>
{
    /**
     * inserts [item] into the repo. replaces any existing record whose
     * [DeltaRepo.Item.pk] == [item.pk].
     *
     * requires that:
     * - [DeltaRepo.Item.isDeleted] = false
     *
     * automatically sets:
     * - [DeltaRepo.Item.isSynced] = true
     * - [DeltaRepo.Item.deleteStamp] = [MasterRepoAdapter.computeNextDeleteStamp]
     * - [DeltaRepo.Item.updateStamp] = [MasterRepoAdapter.computeNextUpdateStamp]
     */
    fun insertOrReplace(item:Item)

    /**
     * inserts items from
     */
    fun merge(items:Set<Item>)

    /**
     * delete all where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pks:Set<ItemPk>)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}

interface MutableMirrorRepo<ItemPk:DeltaRepo.Item.Pk,Item:DeltaRepo.Item<ItemPk,Item>>:MirrorRepo<ItemPk,Item>
{
    fun synchronizeWith(source:DeltaRepo<ItemPk,Item>)

    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk]. the [item]'s [DeltaRepo.Item.isSynced] will be set
     * to false by this function when it is inserted.
     */
    fun insertOrReplace(item:Item)

    /**
     * [DeltaRepo.Item.isDeleted] = true
     * [DeltaRepo.Item.isSynced] = false
     * where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pk:ItemPk)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}
