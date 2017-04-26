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

interface DeltaRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo
{
    companion object
    {
        const val BATCH_SIZE:Int = 500
        val LOCAL_NODE_ID:DeltaRepoPk = DeltaRepoPk(-1)
    }
    interface Pk
    {
        val id:Long
    }
    interface Item<Pk:Item.Pk<Pk>,SubClass:DeltaRepo.Item<Pk,SubClass>>
    {
        interface Pk<SubClass:Pk<SubClass>>
        {
            val nodePk:DeltaRepo.Pk
            val pk:Repo.Item.Pk
            fun copy(
                unit:Unit,
                nodePk:DeltaRepo.Pk = this.nodePk,
                pk:Repo.Item.Pk = this.pk)
                :SubClass
        }
        val pk:Pk
        val updateStamp:Long?
        val deleteStamp:Long?
        val isSynced:Boolean
        val isDeleted:Boolean
        fun copy(
            unit:Unit,
            pk:Pk = this.pk,
            updateStamp:Long? = this.updateStamp,
            deleteStamp:Long? = this.deleteStamp,
            isSynced:Boolean = this.isSynced,
            isDeleted:Boolean = this.isDeleted)
            :SubClass
    }
    fun selectByPk(pk:ItemPk):Item?
    fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
}

// mutable delta repo

interface MutableDeltaRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
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

interface MasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>

interface MirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
{
    /**
     * selects the next [limit] [DeltaRepo.Item]s to sync to the [MasterRepo].
     */
    fun selectNextUnsyncedToSync(limit:Int):List<Item>
}

// master & mirror repo adapters

interface MasterRepoAdapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:MutableDeltaRepo<ItemPk,Item>,MasterRepo<ItemPk,Item>
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

interface MirrorRepoAdapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:MutableDeltaRepo<ItemPk,Item>,MirrorRepo<ItemPk,Item>

// mutable master & mirror repos

interface MutableMasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:MasterRepo<ItemPk,Item>
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
     * inserts items from [MirrorRepo]s into this [MasterRepo].
     */
    fun merge(items:Set<Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)

    /**
     * delete all where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pks:Set<ItemPk>)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}

interface MutableMirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:MirrorRepo<ItemPk,Item>
{
    fun synchronizeWith(source:MasterRepo<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)

    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk]. the [item]'s [DeltaRepo.Item.isSynced] will be set
     * to false by this function when it is inserted.
     */
    fun insertOrReplace(item:Item)

    /**
     * [DeltaRepo.Item.isSynced] = true
     * where [DeltaRepo.Item.pk] == [pk].
     */
    fun markAsSynced(pk:ItemPk)

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

internal fun <ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>> Sequence<Item>.localized(localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk):Sequence<Item>
{
    return this
        // convert remote-relative addresses to absolute addresses
        .map {
            if (it.pk.nodePk == DeltaRepo.LOCAL_NODE_ID)
            {
                it.copy(Unit,pk = it.pk.copy(Unit,nodePk = remoteRepoInterRepoId))
            }
            else
            {
                it
            }
        }
        // convert absolute addresses to local-relative addresses
        .map {
            if (it.pk.nodePk == localRepoInterRepoId)
            {
                it.copy(Unit,pk = it.pk.copy(Unit,nodePk = DeltaRepo.LOCAL_NODE_ID))
            }
            else
            {
                it
            }
        }
}
