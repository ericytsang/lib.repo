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

interface DeltaRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo,Pullable<ItemPk,Item>
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
        val syncStatus:SyncStatus
        val isDeleted:Boolean
        enum class SyncStatus {DIRTY,PUSHED,PULLED}
        fun copy(
            unit:Unit,
            pk:Pk = this.pk,
            updateStamp:Long? = this.updateStamp,
            syncStatus:SyncStatus = this.syncStatus,
            isDeleted:Boolean = this.isDeleted)
            :SubClass
    }
    fun selectByPk(pk:ItemPk):Item?
    override fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>):List<Item>
}

// master & mirror repo adapters

interface BaseRepoAdapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
{
    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk].
     */
    fun insertOrReplace(item:Item)

    /**
     * delete all where [DeltaRepo.Item.updateStamp] < [minUpdateStampToKeep].
     */
    fun delete(minUpdateStampToKeep:Long)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}

interface MasterRepoAdapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:BaseRepoAdapter<ItemPk,Item>
{
    /**
     * returns the next unused update stamp.
     */
    fun computeNextUpdateStamp():Long
}

interface MirrorRepoAdapter<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:BaseRepoAdapter<ItemPk,Item>
{
    /**
     * selects the next [limit] [DeltaRepo.Item]s to sync to the [MasterRepo].
     * [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.DIRTY]
     */
    fun selectNextUnsyncedToSync(limit:Int):List<Item>
}

// read-only master & mirror repos

interface MasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>

interface MirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:DeltaRepo<ItemPk,Item>
{
    /**
     * selects the next [limit] [DeltaRepo.Item]s to sync to the [MasterRepo].
     * [DeltaRepo.Item.syncStatus] == [DeltaRepo.Item.SyncStatus.DIRTY]
     */
    fun selectNextUnsyncedToSync(limit:Int):List<Item>
}

// mutable master & mirror repos

interface MutableMasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:MasterRepo<ItemPk,Item>,Pushable<ItemPk,Item>
{
    /**
     * inserts [items] into the repo. replaces any existing record with
     * conflicting [DeltaRepo.Item.pk].
     *
     * may be used by [MirrorRepo]s to merge new records into the [MasterRepo].
     *
     * if [DeltaRepo.Item.isDeleted] == false, then the record is deleted.
     *
     * automatically sets:
     * - [DeltaRepo.Item.syncStatus] = [DeltaRepo.Item.SyncStatus.PULLED]
     * - [DeltaRepo.Item.updateStamp] = [MasterRepoAdapter.computeNextUpdateStamp]
     */
    override fun insertOrReplace(items:Iterable<Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk):Set<Item>

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
    /**
     * pushes changes from this [MirrorRepo] up to [remote].
     */
    fun push(remote:Pushable<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)

    /**
     * pulls changes from [remote] and applies it to this [MirrorRepo].
     */
    fun pull(remote:Pullable<ItemPk,Item>,localRepoInterRepoId:DeltaRepoPk,remoteRepoInterRepoId:DeltaRepoPk)

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
    fun insertOrReplace(items:Iterable<Item>,localRepoInterRepoId:DeltaRepoPk = DeltaRepo.LOCAL_NODE_ID,remoteRepoInterRepoId:DeltaRepoPk = DeltaRepo.LOCAL_NODE_ID):Set<Item>

    /**
     * convenience method for [insertOrReplace].
     *
     * sets:
     * - [DeltaRepo.Item.isDeleted] = true
     * where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pk:ItemPk)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():ItemPk
}

interface Pushable<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
{
    /**
     * [MutableMasterRepo.insertOrReplace]
     */
    fun insertOrReplace(items:Iterable<Item>,localRepoInterRepoId:DeltaRepoPk = DeltaRepo.LOCAL_NODE_ID,remoteRepoInterRepoId:DeltaRepoPk = DeltaRepo.LOCAL_NODE_ID):Set<Item>
}

interface Pullable<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>
{
    /**
     * [MasterRepo.pageByUpdateStamp]
     */
    fun pageByUpdateStamp(start:Long,order:Order,limit:Int,syncStatus:Set<DeltaRepo.Item.SyncStatus>):List<Item>
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
