package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo
import java.io.Serializable

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
        val LOCAL_NODE_ID:RepoPk = RepoPk(-1)
    }
    data class RepoPk(val id:Long):Serializable
    data class ItemPk(val id:Long):Serializable
    interface Item<Pk:DeltaRepo.Item.Pk<Pk>,SubClass:DeltaRepo.Item<Pk,SubClass>>
    {
        companion object;
        interface Pk<SubClass:DeltaRepo.Item.Pk<SubClass>>
        {
            companion object;
            val repoPk:DeltaRepo.RepoPk
            val itemPk:DeltaRepo.ItemPk
            fun copy(
                unused:Companion,
                repoPk:DeltaRepo.RepoPk = this.repoPk,
                itemPk:DeltaRepo.ItemPk = this.itemPk)
                :SubClass
        }
        val pk:Pk
        val updateStamp:Long?
        val syncStatus:SyncStatus
        val isDeleted:Boolean
        enum class SyncStatus {DIRTY,PUSHED,PULLED}
        fun copy(
            unused:Companion,
            pk:Pk = this.pk,
            updateStamp:Long? = this.updateStamp,
            syncStatus:SyncStatus = this.syncStatus,
            isDeleted:Boolean = this.isDeleted)
            :SubClass
    }

    /**
     * inserts [items] into the repo and replaces any existing records with
     * conflicting [Item.pk]s.
     */
    fun insertOrReplace(items:Iterable<Item>):Set<Item>

    /**
     * deletes all records where [Item.pk] in [pks].
     */
    fun deleteByPk(pks:Set<ItemPk>)

    /**
     * returns the next unused [ItemPk]
     */
    fun computeNextPk():ItemPk
}

internal fun <ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>> Sequence<Item>.localized(localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Sequence<Item>
{
    return this
        // convert remote-relative addresses to absolute addresses
        .map {
            if (it.pk.repoPk == DeltaRepo.LOCAL_NODE_ID)
            {
                it.copy(DeltaRepo.Item.Companion,pk = it.pk.copy(DeltaRepo.Item.Pk.Companion,repoPk = remoteRepoInterRepoId))
            }
            else
            {
                it
            }
        }
        // convert absolute addresses to local-relative addresses
        .map {
            if (it.pk.repoPk == localRepoInterRepoId)
            {
                it.copy(DeltaRepo.Item.Companion,pk = it.pk.copy(DeltaRepo.Item.Pk.Companion,repoPk = DeltaRepo.LOCAL_NODE_ID))
            }
            else
            {
                it
            }
        }
}
