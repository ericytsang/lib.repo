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

data class DeltaRepoPk(override val id:Long):DeltaRepo.Pk

data class RepoItemPk(override val id:Long):Repo.Item.Pk

interface DeltaRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo
{
    companion object
    {
        val LOCAL_NODE_ID:DeltaRepoPk = DeltaRepoPk(-1)
    }
    interface Pk:Serializable
    {
        val id:Long
    }
    interface Item<Pk:DeltaRepo.Item.Pk<Pk>,SubClass:DeltaRepo.Item<Pk,SubClass>>
    {
        interface Pk<SubClass:DeltaRepo.Item.Pk<SubClass>>
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

internal fun <ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>> Sequence<Item>.localized(localRepoInterRepoId:DeltaRepo.Pk,remoteRepoInterRepoId:DeltaRepo.Pk):Sequence<Item>
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
