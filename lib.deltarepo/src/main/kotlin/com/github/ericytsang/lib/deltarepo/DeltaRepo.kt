package com.github.ericytsang.lib.deltarepo

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

interface DeltaRepo
{
    companion object
    {
        val LOCAL_NODE_ID:RepoPk = RepoPk(-1)
    }
    data class RepoPk(val id:Long):Serializable
    data class ItemPk(val id:Long):Serializable
    interface Item<SubClass:Item<SubClass>>
    {
        companion object;
        data class Pk(
            val repoPk:DeltaRepo.RepoPk,
            val itemPk:DeltaRepo.ItemPk)
        data class Metadata(
            val pk:Pk,
            val updateStamp:Long?,
            val syncStatus:SyncStatus,
            val isDeleted:Boolean)
        enum class SyncStatus {DIRTY,PUSHED,PULLED}
        val metadata:Metadata
        fun copy(newMetadata:Metadata):SubClass
    }
}

internal fun <SubClass:DeltaRepo.Item<SubClass>> SubClass.copy(
    pk:DeltaRepo.Item.Pk = metadata.pk,
    updateStamp:Long? = metadata.updateStamp,
    syncStatus:DeltaRepo.Item.SyncStatus = metadata.syncStatus,
    isDeleted:Boolean = metadata.isDeleted)
    :SubClass
{
    return copy(DeltaRepo.Item.Metadata(pk,updateStamp,syncStatus,isDeleted))
}

internal fun <Item:DeltaRepo.Item<Item>> Sequence<Item>.localized(localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Sequence<Item>
{
    return this
        // convert remote-relative addresses to absolute addresses
        .map {
            if (it.metadata.pk.repoPk == DeltaRepo.LOCAL_NODE_ID)
            {
                it.copy(pk = it.metadata.pk.copy(repoPk = remoteRepoInterRepoId))
            }
            else
            {
                it
            }
        }
        // convert absolute addresses to local-relative addresses
        .map {
            if (it.metadata.pk.repoPk == localRepoInterRepoId)
            {
                it.copy(pk = it.metadata.pk.copy(repoPk = DeltaRepo.LOCAL_NODE_ID))
            }
            else
            {
                it
            }
        }
}
