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
    interface Item
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
    }
}

internal interface ItemAdapter<Item:Any>
{
    val Item.metadata:DeltaRepo.Item.Metadata

    fun Item.copy(newMetadata:DeltaRepo.Item.Metadata):Item
}

internal fun <Item:Any> Sequence<Item>.localized(adapter:ItemAdapter<Item>,localRepoInterRepoId:DeltaRepo.RepoPk,remoteRepoInterRepoId:DeltaRepo.RepoPk):Sequence<Item>
{
    return this
        // convert remote-relative addresses to absolute addresses
        .map {
            if (with(adapter) {it.metadata}.pk.repoPk == DeltaRepo.LOCAL_NODE_ID)
            {
                with(adapter) {it.copy(it.metadata.copy(pk = with(adapter) {it.metadata}.pk.copy(repoPk = remoteRepoInterRepoId)))}
            }
            else
            {
                it
            }
        }
        // convert absolute addresses to local-relative addresses
        .map {
            if (with(adapter) {it.metadata}.pk.repoPk == localRepoInterRepoId)
            {
                with(adapter) {it.copy(it.metadata.copy(pk = with(adapter) {it.metadata}.pk.copy(repoPk = DeltaRepo.LOCAL_NODE_ID)))}
            }
            else
            {
                it
            }
        }
}
