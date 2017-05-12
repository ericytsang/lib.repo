package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

data class MockItem(
    val pk:Pk,
    val updateStamp:Long?,
    val syncStatus:DeltaRepo.Item.SyncStatus,
    val isDeleted:Boolean,
    val string:String)
    :DeltaRepo.Item<MockItem>
{
    data class Pk(val value:DeltaRepo.Item.Pk):Serializable
    fun copy(
        unused:DeltaRepo.Item.Companion,
        pk:DeltaRepo.Item.Pk = this.pk.value,
        updateStamp:Long? = this.updateStamp,
        syncStatus:DeltaRepo.Item.SyncStatus = this.syncStatus,
        isDeleted:Boolean = this.isDeleted)
        :MockItem
    {
        return copy(Pk(pk),updateStamp,syncStatus,isDeleted)
    }

    override fun copy(newMetadata:DeltaRepo.Item.Metadata):MockItem
    {
        return copy(DeltaRepo.Item.Companion,newMetadata.pk,newMetadata.updateStamp,newMetadata.syncStatus,newMetadata.isDeleted)
    }

    override val metadata:DeltaRepo.Item.Metadata get()
    {
        return DeltaRepo.Item.Metadata(pk.value,updateStamp,syncStatus,isDeleted)
    }
}
