package com.github.ericytsang.lib.deltarepo

data class MockItem(
    val pk:Pk,
    val updateStamp:Long?,
    val syncStatus:DeltaRepo.Item.SyncStatus,
    val isDeleted:Boolean,
    val string:String)
    :DeltaRepo.Item
{
    data class Pk(val value:DeltaRepo.Item.Pk)
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

    fun metadata(unused:DeltaRepo.Item.Companion):DeltaRepo.Item.Metadata
    {
        return DeltaRepo.Item.Metadata(pk.value,updateStamp,syncStatus,isDeleted)
    }
}
