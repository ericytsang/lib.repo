package com.github.ericytsang.lib.deltarepo

data class MockItem(
    override val pk:Pk,
    override val updateStamp:Long?,
    override val syncStatus:DeltaRepo.Item.SyncStatus,
    override val isDeleted:Boolean,
    val string:String)
    :DeltaRepo.Item<MockItem.Pk,MockItem>
{
    data class Pk(
        override val repoPk:DeltaRepo.RepoPk,
        override val itemPk:DeltaRepo.ItemPk)
        :DeltaRepo.Item.Pk<Pk>
    {
        override fun copy(
            unused:DeltaRepo.Item.Pk.Companion,
            repoPk:DeltaRepo.RepoPk,
            itemPk:DeltaRepo.ItemPk)
            :Pk
        {
            return copy(repoPk,itemPk)
        }
    }
    override fun copy(
        unused:DeltaRepo.Item.Companion,
        pk:Pk,
        updateStamp:Long?,
        syncStatus:DeltaRepo.Item.SyncStatus,
        isDeleted:Boolean)
        :MockItem
    {
        return copy(pk,updateStamp,syncStatus,isDeleted)
    }
}
