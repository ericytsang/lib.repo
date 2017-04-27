package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

data class MockItem(
    override val pk:Pk,
    override val updateStamp:Long?,
    override val syncStatus:DeltaRepo.Item.SyncStatus,
    override val isDeleted:Boolean)
    :DeltaRepo.Item<MockItem.Pk,MockItem>
{
    data class Pk(
        override val nodePk:DeltaRepo.Pk,
        override val pk:Repo.Item.Pk)
        :DeltaRepo.Item.Pk<Pk>
    {
        override fun copy(
            unit:Unit,
            nodePk:DeltaRepo.Pk,
            pk:Repo.Item.Pk)
            :Pk
        {
            return copy(nodePk,pk)
        }
    }
    override fun copy(
        unit:Unit,
        pk:Pk,
        updateStamp:Long?,
        syncStatus:DeltaRepo.Item.SyncStatus,
        isDeleted:Boolean)
        :MockItem
    {
        return copy(pk,updateStamp,syncStatus,isDeleted)
    }
}
