package com.github.ericytsang.lib.deltarepo

interface Item
{
    data class Pk(val repoPk:RepoPk,val itemPk:ItemPk)
    {
        data class RepoPk(val id:Long)
        {
            companion object
            {
                val LOCAL_REPO_PK = RepoPk(-1)
            }
        }
        data class ItemPk(val id:Long)
    }
    enum class SyncStatus {DIRTY, PUSHED, PULLED}
    val pk:Pk
    val updateSequence:Long
    val isDeleted:Boolean
    val syncStatus:SyncStatus
    fun copy(newData:Item):Item
}

internal data class SimpleItem(
    override val pk:Item.Pk,
    override val updateSequence:Long,
    override val isDeleted:Boolean,
    override val syncStatus:Item.SyncStatus):
    Item
{
    override fun copy(newData:Item):Item
    {
        return copy(newData.pk,newData.updateSequence,newData.isDeleted,newData.syncStatus)
    }
}

internal fun Item._copy(
    pk:Item.Pk = this.pk,
    updateSequence:Long = this.updateSequence,
    isDeleted:Boolean = this.isDeleted,
    syncStatus:Item.SyncStatus = this.syncStatus):
    Item
{
    return copy(SimpleItem(pk,updateSequence,isDeleted,syncStatus))
}

internal fun Sequence<Item>.localized(localRepoInterRepoId:Item.Pk.RepoPk,remoteRepoInterRepoId:Item.Pk.RepoPk):Sequence<Item>
{
    return this
        // convert remote-relative addresses to absolute addresses
        .map {
            if (it.pk.repoPk == Item.Pk.RepoPk.LOCAL_REPO_PK)
            {
                it._copy(pk = it.pk.copy(repoPk = remoteRepoInterRepoId))
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
                it._copy(pk = it.pk.copy(repoPk = Item.Pk.RepoPk.LOCAL_REPO_PK))
            }
            else
            {
                it
            }
        }
}
