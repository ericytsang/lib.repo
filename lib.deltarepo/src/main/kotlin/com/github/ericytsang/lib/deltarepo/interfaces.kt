package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

enum class Order {ASC,DESC}

// read-only delta repo

interface DeltaRepo:Repo
{
    companion object
    {
        const val BATCH_SIZE:Int = 500
    }
    interface Pk
    {
        val id:Long
    }
    interface Item
    {
        interface Pk
        {
            val nodePk:DeltaRepo.Pk
            val pk:Repo.Item.Pk
        }
        val pk:Pk
        val updateStamp:Long?
        val deleteStamp:Long?
        val isSynced:Boolean
        val isDeleted:Boolean
        fun copy(
            updateStamp:Long = updateStamp,
            deleteStamp:Long = deleteStamp,
            isSynced:Boolean = isSynced,
            isDeleted:Boolean = isDeleted)
            :Item
    }
    val pk:Pk
    fun selectByPk(pk:Item.Pk):Item?
    fun pageByUpdateStamp(start:Long,order:Order,limit:Int):List<Item>
    fun pageByDeleteStamp(start:Long,order:Order,limit:Int):List<Item>
}

// mutable delta repo

interface MutableDeltaRepo:DeltaRepo
{
    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk].
     */
    fun insertOrReplace(item:DeltaRepo.Item)

    /**
     * delete all where [DeltaRepo.Item.deleteStamp] < [minDeleteStampToKeep].
     */
    fun delete(minDeleteStampToKeep:Long)

    /**
     * returns the next unused primary key.
     */
    fun computeNextPk():DeltaRepo.Item.Pk
}

// read-only master & mirror repos

interface MasterRepo:DeltaRepo

interface MirrorRepo:DeltaRepo
{
    /**
     * selects the next [limit] [DeltaRepo.Item]s to sync to the [MasterRepo].
     */
    fun selectNextUnsyncedToSync(limit:Int):List<DeltaRepo.Item>
}

// master & mirror repo adapters

interface MasterRepoAdapter:MutableDeltaRepo,MasterRepo
{
    /**
     * returns the next unused update stamp.
     */
    fun computeNextUpdateStamp():Long

    /**
     * returns the next unused delete stamp.
     */
    fun computeNextDeleteStamp():Long
}

interface MirrorRepoAdapter:MutableDeltaRepo,MirrorRepo

// mutable master & mirror repos

interface MutableMasterRepo:MasterRepo
{
    /**
     * inserts [item] into the repo. replaces any existing record whose
     * [DeltaRepo.Item.pk] == [item.pk].
     *
     * requires that:
     * - [DeltaRepo.Item.isDeleted] = false
     *
     * automatically sets:
     * - [DeltaRepo.Item.isSynced] = true
     * - [DeltaRepo.Item.deleteStamp] = [MasterRepoAdapter.computeNextDeleteStamp]
     * - [DeltaRepo.Item.updateStamp] = [MasterRepoAdapter.computeNextUpdateStamp]
     */
    fun insertOrReplace(item:DeltaRepo.Item)

    /**
     * inserts items from
     */
    fun merge(items:Set<DeltaRepo.Item>)

    /**
     * delete all where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pks:Set<DeltaRepo.Item.Pk>)
}

interface MutableMirrorRepo:MirrorRepo
{
    fun synchronizeWith(source:DeltaRepo)

    /**
     * inserts [item] into the repo. replaces any existing record with the same
     * [DeltaRepo.Item.pk]. the [item]'s [DeltaRepo.Item.isSynced] will be set
     * to false by this function when it is inserted.
     */
    fun insertOrReplace(item:DeltaRepo.Item)

    /**
     * [DeltaRepo.Item.isDeleted] = true
     * [DeltaRepo.Item.isSynced] = false
     * where [DeltaRepo.Item.pk] == [pk].
     */
    fun deleteByPk(pk:DeltaRepo.Item.Pk)
}
