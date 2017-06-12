package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

interface Item
{
    val updateSequence:Long
    val isDeleted:Boolean
    fun copy(newData:Item):Item
}

internal data class SimpleItem(
    override val updateSequence:Long,
    override val isDeleted:Boolean):
    Item,
    Serializable
{
    override fun copy(newData:Item):SimpleItem
    {
        return copy(newData.updateSequence,newData.isDeleted)
    }
}

internal fun Item._copy(
    updateSequence:Long = this.updateSequence,
    isDeleted:Boolean = this.isDeleted):
    Item
{
    return copy(SimpleItem(updateSequence,isDeleted))
}
