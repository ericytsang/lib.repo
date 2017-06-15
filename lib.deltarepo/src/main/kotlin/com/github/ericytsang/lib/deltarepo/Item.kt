package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

interface Item<C:Item<C>>
{
    val updateSequence:Long
    val isDeleted:Boolean
    fun copy(newData:SimpleItem):C
}

data class SimpleItem(
    override val updateSequence:Long,
    override val isDeleted:Boolean):
    Item<SimpleItem>,
    Serializable
{
    override fun copy(newData:SimpleItem):SimpleItem
    {
        return copy(newData.updateSequence,newData.isDeleted)
    }
}

internal fun <S:Item<S>> S._copy(
    updateSequence:Long = this.updateSequence,
    isDeleted:Boolean = this.isDeleted):
    S
{
    return copy(SimpleItem(updateSequence,isDeleted))
}
