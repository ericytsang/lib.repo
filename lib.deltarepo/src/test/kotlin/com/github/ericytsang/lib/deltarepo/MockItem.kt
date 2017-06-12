package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

data class MockItem(
    val pk:Long,
    override val updateSequence:Long,
    override val isDeleted:Boolean,
    val string:String)
    :Item
    ,Serializable
{
    override fun copy(newData:Item):Item
    {
        return copy(pk,newData.updateSequence,newData.isDeleted,string)
    }
}
