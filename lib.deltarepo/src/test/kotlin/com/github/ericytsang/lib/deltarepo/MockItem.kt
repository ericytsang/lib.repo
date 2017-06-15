package com.github.ericytsang.lib.deltarepo

import java.io.Serializable

data class MockItem(
    val pk:Long,
    override val updateSequence:Long,
    override val isDeleted:Boolean,
    val string:String)
    :Item<MockItem>
    ,Serializable
{
    override fun copy(newData:SimpleItem):MockItem
    {
        return copy(pk,newData.updateSequence,newData.isDeleted,string)
    }
}
