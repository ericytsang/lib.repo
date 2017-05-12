package com.github.ericytsang.lib.deltarepo

import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

class MockItemTest
{
    @Test
    fun serializeableTest()
    {
        val out = ObjectOutputStream(ByteArrayOutputStream())
        out.writeObject(MockItem(MockItem.Pk(DeltaRepo.Item.Pk(DeltaRepo.RepoPk(0),DeltaRepo.ItemPk(0))),3,DeltaRepo.Item.SyncStatus.PULLED,false,""))
    }
}
