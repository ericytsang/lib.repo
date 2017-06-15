package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class IntegrationTest
{
    var mirrorRedownloadedEverything = false

    // create test subjects
    val mirrorAdapter = MockMirrorRepoAdapter()
    val mirror = MirrorRepo(object:MirrorRepo.Adapter<MockItem> by mirrorAdapter
    {
        override fun prepareForResync()
        {
            mirrorRedownloadedEverything = true
            mirrorAdapter.prepareForResync()
        }
    })
    val masterAdapter = MockMasterRepoAdapter()
    val master = MasterRepo(masterAdapter)

    val item1 = MockItem(0,Long.MIN_VALUE+0,false,"0")
    val item2 = MockItem(1,Long.MIN_VALUE+1,false,"1")
    val item3 = MockItem(2,Long.MIN_VALUE+2,false,"2")
    val item4 = MockItem(3,Long.MIN_VALUE+3,false,"3")
    val item5 = MockItem(4,Long.MIN_VALUE+4,false,"4")
    val item6 = MockItem(5,Long.MIN_VALUE+5,false,"5")

    @Test
    fun insertTest()
    {
        // insert records into master
        master.insertOrReplace(item1)
        master.insertOrReplace(item2)
        master.insertOrReplace(item3)
        master.insertOrReplace(item4)
        master.insertOrReplace(item5)
        master.insertOrReplace(item6)

        // sync records to mirrors
        run {
            mirror.pullAll(master)
        }

        // check records are in all repos
        run {
            check(mirrorAdapter.records[0] == item1)
            check(mirrorAdapter.records[1] == item2)
            check(mirrorAdapter.records[2] == item3)
            check(mirrorAdapter.records[3] == item4)
            check(mirrorAdapter.records[4] == item5)
            check(mirrorAdapter.records[5] == item6)
            check(!mirrorRedownloadedEverything)
        }
    }

    @Test
    fun undeleteTest()
    {
        delete2RecordsTest()

        // undelete a record
        master.insertOrReplace(item6)

        // sync
        mirror.pullAll(master)

        // check records are deleted in all repos and merging is as expected
        run {
            check(mirrorAdapter.records[0] == item1)
            check(mirrorAdapter.records[1] == item2)
            check(mirrorAdapter.records[2] == item3)
            check(mirrorAdapter.records[3] == item4)
            check(mirrorAdapter.records[4] == null)
            check(mirrorAdapter.records[5] == item6.copy(updateSequence = Long.MIN_VALUE+8))
            check(!mirrorRedownloadedEverything)
        }
    }

    @Test
    fun delete2RecordsTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            master.delete(item5)
            master.delete(item6)
        }

        // sync master to mirror
        run {
            mirror.pullAll(master)
        }

        // check records are deleted in all repos and merging is as expected
        run {
            check(mirrorAdapter.records[0] == item1)
            check(mirrorAdapter.records[1] == item2)
            check(mirrorAdapter.records[2] == item3)
            check(mirrorAdapter.records[3] == item4)
            check(mirrorAdapter.records[4] == null)
            check(mirrorAdapter.records[5] == null)
            check(!mirrorRedownloadedEverything)
        }
    }

    @Test
    fun delete4RecordsTest()
    {
        insertTest()

        // delete more records from mirror2 such that mirror1 will need to resync
        run {
            master.delete(item1)
            master.delete(item2)
            master.delete(item5)
            master.delete(item6)
        }

        // sync master to mirror
        run {
            mirror.pullAll(master)
        }

        // check records are deleted in all repos and merging is as expected
        run {
            check(mirrorAdapter.records[0] == null)
            check(mirrorAdapter.records[1] == null)
            check(mirrorAdapter.records[2] == item3)
            check(mirrorAdapter.records[3] == item4)
            check(mirrorAdapter.records[4] == null)
            check(mirrorAdapter.records[5] == null)
            check(mirrorRedownloadedEverything)
        }
    }
}
