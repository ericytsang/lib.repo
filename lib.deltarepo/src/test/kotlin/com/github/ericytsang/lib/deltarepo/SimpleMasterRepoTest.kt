package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class SimpleMasterRepoTest
{
    private val testSubjectAdapter = MockMasterRepoAdapter()
    private val testSubject = MasterRepo(testSubjectAdapter)

    @Test
    fun insertTest()
    {
        val item1 = MockItem(0,0,false,"0")
        val item2 = MockItem(1,0,false,"1")
        val item3 = MockItem(2,0,false,"2")
        val item4 = MockItem(3,0,true,"3")
        val item5 = MockItem(4,0,true,"4")
        val item6 = MockItem(5,0,false,"5")
        testSubject.insertOrReplace(item1)
        testSubject.insertOrReplace(item2)
        testSubject.insertOrReplace(item3)
        testSubject.insertOrReplace(item4)
        testSubject.insertOrReplace(item5)
        testSubject.insertOrReplace(item6)

        // update stamps should have changed upon insertion so that mirror repos
        // can query for the updates that they have missed. all other data
        // should as as they were at insertion
        check(testSubjectAdapter.select(item1) == item1.copy(updateSequence = Long.MIN_VALUE+0))
        check(testSubjectAdapter.select(item2) == item2.copy(updateSequence = Long.MIN_VALUE+1))
        check(testSubjectAdapter.select(item3) == item3.copy(updateSequence = Long.MIN_VALUE+2))
        check(testSubjectAdapter.select(item4) == item4.copy(updateSequence = Long.MIN_VALUE+3))
        check(testSubjectAdapter.select(item5) == item5.copy(updateSequence = Long.MIN_VALUE+4))
        check(testSubjectAdapter.select(item6) == item6.copy(updateSequence = Long.MIN_VALUE+5))
    }

    @Test
    fun replaceTest()
    {
        val itemV1 = MockItem(0,0,false,"0")
        val itemV2 = MockItem(0,0,true,"hello")

        // update stamps should have changed upon insertion so that mirror repos
        // can query for the updates that they have missed. all other data
        // should as as they were at insertion
        testSubject.insertOrReplace(itemV1)
        check(testSubjectAdapter.select(itemV1) == itemV1.copy(updateSequence = Long.MIN_VALUE+0))
        testSubject.insertOrReplace(itemV2)
        check(testSubjectAdapter.select(itemV1) == itemV2.copy(updateSequence = Long.MIN_VALUE+1))
    }

    @Test
    fun deleteTest()
    {
        // set up the test subject...insert items into it so we can delete some
        val item1 = MockItem(0,0,false,"0")
        val item2 = MockItem(1,0,false,"1")
        val item3 = MockItem(2,0,false,"2")
        val item4 = MockItem(3,0,false,"3")
        val item5 = MockItem(4,0,false,"4")
        val item6 = MockItem(5,0,false,"5")
        testSubject.insertOrReplace(item1)
        testSubject.insertOrReplace(item2)
        testSubject.insertOrReplace(item3)
        testSubject.insertOrReplace(item4)
        testSubject.insertOrReplace(item5)
        testSubject.insertOrReplace(item6)
        check(testSubjectAdapter.select(item1) == item1.copy(updateSequence = Long.MIN_VALUE+0))
        check(testSubjectAdapter.select(item2) == item2.copy(updateSequence = Long.MIN_VALUE+1))
        check(testSubjectAdapter.select(item3) == item3.copy(updateSequence = Long.MIN_VALUE+2))
        check(testSubjectAdapter.select(item4) == item4.copy(updateSequence = Long.MIN_VALUE+3))
        check(testSubjectAdapter.select(item5) == item5.copy(updateSequence = Long.MIN_VALUE+4))
        check(testSubjectAdapter.select(item6) == item6.copy(updateSequence = Long.MIN_VALUE+5))

        // delete some stuff
        run {
            testSubject.delete(MockItem(1,0,false,""))
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.select(item1) == item1.copy(updateSequence = Long.MIN_VALUE+0))
            check(testSubjectAdapter.select(item2) == item2.copy(updateSequence = Long.MIN_VALUE+6,isDeleted = true))
            check(testSubjectAdapter.select(item3) == item3.copy(updateSequence = Long.MIN_VALUE+2))
            check(testSubjectAdapter.select(item4) == item4.copy(updateSequence = Long.MIN_VALUE+3))
            check(testSubjectAdapter.select(item5) == item5.copy(updateSequence = Long.MIN_VALUE+4))
            check(testSubjectAdapter.select(item6) == item6.copy(updateSequence = Long.MIN_VALUE+5))
        }

        // delete some stuff
        run {
            testSubject.delete(item3)
            testSubject.delete(item4)
            testSubject.delete(item5)
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.select(item1) == item1.copy(updateSequence = Long.MIN_VALUE+0))
            check(testSubjectAdapter.select(item2) == null)
            check(testSubjectAdapter.select(item3) == item3.copy(updateSequence = Long.MIN_VALUE+7,isDeleted = true))
            check(testSubjectAdapter.select(item4) == item4.copy(updateSequence = Long.MIN_VALUE+8,isDeleted = true))
            check(testSubjectAdapter.select(item5) == item5.copy(updateSequence = Long.MIN_VALUE+9,isDeleted = true))
            check(testSubjectAdapter.select(item6) == item6.copy(updateSequence = Long.MIN_VALUE+5))
        }

        // delete some stuff
        run {
            testSubject.delete(item2)
            testSubject.delete(item3)
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.select(item1) == item1.copy(updateSequence = Long.MIN_VALUE+0))
            check(testSubjectAdapter.select(item2) == null)
            check(testSubjectAdapter.select(item3) == item3.copy(updateSequence = Long.MIN_VALUE+10,isDeleted = true))
            check(testSubjectAdapter.select(item4) == item4.copy(updateSequence = Long.MIN_VALUE+8,isDeleted = true))
            check(testSubjectAdapter.select(item5) == item5.copy(updateSequence = Long.MIN_VALUE+9,isDeleted = true))
            check(testSubjectAdapter.select(item6) == item6.copy(updateSequence = Long.MIN_VALUE+5))
        }
    }
}
