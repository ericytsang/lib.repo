package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.testutils.TestUtils
import org.junit.Test

class SimpleMasterRepoTest
{
    private val testSubject = SimpleMasterRepo(MockMasterRepoAdapter())

    private val item1 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},null,null,false,false)
    private val item2 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},null,1,false,false)
    private val item3 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},1,null,false,false)
    private val item4 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},2,2,true,true)
    private val item5 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},4,5,false,true)
    private val item6 = MockMasterRepoAdapter.MockItem(
        testSubject.write {testSubject.computeNextPk()},5,4,true,false)

    @Test
    fun insertTest()
    {
        run {
            val item = item1
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
        }

        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
            check(inserted.deleteStamp == item.deleteStamp)
        }

        run {
            val item = item3
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
        }

        // should throw exception because isDelete is true
        TestUtils.exceptionExpected {testSubject.insertOrReplace(item4)}
        TestUtils.exceptionExpected {testSubject.insertOrReplace(item5)}

        run {
            val item = item6
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
            check(inserted.deleteStamp == item.deleteStamp)
        }
    }

    @Test
    fun replaceTest()
    {
        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
            check(inserted.deleteStamp == item.deleteStamp)
        }

        run {
            val item = item2.copy(deleteStamp = 100)
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.isSynced)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.deleteStamp != null)
            check(inserted.deleteStamp == item.deleteStamp)
        }
    }

    @Test
    fun mergeTest()
    {
        // insert records that will be deleted
        testSubject.write {
            testSubject.insertOrReplace(item4.copy(isDeleted = false))
            testSubject.insertOrReplace(item5.copy(isDeleted = false))
        }

        // insert regular records
        testSubject.write {
            testSubject.insertOrReplace(item1)
            testSubject.insertOrReplace(item2)
        }

        // merge all records
        testSubject.write {
            testSubject.merge(setOf(item1,item2,item3,item4,item5,item6))
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) != null)
            check(testSubject.selectByPk(item2.pk) != null)
            check(testSubject.selectByPk(item3.pk) != null)
            check(testSubject.selectByPk(item4.pk) == null)
            check(testSubject.selectByPk(item5.pk) == null)
            check(testSubject.selectByPk(item6.pk) != null)
        }
    }

    fun deleteByPkTest()
    {
    }

    @Test
    fun computeNextPkTest()
    {
        val pks = testSubject.write {
            (0..4).map {testSubject.computeNextPk()}
        }
        testSubject.read {
            check(pks.all {it.nodePk.id == testSubject.pk.id}) {pks}
            check(pks.map {it.pk.id} == (pks.first().pk.id..pks.last().pk.id).toList()) {pks}
            check(pks.first().pk.id < pks.last().pk.id)
            check(pks[0].copy() == pks[0])
            check(pks[0].copy() !== pks[0])
        }
    }

    fun selectRecordByPkTest()
    {}

    fun selectFakeRecordByPkTest()
    {}

    fun pageByUpdateStampTest()
    {}

    fun pageByDeleteStampTest()
    {}
}
