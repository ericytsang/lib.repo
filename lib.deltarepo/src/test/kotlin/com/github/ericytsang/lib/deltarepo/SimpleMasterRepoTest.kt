package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.testutils.TestUtils
import org.junit.Test

class SimpleMasterRepoTest
{
    private val testSubjectAdapter = MockMasterRepoAdapter()
    private val testSubject = SimpleMasterRepo(testSubjectAdapter)

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

    @Test
    fun deleteByPkTest()
    {
        // insert records that will be deleted
        testSubject.write {
            testSubject.insertOrReplace(item1.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
            testSubject.insertOrReplace(item2.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
            testSubject.insertOrReplace(item3.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
            testSubject.insertOrReplace(item4.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
            testSubject.insertOrReplace(item5.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
            testSubject.insertOrReplace(item6.copy(updateStamp = null,deleteStamp = null,isDeleted = false))
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) != null)
            check(testSubject.selectByPk(item2.pk) != null)
            check(testSubject.selectByPk(item3.pk) != null)
            check(testSubject.selectByPk(item4.pk) != null)
            check(testSubject.selectByPk(item5.pk) != null)
            check(testSubject.selectByPk(item6.pk) != null)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item2.pk))
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) != null)
            check(testSubject.selectByPk(item2.pk) == null)
            check(testSubject.selectByPk(item3.pk) != null)
            check(testSubject.selectByPk(item4.pk) != null)
            check(testSubject.selectByPk(item5.pk) != null)
            check(testSubject.selectByPk(item6.pk) != null)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item3.pk,item4.pk,item5.pk))
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) != null)
            check(testSubject.selectByPk(item2.pk) == null)
            check(testSubject.selectByPk(item3.pk) == null)
            check(testSubject.selectByPk(item4.pk) == null)
            check(testSubject.selectByPk(item5.pk) == null)
            check(testSubject.selectByPk(item6.pk) != null)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item2.pk,item3.pk))
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) != null)
            check(testSubject.selectByPk(item2.pk) == null)
            check(testSubject.selectByPk(item3.pk) == null)
            check(testSubject.selectByPk(item4.pk) == null)
            check(testSubject.selectByPk(item5.pk) == null)
            check(testSubject.selectByPk(item6.pk) != null)
        }
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

    fun prepareTestSubjectForPageTests()
    {
        // insert records that will be deleted
        testSubject.write {
            testSubjectAdapter.records[item4.pk] = item4.copy(isDeleted = false)
            testSubjectAdapter.records[item5.pk] = item5.copy(isDeleted = false)
            testSubjectAdapter.records[item6.pk] = item6
        }

        // check state of test subject
        testSubject.read {
            check(testSubject.selectByPk(item1.pk) == null)
            check(testSubject.selectByPk(item2.pk) == null)
            check(testSubject.selectByPk(item3.pk) == null)
            check(testSubject.selectByPk(item4.pk) != null)
            check(testSubject.selectByPk(item5.pk) != null)
            check(testSubject.selectByPk(item6.pk) != null)
        }
    }

    @Test
    fun selectRecordByPkTest()
    {
        prepareTestSubjectForPageTests()
    }

    @Test
    fun pageByUpdateStampTest()
    {
        prepareTestSubjectForPageTests()

        testSubject.read {
            // -.->..
            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,1).map {it.pk} == listOf(item4.pk))
            // -.-.-.->
            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,10).map {it.pk} == listOf(item4.pk,item5.pk,item6.pk))
            // ..-.->
            check(testSubject.pageByUpdateStamp(5,Order.ASC,10).map {it.pk} == listOf(item6.pk))
            check(testSubject.pageByUpdateStamp(5,Order.ASC,1).map {it.pk} == listOf(item6.pk))
            // ...->
            check(testSubject.pageByUpdateStamp(6,Order.ASC,10).isEmpty())
            // ..<-.-
            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1).map {it.pk} == listOf(item6.pk))
            // <-.-.-.-
            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,10).map {it.pk} == listOf(item6.pk,item5.pk,item4.pk))
            // <-.-..
            check(testSubject.pageByUpdateStamp(2,Order.DESC,10).map {it.pk} == listOf(item4.pk))
            check(testSubject.pageByUpdateStamp(3,Order.DESC,10).map {it.pk} == listOf(item4.pk))
            // <-...
            check(testSubject.pageByUpdateStamp(0,Order.DESC,10).isEmpty())
        }
    }

    @Test
    fun pageByDeleteStampTest()
    {
        prepareTestSubjectForPageTests()

        testSubject.read {
            // -.->..
            check(testSubject.pageByDeleteStamp(Long.MIN_VALUE,Order.ASC,1).map {it.pk} == listOf(item4.pk))
            // -.-.-.->
            check(testSubject.pageByDeleteStamp(Long.MIN_VALUE,Order.ASC,10).map {it.pk} == listOf(item4.pk,item6.pk,item5.pk))
            // ..-.->
            check(testSubject.pageByDeleteStamp(5,Order.ASC,10).map {it.pk} == listOf(item5.pk))
            check(testSubject.pageByDeleteStamp(5,Order.ASC,1).map {it.pk} == listOf(item5.pk))
            // ...->
            check(testSubject.pageByDeleteStamp(6,Order.ASC,10).isEmpty())
            // ..<-.-
            check(testSubject.pageByDeleteStamp(Long.MAX_VALUE,Order.DESC,1).map {it.pk} == listOf(item5.pk))
            // <-.-.-.-
            check(testSubject.pageByDeleteStamp(Long.MAX_VALUE,Order.DESC,10).map {it.pk} == listOf(item5.pk,item6.pk,item4.pk))
            // <-.-..
            check(testSubject.pageByDeleteStamp(2,Order.DESC,10).map {it.pk} == listOf(item4.pk))
            check(testSubject.pageByDeleteStamp(3,Order.DESC,10).map {it.pk} == listOf(item4.pk))
            // <-...
            check(testSubject.pageByDeleteStamp(0,Order.DESC,10).isEmpty())
        }
    }
}
