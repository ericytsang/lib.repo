package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.testutils.TestUtils
import org.junit.Test

class SimpleMasterRepoTest
{
    private val testSubjectAdapter = MockMasterRepoAdapter()
    private val testSubject = SimpleMasterRepo(testSubjectAdapter)

    private val item1 = MockItem(testSubject.write {testSubject.computeNextPk()},null,DeltaRepo.Item.SyncStatus.DIRTY,false,"item1")
    private val item2 = MockItem(testSubject.write {testSubject.computeNextPk()},null,DeltaRepo.Item.SyncStatus.DIRTY,false,"item2")
    private val item3 = MockItem(testSubject.write {testSubject.computeNextPk()},1,DeltaRepo.Item.SyncStatus.DIRTY,false,"item3")
    private val item4 = MockItem(testSubject.write {testSubject.computeNextPk()},2,DeltaRepo.Item.SyncStatus.PULLED,true,"item4")
    private val item5 = MockItem(testSubject.write {testSubject.computeNextPk()},4,DeltaRepo.Item.SyncStatus.DIRTY,true,"item5")
    private val item6 = MockItem(testSubject.write {testSubject.computeNextPk()},5,DeltaRepo.Item.SyncStatus.PULLED,false,"item6")

    @Test
    fun insertTest()
    {
        run {
            val item = item1
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
        }

        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
        }

        run {
            val item = item3
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
        }

        // should throw exception because isDelete is true
        TestUtils.exceptionExpected {testSubject.insertOrReplace(listOf(item4))}
        TestUtils.exceptionExpected {testSubject.insertOrReplace(listOf(item5))}

        run {
            val item = item6
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
        }
    }

    @Test
    fun replaceTest()
    {
        val oldUpdateStamp = run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            inserted.updateStamp
        }

        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.pk == item.pk)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.PULLED)
            check(!inserted.isDeleted)
            check(inserted.updateStamp != null)
            check(inserted.updateStamp != item.updateStamp)
            check(inserted.updateStamp != oldUpdateStamp)
        }
    }

    @Test
    fun insertOrReplaceTest()
    {
        // insert records that will be deleted
        testSubject.write {
            testSubject.insertOrReplace(listOf(
                item4.copy(isDeleted = false),
                item5.copy(isDeleted = false)))
        }

        // insert regular records
        testSubject.write {
            testSubject.insertOrReplace(listOf(item1,item2))
        }

        // merge all records
        testSubject.write {
            testSubject.insertOrReplace(setOf(item1,item2,item3,item4,item5,item6))
        }

        // check state of test subject
        testSubject.read {
            check(testSubjectAdapter.selectByPk(item1.pk)?.isDeleted == false) {testSubjectAdapter.records}
            check(testSubjectAdapter.selectByPk(item2.pk)?.isDeleted == false) {testSubjectAdapter.records}
            check(testSubjectAdapter.selectByPk(item3.pk)?.isDeleted == false) {testSubjectAdapter.records}
            check(testSubjectAdapter.selectByPk(item4.pk)?.isDeleted == true) {testSubjectAdapter.records}
            check(testSubjectAdapter.selectByPk(item5.pk)?.isDeleted == true) {testSubjectAdapter.records}
            check(testSubjectAdapter.selectByPk(item6.pk)?.isDeleted == false) {testSubjectAdapter.records}
        }
    }

    @Test
    fun deleteByPkTest()
    {
        // insert records that will be deleted
        testSubject.write {
            testSubject.insertOrReplace(listOf(
                item1.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item2.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item3.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item4.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item5.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item6.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false)))
        }

        // check state of test subject
        testSubject.read {
            check(testSubjectAdapter.selectByPk(item1.pk) != null)
            check(testSubjectAdapter.selectByPk(item2.pk) != null)
            check(testSubjectAdapter.selectByPk(item3.pk) != null)
            check(testSubjectAdapter.selectByPk(item4.pk) != null)
            check(testSubjectAdapter.selectByPk(item5.pk) != null)
            check(testSubjectAdapter.selectByPk(item6.pk) != null)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item2.pk))
        }

        // check state of test subject
        testSubject.read {

            // true because this was deleted
            check(testSubjectAdapter.selectByPk(item2.pk)?.isDeleted == true)

            // false because not deleted
            check(testSubjectAdapter.selectByPk(item1.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item3.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item4.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item5.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item6.pk)?.isDeleted == false)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item3.pk,item4.pk,item5.pk))
        }

        // check state of test subject
        testSubject.read {

            // null because this was deleted earlier and there is only allowed max n deleted entries
            check(testSubjectAdapter.selectByPk(item2.pk) == null)

            // true because this was deleted
            check(testSubjectAdapter.selectByPk(item3.pk)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item4.pk)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item5.pk)?.isDeleted == true)

            // false because not deleted
            check(testSubjectAdapter.selectByPk(item1.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item6.pk)?.isDeleted == false)
        }

        // delete some stuff
        testSubject.write {
            testSubject.deleteByPk(setOf(item2.pk,item3.pk))
        }

        // check state of test subject
        testSubject.read {
            check(testSubjectAdapter.selectByPk(item1.pk)?.isDeleted == false)
            check(testSubjectAdapter.selectByPk(item2.pk) == null)
            check(testSubjectAdapter.selectByPk(item3.pk)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item4.pk)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item5.pk)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item6.pk)?.isDeleted == false)
        }
    }

    @Test
    fun computeNextPkTest()
    {
        val pks = testSubject.write {
            (0..4).map {testSubject.computeNextPk()}
        }
        testSubject.read {
            check(pks.all {it.repoPk == DeltaRepo.LOCAL_NODE_ID}) {pks}
            check(pks.map {it.itemPk.id} == (pks.first().itemPk.id..pks.last().itemPk.id).toList()) {pks}
            check(pks.first().itemPk.id < pks.last().itemPk.id)
            check(pks[0].copy() == pks[0])
            check(pks[0].copy() !== pks[0])
        }
    }

//    fun prepareTestSubjectForPageTests()
//    {
//        // insert records that will be deleted
//        testSubject.write {
//            testSubjectAdapter.records[item4.pk] = item4.copy(isDeleted = false)
//            testSubjectAdapter.records[item5.pk] = item5.copy(isDeleted = false)
//            testSubjectAdapter.records[item6.pk] = item6
//        }
//
//        // check state of test subject
//        testSubject.read {
//            check(testSubjectAdapter.selectByPk(item1.pk) == null)
//            check(testSubjectAdapter.selectByPk(item2.pk) == null)
//            check(testSubjectAdapter.selectByPk(item3.pk) == null)
//            check(testSubjectAdapter.selectByPk(item4.pk) != null)
//            check(testSubjectAdapter.selectByPk(item5.pk) != null)
//            check(testSubjectAdapter.selectByPk(item6.pk) != null)
//        }
//    }
//
//    @Test
//    fun selectByPkTest()
//    {
//        prepareTestSubjectForPageTests()
//    }
//
//    @Test
//    fun pageByUpdateStampTest()
//    {
//        prepareTestSubjectForPageTests()
//
//        val allStatus = DeltaRepo.Item.SyncStatus.values().toSet()
//
//        testSubject.read {
//            // -.->..
//            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,1,allStatus).map {it.pk} == listOf(item4.pk))
//            // -.-.-.->
//            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,10,allStatus).map {it.pk} == listOf(item4.pk,item5.pk,item6.pk))
//            // ..-.->
//            check(testSubject.pageByUpdateStamp(5,Order.ASC,10,allStatus).map {it.pk} == listOf(item6.pk))
//            check(testSubject.pageByUpdateStamp(5,Order.ASC,1,allStatus).map {it.pk} == listOf(item6.pk))
//            // ...->
//            check(testSubject.pageByUpdateStamp(6,Order.ASC,10,allStatus).isEmpty())
//            // ..<-.-
//            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,allStatus).map {it.pk} == listOf(item6.pk))
//            // <-.-.-.-
//            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,10,allStatus).map {it.pk} == listOf(item6.pk,item5.pk,item4.pk))
//            // <-.-..
//            check(testSubject.pageByUpdateStamp(2,Order.DESC,10,allStatus).map {it.pk} == listOf(item4.pk))
//            check(testSubject.pageByUpdateStamp(3,Order.DESC,10,allStatus).map {it.pk} == listOf(item4.pk))
//            // <-...
//            check(testSubject.pageByUpdateStamp(0,Order.DESC,10,allStatus).isEmpty())
//        }
//    }
}
