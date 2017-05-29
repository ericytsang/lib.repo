package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class SimpleMirrorRepoTest
{
    private val testSubjectAdapter = MockMirrorRepoAdapter()
    private val testSubject = SimpleMirrorRepo(testSubjectAdapter)

    private val item1 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},null,DeltaRepo.Item.SyncStatus.DIRTY,false,"item1")
    private val item2 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},null,DeltaRepo.Item.SyncStatus.DIRTY,false,"item2")
    private val item3 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},1,DeltaRepo.Item.SyncStatus.DIRTY,false,"item3")
    private val item4 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},2,DeltaRepo.Item.SyncStatus.PULLED,true,"item4")
    private val item5 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},4,DeltaRepo.Item.SyncStatus.DIRTY,true,"item5")
    private val item6 = MockItem(run {MockItem.Pk(testSubject.computeNextPk())},5,DeltaRepo.Item.SyncStatus.PULLED,false,"item6")

    @Test
    fun insertTest()
    {
        run {
            val item = item1
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.selectByPk(item.pk.value)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item2
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.selectByPk(item.pk.value)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE+1)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item3
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.selectByPk(item.pk.value)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE+2)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item4
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            run {
                check(testSubjectAdapter.selectByPk(item.pk.value)?.isDeleted == true)
            }
        }

        run {
            val item = item5
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            run {
                check(testSubjectAdapter.selectByPk(item.pk.value)?.isDeleted == true)
            }
        }

        run {
            val item = item6
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.selectByPk(item.pk.value)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE+5)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }
    }

    @Test
    fun replaceTest()
    {
        run {
            val item = item2
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.selectByPk(item.pk.value)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
            check(!inserted.isDeleted)
        }

        run {
            val item = item2.copy(isDeleted = true)
            run {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = run {
                testSubjectAdapter.records[item.pk] ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == Long.MIN_VALUE+1)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
            check(inserted.isDeleted)
        }
    }

    @Test
    fun deleteByPkTest()
    {
        // cannot page for deleted records
        // insert records that will be deleted
        run {
            testSubject.insertOrReplace(listOf(
                item1.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item2.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item3.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item4.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item5.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false),
                item6.copy(DeltaRepo.Item.Companion,updateStamp = null,isDeleted = false)))
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.selectByPk(item1.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item2.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item3.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item4.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item5.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item6.pk.value) != null)
        }

        // delete some stuff
        run {
            testSubject.deleteByPk(setOf(item2.pk.value))
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.selectByPk(item1.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item2.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item3.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item4.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item5.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item6.pk.value) != null)
        }

        // delete some stuff
        run {
            testSubject.deleteByPk(setOf(item3.pk.value))
            testSubject.deleteByPk(setOf(item4.pk.value))
            testSubject.deleteByPk(setOf(item5.pk.value))
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.selectByPk(item1.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item2.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item3.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item4.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item5.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item6.pk.value) != null)
        }

        // delete some stuff
        run {
            testSubject.deleteByPk(setOf(item2.pk.value))
            testSubject.deleteByPk(setOf(item3.pk.value))
        }

        // check state of test subject
        run {
            check(testSubjectAdapter.selectByPk(item1.pk.value) != null)
            check(testSubjectAdapter.selectByPk(item2.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item3.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item4.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item5.pk.value)?.isDeleted == true)
            check(testSubjectAdapter.selectByPk(item6.pk.value) != null)
        }
    }

    @Test
    fun computeNextPkTest()
    {
        val pks = run {
            (0..4).map {testSubject.computeNextPk()}
        }
        run {
            check(pks.all {it.repoPk == DeltaRepo.LOCAL_NODE_ID}) {pks}
            check(pks.map {it.itemPk.id} == (pks.first().itemPk.id..pks.last().itemPk.id).toList()) {pks}
            check(pks.first().itemPk.id < pks.last().itemPk.id)
            check(pks[0].copy() == pks[0])
            check(pks[0].copy() !== pks[0])
        }
    }
}
