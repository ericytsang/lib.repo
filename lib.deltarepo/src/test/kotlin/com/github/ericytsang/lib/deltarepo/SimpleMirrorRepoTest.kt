package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class SimpleMirrorRepoTest
{
    private val testSubjectAdapter = MockMirrorRepoAdapter()
    private val testSubject = SimpleMirrorRepo(testSubjectAdapter)

    private val item1 = MockItem(testSubject.write {testSubject.computeNextPk()},null,DeltaRepo.Item.SyncStatus.DIRTY,false)
    private val item2 = MockItem(testSubject.write {testSubject.computeNextPk()},null,DeltaRepo.Item.SyncStatus.DIRTY,false)
    private val item3 = MockItem(testSubject.write {testSubject.computeNextPk()},1,DeltaRepo.Item.SyncStatus.DIRTY,false)
    private val item4 = MockItem(testSubject.write {testSubject.computeNextPk()},2,DeltaRepo.Item.SyncStatus.PULLED,true)
    private val item5 = MockItem(testSubject.write {testSubject.computeNextPk()},4,DeltaRepo.Item.SyncStatus.DIRTY,true)
    private val item6 = MockItem(testSubject.write {testSubject.computeNextPk()},5,DeltaRepo.Item.SyncStatus.PULLED,false)

    @Test
    fun synchronizeWithTest()
    {
        // create test subjects
        val mirror1Adapter = MockMirrorRepoAdapter()
        val mirror1 = SimpleMirrorRepo(mirror1Adapter)
        val mirror2Adapter = MockMirrorRepoAdapter()
        val mirror2 = SimpleMirrorRepo(mirror2Adapter)
        val masterAdapter = MockMasterRepoAdapter()
        val master = SimpleMasterRepo(masterAdapter)

        // create pks
        val pks = (1..10).map {mirror1.write {mirror1.computeNextPk()}}

        // insert records into mirror1
        mirror1.write {
            mirror1.insertOrReplace(listOf(
                MockItem(pks[0],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[1],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[2],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[3],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[4],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[5],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[6],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[7],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[8],null,DeltaRepo.Item.SyncStatus.DIRTY,false),
                MockItem(pks[9],null,DeltaRepo.Item.SyncStatus.DIRTY,false)))
        }

        // inter repo ids
        val mirror1Id = DeltaRepoPk(0)
        val masterId = DeltaRepoPk(1)
        val mirror2Id = DeltaRepoPk(2)

        // sync records to master & mirror2
        run {
            // merge into master
            master.write {
                mirror1.write {
                    mirror1.push(master,mirror1Id,masterId)
                }
            }

            // pull from master into mirror2
            master.read {
                mirror2.write {
                    mirror2.pull(master,mirror2Id,masterId)
                }
            }
        }

        // check records are in all repos
        run {
            check(mirror2Adapter.records.keys.containsAll(pks.map {it.copy(Unit,nodePk = mirror1Id)}))
            check(mirror2Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED})
            check(mirror1Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PUSHED})
            check(mirror2Adapter.records.values.all {!it.isDeleted})
            check(mirror1Adapter.records.values.all {!it.isDeleted})
            check(mirror2Adapter.records.values.all {it.updateStamp != null})
            check(mirror1Adapter.records.values.all {it.updateStamp != null})
        }

        // delete records from mirror2
        run {
            mirror2.write {
                mirror2.deleteByPk(pks[5].copy(Unit,nodePk = mirror1Id))
                mirror2.deleteByPk(pks[6].copy(Unit,nodePk = mirror1Id))
            }
        }

        // sync records to master & mirror1
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.push(master,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.pull(master,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.pull(master,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos
        mirror1.read {
            check(mirror1.selectByPk(pks[5]) == null)
            check(mirror1.selectByPk(pks[6]) == null)
        }
        master.read {
            check(master.selectByPk(pks[5].copy(Unit,nodePk = mirror1Id)) == null)
            check(master.selectByPk(pks[6].copy(Unit,nodePk = mirror1Id)) == null)
        }
        mirror2.read {
            check(mirror2.selectByPk(pks[5].copy(Unit,nodePk = mirror1Id)) == null)
            check(mirror2.selectByPk(pks[6].copy(Unit,nodePk = mirror1Id)) == null)
        }
    }

    @Test
    fun insertTest()
    {
        run {
            val item = item1
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == null)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == null)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item3
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == null)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }

        run {
            val item = item4
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            testSubject.read {
                check(testSubject.selectByPk(item.pk) == null)
            }
        }

        run {
            val item = item5
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            testSubject.read {
                check(testSubject.selectByPk(item.pk) == null)
            }
        }

        run {
            val item = item6
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(updateStamp = item.updateStamp,syncStatus = item.syncStatus) == item)
            check(inserted.updateStamp == null)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
        }
    }

    @Test
    fun replaceTest()
    {
        run {
            val item = item2
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(syncStatus = item.syncStatus) == item)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
            check(!inserted.isDeleted)
        }

        run {
            val item = item2.copy(isDeleted = true)
            testSubject.write {
                testSubject.insertOrReplace(listOf(item))
            }
            val inserted = testSubject.read {
                testSubjectAdapter.records[item.pk] ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(syncStatus = item.syncStatus) == item)
            check(inserted.syncStatus == DeltaRepo.Item.SyncStatus.DIRTY)
            check(inserted.isDeleted)
        }
    }

    @Test
    fun deleteByPkTest()
    {
        // cannot page for deleted records
        // insert records that will be deleted
        testSubject.write {
            testSubject.insertOrReplace(listOf(
                item1.copy(Unit,updateStamp = null,isDeleted = false),
                item2.copy(Unit,updateStamp = null,isDeleted = false),
                item3.copy(Unit,updateStamp = null,isDeleted = false),
                item4.copy(Unit,updateStamp = null,isDeleted = false),
                item5.copy(Unit,updateStamp = null,isDeleted = false),
                item6.copy(Unit,updateStamp = null,isDeleted = false)))
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
            testSubject.deleteByPk(item2.pk)
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
            testSubject.deleteByPk(item3.pk)
            testSubject.deleteByPk(item4.pk)
            testSubject.deleteByPk(item5.pk)
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
            testSubject.deleteByPk(item2.pk)
            testSubject.deleteByPk(item3.pk)
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
            check(pks.all {it.nodePk == DeltaRepo.LOCAL_NODE_ID}) {pks}
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
    fun selectByPkTest()
    {
        prepareTestSubjectForPageTests()
    }

    @Test
    fun pageByUpdateStampTest()
    {
        prepareTestSubjectForPageTests()

        val allStatus = DeltaRepo.Item.SyncStatus.values().toSet()

        testSubject.read {
            // -.->..
            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,1,allStatus).map {it.pk} == listOf(item4.pk))
            // -.-.-.->
            check(testSubject.pageByUpdateStamp(Long.MIN_VALUE,Order.ASC,10,allStatus).map {it.pk} == listOf(item4.pk,item5.pk,item6.pk))
            // ..-.->
            check(testSubject.pageByUpdateStamp(5,Order.ASC,10,allStatus).map {it.pk} == listOf(item6.pk))
            check(testSubject.pageByUpdateStamp(5,Order.ASC,1,allStatus).map {it.pk} == listOf(item6.pk))
            // ...->
            check(testSubject.pageByUpdateStamp(6,Order.ASC,10,allStatus).isEmpty())
            // ..<-.-
            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,1,allStatus).map {it.pk} == listOf(item6.pk))
            // <-.-.-.-
            check(testSubject.pageByUpdateStamp(Long.MAX_VALUE,Order.DESC,10,allStatus).map {it.pk} == listOf(item6.pk,item5.pk,item4.pk))
            // <-.-..
            check(testSubject.pageByUpdateStamp(2,Order.DESC,10,allStatus).map {it.pk} == listOf(item4.pk))
            check(testSubject.pageByUpdateStamp(3,Order.DESC,10,allStatus).map {it.pk} == listOf(item4.pk))
            // <-...
            check(testSubject.pageByUpdateStamp(0,Order.DESC,10,allStatus).isEmpty())
        }
    }

    @Test
    fun selectNextUnsyncedToSyncTest()
    {
        // insert test items
        testSubjectAdapter.records += item1.let {it.pk to it}
        testSubjectAdapter.records += item2.let {it.pk to it}
        testSubjectAdapter.records += item3.let {it.pk to it}
        testSubjectAdapter.records += item4.let {it.pk to it}
        testSubjectAdapter.records += item5.let {it.pk to it}
        testSubjectAdapter.records += item6.let {it.pk to it}

        testSubject.read {
            // must respect limit
            check(testSubject.selectNextUnsyncedToSync(1).size == 1)

            // even deleted items should be selected here
            val selected = testSubject.selectNextUnsyncedToSync(10).associate {it.pk to it}

            // regular unsynced items should be here
            check(selected[item1.pk] == item1.copy(syncStatus = DeltaRepo.Item.SyncStatus.DIRTY))
            check(selected[item2.pk] == item2.copy(syncStatus = DeltaRepo.Item.SyncStatus.DIRTY))
            check(selected[item3.pk] == item3.copy(syncStatus = DeltaRepo.Item.SyncStatus.DIRTY))

            // even deleted items should be selected here
            check(selected[item5.pk] == item5.copy(syncStatus = DeltaRepo.Item.SyncStatus.DIRTY))

            // this item is marked as synced and should not be here
            check(selected[item4.pk] == null)
            check(selected[item6.pk] == null)
        }
    }
}
