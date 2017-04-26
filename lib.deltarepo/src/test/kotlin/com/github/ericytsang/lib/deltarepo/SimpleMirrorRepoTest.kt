package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class SimpleMirrorRepoTest
{
    private val testSubjectAdapter = MockMirrorRepoAdapter()
    private val testSubject = SimpleMirrorRepo(testSubjectAdapter)

    private val item1 = MockItem(testSubject.write {testSubject.computeNextPk()},null,null,false,false)
    private val item2 = MockItem(testSubject.write {testSubject.computeNextPk()},null,1,false,false)
    private val item3 = MockItem(testSubject.write {testSubject.computeNextPk()},1,null,false,false)
    private val item4 = MockItem(testSubject.write {testSubject.computeNextPk()},2,2,true,true)
    private val item5 = MockItem(testSubject.write {testSubject.computeNextPk()},4,5,false,true)
    private val item6 = MockItem(testSubject.write {testSubject.computeNextPk()},5,4,true,false)

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
            mirror1.insertOrReplace(MockItem(pks[0],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[1],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[2],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[3],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[4],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[5],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[6],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[7],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[8],null,null,false,false))
            mirror1.insertOrReplace(MockItem(pks[9],null,null,false,false))
        }

        // inter repo ids
        val mirror1Id = DeltaRepoPk(0)
        val masterId = DeltaRepoPk(1)
        val mirror2Id = DeltaRepoPk(2)

        // sync records to master & mirror2
        run {
            // get unsynced from mirror and mark as synced
            val unsynced = mirror1.read {
                mirror1.selectNextUnsyncedToSync(12)
            }
            mirror1.write {
                unsynced.map {it.copy(Unit,isSynced = true)}.forEach {mirror1.markAsSynced(it.pk)}
            }

            // merge into master
            master.write {
                master.merge(unsynced.toSet(),masterId,mirror1Id)
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror2.write {
                    mirror2.synchronizeWith(master,mirror2Id,masterId)
                }
            }
        }

        // check records are in all repos
        run {
            check(mirror2Adapter.records.keys.containsAll(pks.map {it.copy(Unit,nodePk = mirror1Id)}))
            check(mirror2Adapter.records.values.all {it.isSynced})
            check(mirror2Adapter.records.values.all {!it.isDeleted})
            check(mirror2Adapter.records.values.all {it.updateStamp != null})
            check(mirror2Adapter.records.values.all {it.deleteStamp != null})
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
            // get unsynced from mirror and mark as synced
            val unsynced = mirror2.read {
                mirror2.selectNextUnsyncedToSync(12)
            }
            mirror2.write {
                unsynced.map {it.copy(Unit,isSynced = true)}.forEach {mirror2.markAsSynced(it.pk)}
            }

            // merge into master
            master.write {
                master.merge(unsynced.toSet(),masterId,mirror1Id)
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.synchronizeWith(master,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.synchronizeWith(master,mirror2Id,masterId)
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
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
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
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
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
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
        }

        run {
            val item = item4
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            testSubject.read {
                check(testSubject.selectByPk(item.pk) == null)
            }
        }

        run {
            val item = item5
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            testSubject.read {
                check(testSubject.selectByPk(item.pk) == null)
            }
        }

        run {
            val item = item6
            testSubject.write {
                testSubject.insertOrReplace(item)
            }
            val inserted = testSubject.read {
                testSubject.selectByPk(item.pk)
                    ?: throw RuntimeException("insert failed")
            }
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
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
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
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
            check(inserted.copy(isSynced = item.isSynced) == item)
            check(!inserted.isSynced)
        }
    }

    @Test
    fun deleteByPkTest()
    {
        // cannot page for deleted records
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
            check(selected[item1.pk] == item1.copy(isSynced = false))
            check(selected[item2.pk] == item2.copy(isSynced = false))
            check(selected[item3.pk] == item3.copy(isSynced = false))

            // even deleted items should be selected here
            check(selected[item5.pk] == item5.copy(isSynced = false))

            // this item is marked as synced and should not be here
            check(selected[item4.pk] == null)
            check(selected[item6.pk] == null)
        }
    }
}
