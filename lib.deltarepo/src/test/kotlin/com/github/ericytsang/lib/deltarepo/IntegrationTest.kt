package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class IntegrationTest
{

    // create test subjects
    val mirror1Adapter = MockMirrorRepoAdapter()
    val mirror1 = SimpleMirrorRepo(object:SimpleMirrorRepo.Adapter<MockItem> by mirror1Adapter
    {
        override fun deleteAllPushed()
        {
            mirror1Adapter.deleteAllPushed()
        }
    })
    val mirror2Adapter = MockMirrorRepoAdapter()
    val mirror2 = SimpleMirrorRepo(object:SimpleMirrorRepo.Adapter<MockItem> by mirror2Adapter
    {
        override fun deleteAllPushed()
        {
            mirror2Adapter.deleteAllPushed()
        }
    })
    val masterAdapter = MockMasterRepoAdapter()
    val master = SimpleMasterRepo(masterAdapter)

    // create pks
    val pks = (1..10).map {MockItem.Pk(mirror1.computeNextPk())}

    // inter repo ids
    val mirror1Id = DeltaRepo.RepoPk(0)
    val masterId = DeltaRepo.RepoPk(1)
    val mirror2Id = DeltaRepo.RepoPk(2)

    @Test
    fun insertTest()
    {
        // insert records into mirror1
        mirror1.insertOrReplace(listOf(
            MockItem(pks[0],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[0]"),
            MockItem(pks[1],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[1]"),
            MockItem(pks[2],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[2]"),
            MockItem(pks[3],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[3]"),
            MockItem(pks[4],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[4]"),
            MockItem(pks[5],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[5]"),
            MockItem(pks[6],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[6]"),
            MockItem(pks[7],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[7]"),
            MockItem(pks[8],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[8]"),
            MockItem(pks[9],null,DeltaRepo.Item.SyncStatus.DIRTY,false,"pks[9]")))

        // sync records to master & mirror2
        run {
            // merge into master
            mirror1.pusher.pushAll(master.pushTarget,mirror1Id,masterId)

            // pull from master into mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are in all repos
        run {
            check(mirror2Adapter.records.keys.map {it.value}.containsAll(pks.map {it.value.copy(repoPk = mirror1Id)}))
            check(mirror2Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED})
            check(mirror1Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED})
            check(mirror2Adapter.records.values.all {!it.isDeleted})
            check(mirror1Adapter.records.values.all {!it.isDeleted})
            check(mirror2Adapter.records.values.all {it.updateStamp != null})
            check(mirror1Adapter.records.values.all {it.updateStamp != null})
            check(mirror2Adapter.records.values.all {it.string.count {it == 'p'} == 1})
            check(mirror1Adapter.records.values.all {it.string.count {it == 'p'} == 2})
        }
    }

    @Test
    fun delete2RecordsTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            mirror2.deleteByPk(setOf(pks[5].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[6].value.copy(repoPk = mirror1Id)))
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            mirror2.pusher.pushAll(master.pushTarget,mirror2Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are deleted in all repos and merging is as expected
        check(mirror1Adapter.selectByPk(pks[5].value) == null)
        check(mirror1Adapter.selectByPk(pks[6].value) == null)
        // (up to 3 (number is defined by adapter) deleted records are kept on master repo)
        check(masterAdapter.selectByPk(pks[5].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(masterAdapter.selectByPk(pks[6].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(mirror2Adapter.selectByPk(pks[5].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[6].value.copy(repoPk = mirror1Id)) == null)
    }

    @Test
    fun delete4RecordsTest()
    {
        insertTest()

        // delete more records from mirror2 such that mirror1 will need to resync
        run {
            mirror2.deleteByPk(setOf(pks[0].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[1].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[2].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[3].value.copy(repoPk = mirror1Id)))
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            mirror2.pusher.pushAll(master.pushTarget,mirror2Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are deleted in all repos and merging is as expected
        check(mirror1Adapter.selectByPk(pks[0].value) == null)
        check(mirror1Adapter.selectByPk(pks[1].value) == null)
        check(mirror1Adapter.selectByPk(pks[2].value) == null)
        check(mirror1Adapter.selectByPk(pks[3].value) == null)
        // (up to 3 (number is defined by adapter) deleted records are kept on master repo)
        check(masterAdapter.selectByPk(pks[0].value.copy(repoPk = mirror1Id)) == null)
        check(masterAdapter.selectByPk(pks[1].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(masterAdapter.selectByPk(pks[2].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(masterAdapter.selectByPk(pks[3].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(mirror2Adapter.selectByPk(pks[0].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[1].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[2].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[3].value.copy(repoPk = mirror1Id)) == null)
    }

    @Test
    fun pushedRecordsDeletedBeforePullTest()
    {
        insertTest()

        // delete more records from mirror2 such that mirror1 will need to resync
        run {
            mirror2.deleteByPk(setOf(pks[0].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[1].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[2].value.copy(repoPk = mirror1Id)))
            mirror2.deleteByPk(setOf(pks[3].value.copy(repoPk = mirror1Id)))
            val item = mirror1Adapter.selectByPk(pks[0].value)!!
            mirror1.insertOrReplace(setOf(item.copy(string = "pee")))
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            mirror1.pusher.pushAll(master.pushTarget,mirror1Id,masterId)
            mirror2.pusher.pushAll(master.pushTarget,mirror2Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are deleted in all repos and merging is as expected
        check(mirror1Adapter.selectByPk(pks[0].value) == null)
        check(mirror1Adapter.selectByPk(pks[1].value) == null)
        check(mirror1Adapter.selectByPk(pks[2].value) == null)
        check(mirror1Adapter.selectByPk(pks[3].value) == null)
        // (up to 3 (number is defined by adapter) deleted records are kept on master repo)
        check(masterAdapter.selectByPk(pks[0].value.copy(repoPk = mirror1Id)) == null)
        check(masterAdapter.selectByPk(pks[1].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(masterAdapter.selectByPk(pks[2].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(masterAdapter.selectByPk(pks[3].value.copy(repoPk = mirror1Id))?.isDeleted == true)
        check(mirror2Adapter.selectByPk(pks[0].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[1].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[2].value.copy(repoPk = mirror1Id)) == null)
        check(mirror2Adapter.selectByPk(pks[3].value.copy(repoPk = mirror1Id)) == null)
    }

    @Test
    fun mergeMirrorIntoMasterTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            val item = mirror2Adapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))!!
            mirror2.insertOrReplace(setOf(item.copy(string = "pee")))
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            mirror2.pusher.pushAll(master.pushTarget,mirror2Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are deleted in all repos and merging is as expected
        check(mirror1Adapter.selectByPk(pks[7].value)?.string == "pks[7]pks[7]pks[7]pee") {mirror1Adapter.selectByPk(pks[7].value)?.string ?: ""}
        check(masterAdapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "pks[7]pee")
        check(mirror2Adapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "peepks[7]pee")
    }

    @Test
    fun mergeMasterIntoMirrorTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            val item = mirror2Adapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))!!
            mirror2.insertOrReplace(setOf(item.copy(string = "pee")))
        }
        run {
            val item = mirror1Adapter.selectByPk(pks[7].value)!!
            mirror1.insertOrReplace(setOf(item.copy(string = "poo")))
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            mirror2.pusher.pushAll(master.pushTarget,mirror2Id,masterId)
            mirror1.pusher.pushAll(master.pushTarget,mirror1Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check records are deleted in all repos and merging is as expected
        check(mirror1Adapter.selectByPk(pks[7].value)?.string == "poopks[7]peepoo")
        check(masterAdapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "pks[7]peepoo")
        check(mirror2Adapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "peepks[7]peepoo")

        // sync mirror1 records to master & mirrors
        run {
            // merge into master
            mirror1.pusher.pushAll(master.pushTarget,mirror1Id,masterId)

            // pull from master into mirror1 & mirror2
            mirror1.puller.pullAll(master.pullTarget,mirror1Id,masterId)
            mirror2.puller.pullAll(master.pullTarget,mirror2Id,masterId)
        }

        // check that merging is as expected
        check(mirror1Adapter.selectByPk(pks[7].value)?.string == "poopks[7]peepoo") {mirror1Adapter.selectByPk(pks[7].value)?.string!!}
        check(masterAdapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "pks[7]peepoo")
        check(mirror2Adapter.selectByPk(pks[7].value.copy(repoPk = mirror1Id))?.string == "peepks[7]peepoo")
    }
}
