package com.github.ericytsang.lib.deltarepo

import org.junit.Test

class IntegrationTest
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

    // inter repo ids
    val mirror1Id = DeltaRepo.RepoPk(0)
    val masterId = DeltaRepo.RepoPk(1)
    val mirror2Id = DeltaRepo.RepoPk(2)

    @Test
    fun insertTest()
    {
        // insert records into mirror1
        mirror1.write {
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
        }

        // sync records to master & mirror2
        run {
            // merge into master
            master.write {
                mirror1.write {
                    mirror1.pusher.push(master.pushTarget,mirror1Id,masterId)
                }
            }

            // pull from master into mirror2
            master.read {
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are in all repos
        run {
            check(mirror2Adapter.records.keys.containsAll(pks.map {it.copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)}))
            check(mirror2Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PULLED})
            check(mirror1Adapter.records.values.all {it.syncStatus == DeltaRepo.Item.SyncStatus.PUSHED})
            check(mirror2Adapter.records.values.all {!it.isDeleted})
            check(mirror1Adapter.records.values.all {!it.isDeleted})
            check(mirror2Adapter.records.values.all {it.updateStamp != null})
            check(mirror1Adapter.records.values.all {it.updateStamp != null})
        }
    }

    @Test
    fun delete2RecordsTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            mirror2.write {
                mirror2.deleteByPk(setOf(pks[5].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[6].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
            }
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.pusher.push(master.pushTarget,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[5]) == null)
            check(mirror1Adapter.selectByPk(pks[6]) == null)
        }
        master.read {
            // up to 3 (number is defined by adapter) deleted records are kept on master repo
            check(masterAdapter.selectByPk(pks[5].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[6].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[5].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[6].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
        }
    }

    @Test
    fun delete4RecordsTest()
    {
        insertTest()

        // delete more records from mirror2 such that mirror1 will need to resync
        run {
            mirror2.write {
                mirror2.deleteByPk(setOf(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
            }
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.pusher.push(master.pushTarget,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[0]) == null)
            check(mirror1Adapter.selectByPk(pks[1]) == null)
            check(mirror1Adapter.selectByPk(pks[2]) == null)
            check(mirror1Adapter.selectByPk(pks[3]) == null)
        }
        master.read {
            // up to 3 (number is defined by adapter) deleted records are kept on master repo
            check(masterAdapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(masterAdapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
        }
    }

    @Test
    fun mergeMirrorIntoMasterTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            mirror2.write {
                val item = mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))!!
                mirror2.insertOrReplace(setOf(item.copy(string = "pee")))
            }
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.pusher.push(master.pushTarget,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[7])?.string == "pks[7]pee")
        }
        master.read {
            check(masterAdapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]pee")
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]pee")
        }
    }

    @Test
    fun mergeMasterIntoMirrorTest()
    {
        insertTest()

        // delete records from mirror2 & insert record into mirror1
        run {
            mirror2.write {
                val item = mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))!!
                mirror2.insertOrReplace(setOf(item.copy(string = "pee")))
            }
            mirror1.write {
                val item = mirror1Adapter.selectByPk(pks[7])!!
                mirror1.insertOrReplace(setOf(item.copy(string = "poo")))
            }
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.pusher.push(master.pushTarget,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[7])?.string == "poopks[7]pee")
        }
        master.read {
            check(masterAdapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]pee")
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]pee")
        }

        // sync mirror1 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror1.write {
                    mirror1.pusher.push(master.pushTarget,mirror1Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check that merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[7])?.string == "pks[7]peepoopks[7]pee") {mirror1Adapter.selectByPk(pks[7])?.string!!}
        }
        master.read {
            check(masterAdapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]peepoopks[7]pee")
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]peepoopks[7]pee")
        }
    }

    @Test
    fun retainDirtyRecordsWhenReSyncingTest()
    {
        insertTest()

        // delete more records from mirror2 such that mirror1 will need to resync
        run {
            mirror2.write {
                mirror2.deleteByPk(setOf(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
                mirror2.deleteByPk(setOf(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)))
            }
            mirror1.write {
                val item = mirror1Adapter.selectByPk(pks[7])!!
                mirror1.insertOrReplace(setOf(item.copy(string = "poo")))
            }
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror2.write {
                    mirror2.pusher.push(master.pushTarget,mirror2Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[0]) == null)
            check(mirror1Adapter.selectByPk(pks[1]) == null)
            check(mirror1Adapter.selectByPk(pks[2]) == null)
            check(mirror1Adapter.selectByPk(pks[3]) == null)
            check(mirror1Adapter.selectByPk(pks[7])?.string == "poo")
        }
        master.read {
            // up to 3 (number is defined by adapter) deleted records are kept on master repo
            check(masterAdapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(masterAdapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]")
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]")
        }

        // sync mirror2 records to master & mirrors
        run {
            // merge into master
            master.write {
                mirror1.write {
                    mirror1.pusher.push(master.pushTarget,mirror1Id,masterId)
                }
            }

            // pull from master into mirror1 & mirror2
            master.read {
                mirror1.write {
                    mirror1.puller.pull(master.pullTarget,mirror1Id,masterId)
                }
                mirror2.write {
                    mirror2.puller.pull(master.pullTarget,mirror2Id,masterId)
                }
            }
        }

        // check records are deleted in all repos and merging is as expected
        mirror1.read {
            check(mirror1Adapter.selectByPk(pks[0]) == null)
            check(mirror1Adapter.selectByPk(pks[1]) == null)
            check(mirror1Adapter.selectByPk(pks[2]) == null)
            check(mirror1Adapter.selectByPk(pks[3]) == null)
            check(mirror1Adapter.selectByPk(pks[7])?.string == "pks[7]poo")
        }
        master.read {
            // up to 3 (number is defined by adapter) deleted records are kept on master repo
            check(masterAdapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(masterAdapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.isDeleted == true)
            check(masterAdapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]poo")
        }
        mirror2.read {
            check(mirror2Adapter.selectByPk(pks[0].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[1].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[2].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[3].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id)) == null)
            check(mirror2Adapter.selectByPk(pks[7].copy(DeltaRepo.Item.Pk,nodePk = mirror1Id))?.string == "pks[7]poo")
        }
    }
}
