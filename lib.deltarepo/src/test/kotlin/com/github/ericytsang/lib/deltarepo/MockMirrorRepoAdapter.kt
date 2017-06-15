package com.github.ericytsang.lib.deltarepo

class MockMirrorRepoAdapter:MirrorRepo.Adapter<MockItem>
{
    val records = mutableMapOf<Long,MockItem>()
    override val BATCH_SIZE:Int = 3
    override var latestIsDeletedUpdateSequence:Long = Long.MIN_VALUE
    override var latestIsNotDeletedUpdateSequence:Long = Long.MIN_VALUE

    override fun merge(items:List<MockItem>)
    {
        items.forEach {
            if (it.isDeleted)
                records.remove(it.pk)
            else
                records[it.pk] = it
        }
    }

    override fun prepareForResync()
    {
        val allPks = records.keys.toList()
        allPks.forEach {
            val record = records[it]!!
            records[it] = record.copy(isDeleted = true)
        }
    }

    override fun completeResync()
    {
        val allPks = records.keys.toList()
        allPks.forEach {
            val record = records[it]!!
            if (record.isDeleted) records.remove(it)
        }
    }
}
