package com.github.ericytsang.lib.repo

import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

open class SimpleRepo:Repo
{
    private val readWriteLock = ReentrantReadWriteLock()

    private val readers = Collections.synchronizedSet(mutableSetOf<Thread>())

    override fun <R> read(block:()->R):R
    {
        synchronized(readers)
        {
            check(Thread.currentThread() !in readers)
            readers += Thread.currentThread()
        }
        try
        {
            return readWriteLock.read(block)
        }
        finally
        {
            readers -= Thread.currentThread()
        }
    }

    override fun <R> write(block:()->R):R
    {
        return readWriteLock.write(block)
    }

    protected fun checkCanRead()
    {
        check(readWriteLock.isWriteLockedByCurrentThread || Thread.currentThread() in readers)
    }

    protected fun checkCanWrite()
    {
        check(readWriteLock.isWriteLockedByCurrentThread)
    }
}
