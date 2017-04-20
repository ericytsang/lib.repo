package com.github.ericytsang.lib.repo

interface Repo
{
    interface Item
    {
        interface Pk
        {
            val id:Long
        }
    }
    fun <R> read(block:()->R):R
    fun <R> write(block:()->R):R
}
