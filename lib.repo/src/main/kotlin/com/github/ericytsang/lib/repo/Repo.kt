package com.github.ericytsang.lib.repo

interface Repo
{
    fun <R> read(block:()->R):R
    fun <R> write(block:()->R):R
}
