package com.github.ericytsang.lib.deltarepo

interface MasterRepo<Item:Any>
{
    val pushTarget:Pusher.Remote<Item>
    val pullTarget:Puller.Remote<Item>
}