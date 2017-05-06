package com.github.ericytsang.lib.deltarepo

interface MasterRepo<Item:DeltaRepo.Item<Item>>
{
    val pushTarget:Pusher.Remote<Item>
    val pullTarget:Puller.Remote<Item>
}