package com.github.ericytsang.lib.deltarepo

interface MirrorRepo<Item:DeltaRepo.Item<Item>>
{
    val pusher:Pusher<Item>
    val puller:Puller<Item>
    val pullTarget:Puller.Remote<Item>
}
