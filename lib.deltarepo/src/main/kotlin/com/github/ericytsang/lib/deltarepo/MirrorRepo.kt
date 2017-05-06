package com.github.ericytsang.lib.deltarepo

interface MirrorRepo<Item:Any>
{
    val pusher:Pusher<Item>
    val puller:Puller<Item>
}
