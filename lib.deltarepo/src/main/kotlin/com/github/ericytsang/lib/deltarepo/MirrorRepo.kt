package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

interface MirrorRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo,DeltaRepo<ItemPk,Item>
{
    val pusher:Pusher<ItemPk,Item>
    val puller:Puller<ItemPk,Item>
}
