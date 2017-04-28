package com.github.ericytsang.lib.deltarepo

import com.github.ericytsang.lib.repo.Repo

interface MasterRepo<ItemPk:DeltaRepo.Item.Pk<ItemPk>,Item:DeltaRepo.Item<ItemPk,Item>>:Repo,DeltaRepo<ItemPk,Item>
{
    val pushTarget:Pusher.Remote<ItemPk,Item>
    val pullTarget:Puller.Remote<ItemPk,Item>
}
