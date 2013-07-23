package com.wibidata.wibidota


import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.DSL._
import com.wibidata.wibidota.DotaValues.{LeaverStatus, LobbyType}

class TransferDerivedData(args: Args) extends KijiJob(args) {

  def extractPlayerIds(players : KijiSlice[AvroRecord]) : List[Int] = {
    players.getFirstValue().apply("players").asList().map{player : AvroValue => player.apply("account_id").asInt()}.filter{account_id : Int => account_id != -1 && account_id != None}
  }

  KijiInput(args("table-in")).apply(
    Map (
      MapFamily("derived_data", versions = latest) -> 'data,
      Column("data:player_data", versions = latest) -> 'players,
      Column("data:start_time", versions = latest) -> 'start_time
    )
  ).map('players -> 'account_ids){extractPlayerIds}.filter('acccount_ids){account_ids : List[Int] => account_ids.length == 0}.discard('players)
   .map('data -> 'value, 'field){data : KijiSlice[Double] => (data.getFirstValue(), data.getFirst().qualifier)}.discard('data)
   .flatten('account_ids -> 'account_id)
   .write(KijiInput(args("table-out"))(Map(MapFamily("derived_data", qualifierMatches = 'field, versions = 'start_time) -> 'value)))


//    .flatMapTo('derived_data -> 'cells){derived_data : Seq[KijiSlice[Double]] => derived_data}
}
