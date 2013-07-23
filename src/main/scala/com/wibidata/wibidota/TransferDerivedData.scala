package com.wibidata.wibidota


import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.DSL._
import org.kiji.schema.KijiURI

class TransferDerivedData(args: Args) extends KijiJob(args) {

  def extractPlayerIds(players : KijiSlice[AvroRecord]) : List[Int] = {
    players.getFirstValue().apply("players").asList().map{player : AvroValue => player.apply("account_id").asInt()}.filter{account_id : Int => account_id != None}
  }

  KijiInput(args("table-in"))(
    Map (
//      MapFamily("derived_data", versions = latest) -> 'data,
//      Column("data:player_data", versions = latest) -> 'players,
      Column("data:start_time", versions = latest) -> 'start_time
    )
  )//d.discard('entityId)//.map('players -> 'account_ids){extractPlayerIds}.filter('account_ids){account_ids : List[Int] => account_ids.length != 0}.discard('players)
//   .map('data -> 'value, 'field){data : KijiSlice[Double] => (data.getFirstValue(), data.getFirst().qualifier)}.discard('data)
//   .flatMap('account_ids -> 'entityId){account_ids : List[Int] => account_ids.map(id => EntityId.fromComponents(args("table-out"), Seq(id)))}
    .write(Tsv("fake_out.tsv"))
//   .write(KijiOutput(args("table-out"), 'start_time)(Map(MapFamily("derived_data")('field) -> 'value)))
}
