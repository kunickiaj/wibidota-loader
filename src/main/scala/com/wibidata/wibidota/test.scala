package com.wibidata.wibidota

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.DSL._

import com.twitter.scalding._


class StreakCount(args: Args) extends KijiJob(args) {

  def getData(slice: KijiSlice[AvroRecord]): Seq[AvroRecord] = {
    slice.cells.map { cell => cell.datum }
  }

  KijiInput(args("table-uri"))(Map(Column("data:player_data", latest) -> 'player_data))
    .flatMapTo('player_data -> 'data) { getData }
    .mapTo('data -> 'id){data : AvroRecord => data("players").asList()
      .mapTo('element -> 'hero_id){
        element : AvroRecord => element("hero_id").asInt();
    }}

    .write(Tsv(args("output")))
}