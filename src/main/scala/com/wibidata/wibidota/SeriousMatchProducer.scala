package com.wibidata.wibidota

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.DSL._
import com.wibidata.wibidota.DotaValues.{LeaverStatus, LobbyType}

class SeriousMatchProducer(args: Args) extends KijiJob(args) {

  def isSeriousMatch(gmCell : KijiSlice[Int], ltCell : KijiSlice[Int], playerCell : KijiSlice[AvroRecord]): Boolean = {
    var gameMode = gmCell.getFirstValue();
    var lobbyType = LobbyType.fromInt(ltCell.getFirstValue());
    var players = playerCell.getFirstValue();
    return ( (gameMode < 6 || gameMode == 12 || gameMode == 14)  &&
      ( lobbyType == LobbyType.SOLO_QUEUE || lobbyType == LobbyType.TEAM_MATCH ||
        lobbyType == LobbyType.PUBLIC_MATCHMAKING ||lobbyType == LobbyType.TOURNAMENT ) &&
      players.asRecord()("players").asList().forall(p => p.asRecord()("leaver_status") == LeaverStatus.STAYED.getEncoding()))
  }

  var in = KijiInput(args("table-uri"))(
    Map(
      Column("data:game_mode", latest) -> 'game_mode,
      Column("data:lobby_type", latest) -> 'lobby_type,
      Column("data:player_data", latest) -> 'players
    )
  ).filter('game_mode, 'lobby_type, 'players){
    fields : (KijiSlice[Int], KijiSlice[Int], KijiSlice[AvroRecord]) =>
      isSeriousMatch(fields._1, fields._2, fields._3)
  }.project('entityId).insert(('name, 'serious_match),("serious_match", 1.0))
  .write(KijiOutput(args("table-uri"))(Map(MapFamily("derived_data")('name) -> 'serious_match)))
}