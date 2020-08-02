package com.sankir.smp.app

import org.codehaus.jackson.annotate.JsonProperty

class SDFMessage(@JsonProperty("_m") metaData: SDFMessage#Metadata) {

  val METADATA = "_m"
  val PAYLOAD = "_p"

  val RECEIVED_TIME = "_rt"
  val SOURCE = "_src"
  val OWNER = "_o"
  val SOURCE_DETAILS = "_src_dtls"

  case class Metadata(owner: String, source: String) {
    def setOwner(owner: String) {
      Metadata(owner, source)
    }
  }
  //  @JsonProperty("_m")
  //  Metadata


}
