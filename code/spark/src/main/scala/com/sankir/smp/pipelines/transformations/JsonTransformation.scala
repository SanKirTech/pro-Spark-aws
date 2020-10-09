package com.sankir.smp.pipelines.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sankir.smp.app.JsonUtils

object JsonTransformation {

  def convertJsonNodesToProperFormat(jsonnode:JsonNode) : JsonNode = {

     val objectnode :  ObjectNode =  jsonnode.asInstanceOf[ObjectNode]

     objectnode.put("Quantity", JsonUtils.getStringPropertyOptional(jsonnode,"Quantity").getOrElse("0").toInt )
     objectnode.put("UnitPrice", JsonUtils.getStringPropertyOptional(jsonnode,"UnitPrice").getOrElse("0").toDouble )

    objectnode
  }

}
