/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

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
