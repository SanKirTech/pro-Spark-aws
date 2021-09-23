package com.sankir.smp.core.validators

import com.fasterxml.jackson.databind.JsonNode
import scala.reflect.runtime.{universe => ru}

import scala.util.Try

trait BusinessValidator extends Serializable {
  def validate(data: JsonNode): Try[JsonNode]
}

object BusinessValidator {
  def getFromReflection(className: String): BusinessValidator = {
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(className)
    val obj = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[BusinessValidator]
  }
}

