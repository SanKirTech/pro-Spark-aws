package com.sankir.smp.core.validators

import com.fasterxml.jackson.databind.JsonNode
import scala.reflect.runtime.{universe => ru}

import scala.util.Try

trait GenericBusinessValidator extends Serializable {
  def validateBusiness(data: JsonNode): Try[JsonNode]
}

object GetBusinessValidatorFromReflection {
  def apply(className: String): GenericBusinessValidator = {
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(className)
    val obj = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[GenericBusinessValidator]
  }
}

