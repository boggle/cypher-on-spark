package org.opencypher.spark.impl.addr

import scala.language.implicitConversions

final case class GraphSpace(uri: String) extends AnyVal {
  def `/=`(size: Int) = IdMask(size).slice(uri.hashCode)
}

object GraphSpace {
  implicit def from(uri: String): GraphSpace = apply(uri)
}
