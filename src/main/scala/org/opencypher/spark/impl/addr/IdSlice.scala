package org.opencypher.spark.impl.addr

final case class IdSlice(hash: Long) {
  def mask = IdMask(size)
  def size = {
    val result = java.lang.Long.numberOfTrailingZeros(hash) + 1
    if (result < 16 || result > 62)
      throw new IllegalArgumentException(s"Size must be from 16..62 inclusive but was $size")
    else
      result
  }
}
