package org.opencypher.spark.impl.addr

final case class IdMask(size: Int = 42) extends AnyVal {

  def prefixSize = 64 - offsetSize

  // One of: 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62
  def offsetSize = Math.max((size+1) & 62, 16)

  def slice(hash: Long) = IdSlice(prefix(hash) | (1 << offsetSize))
  def prefix(id: Long) = id & (~(Long.MaxValue >>> prefixSize))
  def suffix(id: Long) = id & (Long.MaxValue >>> prefixSize)
}
