package org.opencypher.spark.impl.addr

case class SpaceMap(
  slices: IndexedSeq[IdSlice],
  spaces: IndexedSeq[GraphSpace]
) {
  def lookup(space: GraphSpace): Option[IdSlice] = ???
  def reserve(space: GraphSpace): SpaceMap = ???
}


