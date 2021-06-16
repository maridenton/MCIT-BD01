package Assign2

//**  Disclaimer: Not my code, all credit to the Author (Iraj?)

trait Join[L, R, Q] {
  def join(a: List[L], b: List[R]): List[Q]
}

case class JoinOutput(left: Any, right: Any)

class GenericNestedLoopJoin[L, R](val joinCond: (L, R) => Boolean) extends Join[L, R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    i <- a
    j <- b
    if joinCond(i,j)
  } yield JoinOutput(i, j)
}




class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.route_id -> route).toMap

  def lookup(routeId: Int): Route = lookupTable.getOrElse(routeId, null)
}
