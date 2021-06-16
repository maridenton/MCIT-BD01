package Assign2

import scala.io.Source
import scala.language.postfixOps

object DataEnrichment extends App {

  //  *** Read data from CSV file into List *****
  //  ===========================================
  //   Creating tripList from trips.txt

  val bufferedSourceTrips = Source.fromFile("/home/bd-user/Documents/2MD2/Project/Data/trips.txt")
  val tripList: List[Trip] = bufferedSourceTrips
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Trip(p(0).toInt, p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,
      if (p(7).isEmpty) None else Some(p(7)),
      if (p(8).isEmpty) None else Some(p(8))))
  bufferedSourceTrips.close

  // Creating routeList from routes.txt

  val bufferedSourceRoute = Source.fromFile("/home/bd-user/Documents/2MD2/Project/Data/routes.txt")
  val routeList: List[Route] = bufferedSourceRoute
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Route(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6), p(7)))
  bufferedSourceRoute.close

  // Creating calenderList from calendar.txt

  val bufferedSourceCalendar = Source.fromFile("/home/bd-user/Documents/2MD2/Project/Data/calendar.txt")
  val calendarList: List[Calendar] = bufferedSourceCalendar
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Calendar(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7) toInt, p(8), p(9)))
  bufferedSourceCalendar.close

  //  Enrich trips by joining with Routes on route_ID,
  //  using clever little method we found:  GenericNestedLoopJoin*
  //  *Disclaimer: not my code, I'm just borrowing it.  All credit to the author.

  val routeTrips: List[JoinOutput] = new GenericNestedLoopJoin[Trip, Route]((i, j) => i.route_id == j.route_id)
    .join(tripList, routeList)

  //  Enrich further by joining Calender on service ID

  val enrichedTrips: List[JoinOutput] =
    new GenericNestedLoopJoin[JoinOutput, Calendar]((i, j) => i.left.asInstanceOf[Trip].service_id == j.service_id)
      .join(routeTrips, calendarList)

  val csvFields = List("route_id", "service_id", "trip_id", "trip_headsign","direction_id","shape_id","wheelchair_accessible","note_fr","note_en"
    ,"agency_id","route_short_name","route_long_name","route_type", "route_url","route_color","route_text_color"
    ,"monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date")
  println(csvFields)


  // val linesCSV :List[String]  =    //removed because it does not work
  enrichedTrips
    .map(n =>
      EnrichedTrip.toCSV(n.left.asInstanceOf[JoinOutput].left.asInstanceOf[Trip],
        n.left.asInstanceOf[JoinOutput].right.asInstanceOf[Route],
        n.right.asInstanceOf[Calendar]))
    .foreach(println)

  //  val finalRouteTrips : List[String] = routeTrips
  //    .map(n => RouteTrip.letsMakeOutput(n.left.asInstanceOf[Trip], n.right.asInstanceOf[Route]))

/*
  val file = new File("/home/bd-user/Documents/2MD2/Project/Data/enrichedtrips.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  for (line <- finalRouteTrips) {
    bw.write(line)
  }
  bw.close()
*/


}
