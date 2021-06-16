package Assign2

case class RouteTrip(trip: Trip, route: Route)


object RouteTrip {
  def letsMakeOutput(trip: Trip, route: Route): String = {
      trip.route_id + "," +
      trip.service_id + "," +
      trip.trip_id + "," +
      trip.trip_headsign + "," +
      trip.direction_id + "," +
      trip.shape_id + "," +
      trip.wheelchair_accessible + "," +
      trip.note_fr.getOrElse("") + "," +
      trip.note_en.getOrElse("") + "," +
      route.route_id + "," +
      route.agency_id + "," +
      route.route_short_name + "," +
      route.route_long_name + "," +
      route.route_type + "," +
      route.route_url + "," +
      route.route_color + "," +
      route.route_text_color


  }
}