package Assign2

case class Trip(
                  route_id: Int,
                  service_id: String,
                  trip_id: String,
                  trip_headsign: String,
                  direction_id: Int,
                  shape_id: Int,
                  wheelchair_accessible: Int,
                  note_fr: Option[String],
                  note_en: Option[String])
