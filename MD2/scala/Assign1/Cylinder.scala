package Assign1

class Cylinder(r: Double, h: Double) extends Circle(r) {

  override def draw(): String = s"A CYLINDER of radius $r and height $h has an area of: ${area()}, and volume: ${volume()}"
  override def area(): Double = ((2 * Circle.pi * r * h) + (2*super.area()))
  override def volume(): Double = (h * super.area())

}
