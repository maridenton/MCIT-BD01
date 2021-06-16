package Assign1

class Circle(r: Double) extends Shape {
  override def draw(): String = s"A CIRCLE with a radius of $r has a surface area of: ${area()}"
  override def area(): Double = Circle.pi *r*r
  override def volume(): Double = 0
}

object Circle {

  val pi: Double = 3.14
}
