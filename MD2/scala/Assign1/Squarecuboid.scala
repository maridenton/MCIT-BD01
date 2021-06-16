package Assign1

class Squarecuboid (x: Double, y: Double) extends Square(x) {

  override  def draw(): String = s"A SQUARECUBOID with a $x x $x base and height $y has an area of: ${area()}, and volume: ${volume()}"
  override  def area(): Double = (2 * super.area()) + (4*x*y)
  override def volume(): Double = x * x * y

}
