package Assign1

class Square (x: Double) extends Shape {
  override  def draw(): String = s"A SQUARE with sides $x has a surface area of: ${area()}"
  override  def area(): Double = x*x
  override  def volume(): Double = 0

}
