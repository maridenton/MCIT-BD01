package Assign1

object main extends App {
  println("Simple test, instantiating various shape objects")
  println("================================================")

  val cir: Circle = new Circle(5.0)
  render(cir)

  val cyl: Cylinder = new Cylinder(5.0,6.0)
  render(cyl)

  val sq: Square = new Square(3.0)
  render(sq)

  val sqcube: Squarecuboid = new Squarecuboid(3.0,5.0)
  render(sqcube)


  println("Test using the shape.create method")
  println("==================================")

  val cir2  = Shape.create("circle",5.0,0.0)
  render(cir2)
  val cyl2  = Shape.create("cylinder",5.0,6.0)
  render(cyl2)
  val sq2  = Shape.create("square",3.0,0.0)
  render(sq2)
  val sqcub2  = Shape.create("cube",3.0,5.0)
  render(sqcub2)

  //======================================//

  def render(shape: Shape): Unit = {
    println(shape.draw())
  }

}
