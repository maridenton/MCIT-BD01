package Assign1

trait Shape {

  def draw(): String

  def area(): Double

  def volume(): Double

}

object Shape {

  def create(name: String, x: Double, y: Double): Shape = {

    // this feels like  a terrible inelegant implementation, but I just dont know how else

    name match {

      case "circle" => {
        val shape: Shape = new Circle(x);
        return shape
      }
      case "cylinder" => {
        val shape: Shape = new Cylinder(x,y);
        return shape
      }
      case "square" => {
        val shape: Shape = new Square(x);
        return shape
      }
      case "cube" => {
        val shape: Shape = new Squarecuboid(x,y);
        return shape
      }
      case _ => {
        val shape: Shape = new Square(1);
        return shape // default if not matched
      }

    }

  }

}


