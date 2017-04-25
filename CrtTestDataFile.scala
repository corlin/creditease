/**
  * Created by corlinchen on 2017/4/25.
  */

import java.io._
import scala.util.Random
def printToFile(path: String)(op: PrintWriter => Unit) {
  val p = new PrintWriter(new File(path))
  try { op(p) } finally { p.close() }
}

def nextString = (1 to 10) map (_ => Random.nextPrintableChar) mkString
def nextLine = (1 to 4) map (_ => nextString) mkString "\t"

printToFile("/devbase/test") { p =>
  for (_ <- 1 to 100) {
    p.println(nextLine)
  }
}