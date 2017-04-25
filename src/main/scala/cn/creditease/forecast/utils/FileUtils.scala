package cn.creditease.forecast.utils

import java.io.{File, PrintWriter}

object FileUtils extends FileUtils

trait FileUtils {
  def pfRight(path: String): String = {
    val right = if (path.endsWith("/")) path.dropRight(1) else path
    right
  }

  def pf(path: String): String = {
    val right = if (path.endsWith("/")) path.dropRight(1) else path
    val left = if (right.startsWith("/")) right.tail else right
    left
  }

  def projectRootPath(clazz: Class[_]): String = {
    val currentPath = clazz.getResource("").getPath
    val rootPath = pf(currentPath.substring(0, currentPath.indexOf("target")))
    s"/$rootPath"
  }

  def testResourcesPath(clazz: Class[_]): String = s"/${projectRootPath(clazz)}/src/test/resources"

  //  def testResourcesPath(clazz: Class[_]): String = {
  //    val currentPath = clazz.getResource("").getPath
  //    val rootPath = pf(currentPath.substring(0, currentPath.indexOf("target")))
  //    s"/$rootPath/src/test/resources"
  //  }

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}
