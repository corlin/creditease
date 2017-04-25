package cn.creditease.forecast.utils

import cn.creditease.forecast.utils._

object EdpCommon extends EdpCommon

trait EdpCommon extends EdpDefault
  with CommonUtils
  with JsonUtils
  with DateUtils
  with RetryUtils
  with FileUtils {

  val empty = ""
  val nullity = "_NULL_"

  def isNull(s: String) = null == s || nullity == s.trim

  def nullify(s: String) = if (isNull(s)) null else s
}
