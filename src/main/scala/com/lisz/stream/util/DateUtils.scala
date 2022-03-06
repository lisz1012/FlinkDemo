package com.lisz.stream.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  def getMin(date: Date): String = {
    val pattern = "yyyyMMddHHmm"
    val dateFormat = new SimpleDateFormat(pattern)
    val min = dateFormat.format(date)
    min
  }
}
