package com.atguigu.sparkspall.common.util

object StringUtil {

def isNotEmply(s:String): Boolean ={
  s != null && !"".equals(s.trim)
}
}
