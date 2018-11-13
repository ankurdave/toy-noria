package com.ankurdave

package object toynoria {
  type Id = Long

  var trace = false

  def logTrace(msg: String) = {
    if (trace) {
      println(msg)
    }
  }
}
