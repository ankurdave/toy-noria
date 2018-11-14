package com.ankurdave

/**
 * Toy implementation of the Noria dataflow model
 * ([[https://pdos.csail.mit.edu/papers/noria:osdi18.pdf Gjengset et al., OSDI 2018]]).
 */
package object toynoria {
  /** The key type for a [[Record]]. */
  type Id = Long

  var trace = false

  def logTrace(msg: String) = {
    if (trace) {
      println(msg)
    }
  }
}
