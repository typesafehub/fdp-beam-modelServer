package com.lightbend.scala.beam.processors

import org.apache.beam.sdk.transforms.SimpleFunction

class SimplePrinterFn[T] extends SimpleFunction[T,T]{

  override def apply(input: T): T = {
    println(s"Processing data $input")
    input
  }
}
