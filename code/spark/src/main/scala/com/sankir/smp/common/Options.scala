package com.sankir.smp.common

object Options {
  def or[A](first: Option[A], second: Option[A]): Option[A] = {
    if (first.isDefined) first else second
  }

  def productK[A, B, C](left: Option[A], right: Option[B], fun: (A, B) => Option[C]): Option[C] = {
    left.flatMap(
      leftValue => right.flatMap(
        rightValue => fun(leftValue, rightValue)
      )
    )

  }
}
