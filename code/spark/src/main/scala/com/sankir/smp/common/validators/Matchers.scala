package com.sankir.smp.common.validators

trait  Matcher[T] {
  def test(t: T): Boolean
}

object Matchers {

  def just[T](condition: Boolean): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = condition
  }

  def and[T](first: Matcher[T], second: Matcher[T]):Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = first.test(t) && second.test(t)
  }

  def and[T](matchers: Seq[Matcher[T]]): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = matchers.toStream.forall(_.test(t))
  }

  def or[T](first: Matcher[T], second: Matcher[T]):Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = first.test(t) || second.test(t)
  }

  def or[T](matchers: Seq[Matcher[T]]): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = matchers.toStream.exists(_.test(t))
  }

}