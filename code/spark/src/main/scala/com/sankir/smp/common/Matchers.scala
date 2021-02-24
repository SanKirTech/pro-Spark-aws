/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.common

trait Matcher[T] {
  def test(t: T): Boolean
}

object Matchers {

  def just[T](condition: Boolean): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = condition
  }

  def and[T](first: Matcher[T], second: Matcher[T]): Matcher[T] =
    new Matcher[T] {
      override def test(t: T): Boolean = first.test(t) && second.test(t)
    }

  def and[T](matchers: Seq[Matcher[T]]): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = matchers.toStream.forall(_.test(t))
  }

  def or[T](first: Matcher[T], second: Matcher[T]): Matcher[T] =
    new Matcher[T] {
      override def test(t: T): Boolean = first.test(t) || second.test(t)
    }

  def or[T](matchers: Seq[Matcher[T]]): Matcher[T] = new Matcher[T] {
    override def test(t: T): Boolean = matchers.toStream.exists(_.test(t))
  }

}
