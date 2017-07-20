package com.flaminem.flamy.utils.macros

import org.scalatest.FunSuite

/**
 * Created by fpin on 12/25/14.
 */
class SealedValues$Test extends FunSuite {

  object SealedTrait {
    sealed trait SealedTrait
    case object A extends SealedTrait
    case object B extends SealedTrait
    case object C extends SealedTrait
    val values: Seq[SealedTrait] = SealedValues.values[SealedTrait]
  }

  object SealedClass {
    sealed abstract class SealedClass
    case object A extends SealedClass
    case object B extends SealedClass
    case object C extends SealedClass
    val values: Seq[SealedClass] = SealedValues.values[SealedClass]
  }

  object SealedTypedClass {
    sealed abstract class SealedTypedClass[T]
    case object A extends SealedTypedClass[String]
    case object B extends SealedTypedClass[Int]
    case object C extends SealedTypedClass[List[String]]
    val values: Seq[SealedTypedClass[_]] = SealedValues.values[SealedTypedClass[_]]
  }

  test("We should be able to iterate over a sealed trait implementation") {
    assert(SealedTrait.values.size == 3)
    assert(SealedTrait.values.map {_.toString} == Seq("A", "B", "C"))

    assert(SealedClass.values.size == 3)
    assert(SealedClass.values.map {_.toString} == Seq("A", "B", "C"))

    assert(SealedTypedClass.values.size == 3)
    assert(SealedTypedClass.values.map {_.toString} == Seq("A", "B", "C"))
  }

  test("Declaring the values before the objects WILL NOT work") {

    object WrongSealedTrait {
      val values: Seq[SealedTrait] = SealedValues.values[SealedTrait]
      sealed trait SealedTrait
      case object A extends SealedTrait
      case object B extends SealedTrait
      case object C extends SealedTrait
    }

    assert(WrongSealedTrait.values.isEmpty)
  }

}
