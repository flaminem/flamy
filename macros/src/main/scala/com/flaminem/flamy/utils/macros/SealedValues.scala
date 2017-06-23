package com.flaminem.flamy.utils.macros

import scala.collection.immutable.Set
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

//scalastyle:off
/**:: Experimental ::
  *
  * Created by fpin on 12/25/14.
  * Pumped from http://stackoverflow.com/questions/13671734/iteration-over-a-sealed-trait-in-scala answer from user673551
  *
  * This class uses a macro to allow iteration over all implementations of a sealed trait (or class)
  * If the trait (or class) is not sealed, you will get a compilation error.
  *
  * Example:
  *  object SealedTrait {
  *   sealed trait SealedTrait
  *   case object A extends SealedTrait
  *   case object B extends SealedTrait
  *   case object C extends SealedTrait
  *   val values: Set[SealedTrait] = SealedTraitValues.values[SealedTrait]
  * }
  *
  * println(SealedTrait.values)
  * > Set(A, B, C)
  *
  * object SealedClass {
  *   sealed abstract class SealedClass
  *   case object A extends SealedClass
  *   case object B extends SealedClass
  *   case object C extends SealedClass
  *   val values: Set[SealedClass] = SealedTraitValues.values[SealedClass]
  * }
  *
  * println(SealedClass.values)
  * > Set(A, B, C)
  *
  */
object SealedValues {

  def values[A]: Set[A] = macro values_impl[A]

  def values_impl[A: c.WeakTypeTag](c: blackbox.Context) : c.Expr[Set[A]] = {
    import c.universe._

    val symbol = weakTypeOf[A].typeSymbol

    if (!symbol.isClass)  { c.abort(
      c.enclosingPosition,
      "Can only enumerate values of a sealed trait or class."
    )}
    else if (!symbol.asClass.isSealed) {
      c.abort(
        c.enclosingPosition,
        "Can only enumerate values of a sealed trait or class."
      )
    } else {
      val siblingSubclasses: List[Symbol] = scala.util.Try {
        val enclosingModule = c.macroApplication
        enclosingModule.filter { x =>
          scala.util.Try(x.symbol.asModule.moduleClass.asClass.baseClasses.contains(symbol))
            .getOrElse(false)
        }.map(_.symbol)
      } getOrElse {
        Nil
      }

      val children = symbol.asClass.knownDirectSubclasses.toList ::: siblingSubclasses
      if (!children.forall(x => x.isModuleClass || x.isModule)) {
        c.abort(
          c.enclosingPosition,
          "All children must be objects."
        )
      } else c.Expr[Set[A]] {
        def sourceModuleRef(sym: Symbol) = Ident(
          if (sym.isModule) {
            sym
          }
          else {
            sym.asInstanceOf[
              scala.reflect.internal.Symbols#Symbol
              ].sourceModule.asInstanceOf[Symbol]
          }
        )

        Apply(
          Select(
            reify(Set).tree,
            TermName("apply")
          ),
          children.map(sourceModuleRef(_))
        )
      }
    }
  }
}

//scalastyle:on
