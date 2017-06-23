/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.flaminem.flamy.parsing

import com.flaminem.flamy.parsing.hive.FlamyParsingException

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Table name rules in Hive:
  *
  * - All table names are case-insensitive.
  *
  * - All permanent tables must contain only alphanumeric characters or underscores.
  *   Table names in backquotes are allowed and do not require to be separated with a space from other keywords.
  *   For instance "SELECT * FROM`schema.table`" is valid.
  *
  * - Temporary table aliases and CTE names must be simple names,
  *   but when inside backquotes, any UTF-8 character is allowed (except backquotes of course).
  *   The name is still case-insensitive though.
  *
  */
object ParsingUtils {

  /*
     Explanation of (?:[^\\]|\\(?!(?:\\|\k<quote>))|\\\\|\\\k<quote>)*? :

     ?: means non matching group
     we match:
        [^\\]                    everything else than backslash
     or \\(?!(?:\\|\k<quote>))   a backslashes NOT followed by another backslash or by the quote char
     or \\\\                     two consecutive backslashes
     or \\\k<quote>              a backslash followed by the quote char

     *? means: any number of time, non-greedy (we take the smallest possible match)
   */
  /** Matches any quoted string (between simple or double quotes)
    * and captures the quote char as first group (named "quote"), and the quoted string as second group (named "string").
    * eg:
    * "this is a string"
    * 'this is also a string'
    * "this 'one' is \"weird\" but it is \'definitely\' a string"
    * "this one is dangerous but still is a \\string\\"
    */
  val quotedStringPattern: String = """(?<quote>['"])(?<string>(?:[^\\]|\\(?!(?:\\|\k<quote>))|\\\\|\\\k<quote>)*?)\k<quote>"""

  /** Matches any quoted string (between simple or double quotes)
    * and captures the quote char as first group (named "quote"), and the quoted string as second group (named "string").
    * eg:
    * "this is a string"
    * 'this is also a string'
    * "this 'one' is \"weird\" but it is \'definitely\' a string"
    * "this one is dangerous but still is a \\string\\"
    */
  val quotedStringRegex: Regex = ParsingUtils.quotedStringPattern.r

  /**
    * A regex pattern that matches zero or more space-like characters
    */
  val z = """\s*"""

  /**
    * A regex pattern that matches one or more space-like characters
    */
  val s = """\s+"""

  /**
    * A regex pattern that matches one or more alphanumeric character or underscore.
    */
  val t = """[_\p{Alnum}]+"""

  /**
    * A regex pattern to match simple table names after an alphanumeric character (a separating space is required)
    * example (the matched part is put in brackets): INSERT OVERWRITE TABLE [table_name]
    * WARNING: this will also match the space before the table if there is one, be sure to use trim if you want to get the table name.
    */
  val simpleTableNamePattern = s"""(?:$s$t(?![.])|$z`[^`]+`(?![.]))"""

  /**
    * A regex pattern to match simple table names after a non-alphanumeric character (no separating space required)
    * example (the matched part is put in brackets): FROM T,[table_name]
    */
  val noSpaceSimpleTableNamePattern = s"""(?:$t(?![.])|`[^`]+`(?![.]))"""

  /**
    * A regex pattern to match simple or fully qualified table names after an alphanumeric character (a separating space is required)
    * example (the matched part is put in brackets): INSERT OVERWRITE TABLE [schema_name.table_name]
    * WARNING: this will also match the space before the table if there is one, be sure to use trim if you want to get the table name.
    */
  val tableNamePattern =        s"""(?:$s$t[.]$t|$z`$t[.]$t`|$z`$t`[.]`$t`|$s$t[.]`$t`|$z`$t`[.]$t|$s$t(?![.])|$z`[^`]+`(?![.]))"""

  /**
    * A regex pattern to match simple or fully qualified table names after a non-alphanumeric character (no separating space required)
    * example (the matched part is put in brackets) : FROM T,[schema_name.table_name]
    */
  val noSpaceTableNamePattern = s"""(?:$t[.]$t|`$t[.]$t`|`$t`[.]`$t`|$t[.]`$t`|`$t`[.]$t|$t(?![.])|`[^`]+`(?![.]))"""

  /**
    * A regex pattern to match fully qualified table names after an alphanumeric character (a separating space is required)
    * example (the matched part is put in brackets): INSERT OVERWRITE TABLE [schema_name.table_name]
    * WARNING: this will also match the space before the table if there is one, be sure to use trim if you want to get the table name.
    */
  val fullTableNamePattern =        s"""(?:$s$t[.]$t|$z`$t[.]$t`|$z`$t`[.]`$t`|$s$t[.]`$t`|$z`$t`[.]$t)"""

  /**
    * Find the next opening parenthesis and return the index of the character just after the corresponding closing parenthesis.
    *
    * @param s
    * @param start
    * @return
    */
  def skipParentheses(s: String, start: Integer): Int = {
    skipParentheses(s.toCharArray, start)
  }

  /**
    * Find the next opening parenthesis and return the index of the character just after the corresponding closing parenthesis.
    *
    * @param a
    * @param start
    * @return
    */
  def skipParentheses(a: Array[Char], start: Integer): Int = {
    var i = start
    var height = 0
    var continue = true
    while(continue && i < a.length){
      a(i) match {
        case _ if height < 0 =>
          throw new FlamyParsingException("Wrong number of parentheses")
        case '(' =>
          height += 1
        case ')' if height == 1 =>
          continue = false
        case ')' =>
          height -= 1
        case ''' | '"' =>
          i = skipString(a, i) - 1
        case _ => ()
      }
      i += 1
    }
    if(continue){
      throw new FlamyParsingException("Wrong number of parentheses")
    }
    else{
      i
    }
  }

  /**
    * Find the next occurrence of the given character, and ignore the content of strings
    *
    * @param a
    * @param start
    * @return
    */
  //noinspection ScalaStyle
  def findNextCharOutsideQuotes(a: Array[Char], char: Char, start: Integer): Option[Int] = {
    var i = start
    var height = 0
    var continue = true
    while(continue && i < a.length){
      a(i) match {
        case c if c == char => continue = false
        case ''' | '"' => i = skipString(a, i) - 1
        case _ => ()
      }
      i += 1
    }
    if(continue){
      None
    }
    else{
      Some(i-1)
    }
  }

  /**
    * Find the next quote char (" or ') and return the index of character just after the corresponding closing quote char.
    *
    * @param a
    * @param start
    * @return
    */
  def skipString(a: Array[Char], start: Integer): Int = {
    var i = start
    var height = 0
    var quoteChar: Option[Char] = None
    var continue = true
    while(continue && i < a.length){
      (a(i), quoteChar) match {
        case (''', None) => quoteChar = Some(''')
        case ('"', None) => quoteChar = Some('"')
        case ('\\', _) => i+=1
        case (''', Some(''')) => continue = false
        case ('"', Some('"')) => continue = false
        case _ => ()
      }
      i += 1
    }
    i
  }


  /**
    * Replace every string in the query by a numbered string ("1", "2", "3", etc.)
    * String content is useless for the parsing but may be harmful when containing matchable keywords.
    * @param query
    */
  def replaceStrings(query: String): (String, Map[String, String]) = {
    val stringRE = """(?i)(['"])(.*?)(?<!\\)\1""".r

    val map = new mutable.HashMap[String, String]
    var counter = 0
    def replacer(m: Regex.Match): String = {
      counter += 1
      map += counter.toString -> m.group(2)
      m.group(1) + counter + m.group(1)
    }
    val res = stringRE.replaceAllIn(query, replacer _)
    (res, map.toMap)
  }


}
