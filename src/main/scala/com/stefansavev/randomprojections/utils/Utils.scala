package com.stefansavev.randomprojections.utils

import java.io.File
import java.util.Random

import scala.reflect.ClassTag

case class TimedResult[T](result: T, timeMillisecs: Long)

object Utils{
  def todo(): Nothing = {
    throw new RuntimeException("todo")
  }

  def internalError(): Nothing = {
    throw new RuntimeException("internal error")
  }

  def failWith(msg: String): Nothing = {
    throw new RuntimeException(msg)
  }


  def timed[R](msg: String, codeBlock: => R): TimedResult[R] = {
    val start = System.currentTimeMillis()
    val result = codeBlock    // call-by-name
    val end = System.currentTimeMillis()
    val elapsed = end - start
    val timeAsStr = if (elapsed >= 1000) (elapsed/1000.0 + " secs.") else (elapsed + " ms.")
    println(s"Time for '${msg}' ${timeAsStr}")
    TimedResult(result, elapsed)
  }

  def combinePaths(path1: String, path2: String): String =
  {
    val file1 = new File(path1)
    val file2 = new File(file1, path2)
    file2.getPath()
  }

}

object ImplicitExtensions{
  import scala.reflect.ClassTag

  implicit class RichArray(v: Array.type){

    def zip3[A: ClassTag, B: ClassTag, C: ClassTag](a: Array[A], b: Array[B], c: Array[C]): Array[(A, B, C)] = {
      a.zip(b).zip(c).map({case ((va, vb), vc) => (va, vb, vc)})
    }

    def unzip3[A: ClassTag, B: ClassTag, C: ClassTag](arr: Array[(A, B, C)]): (Array[A], Array[B], Array[C]) = {
      val dim = arr.length
      val (a,b,c) = (Array.ofDim[A](dim),Array.ofDim[B](dim),Array.ofDim[C](dim))
      var i = 0
      while(i < dim){
        val (va,vb,vc) = arr(i)
        a(i) = va
        b(i) = vb
        c(i) = vc
        i += 1
      }
      (a,b,c)
    }

    def init[T : ClassTag](sz: Int, f: () => T ): Array[T] = {
      val arr = Array.ofDim[T](sz)
      var i = 0
      while(i < sz){
        arr(i) = f()
        i += 1
      }
      arr
    }

    def init_i[T : ClassTag](sz: Int, f: Int => T ): Array[T] = {
      val arr = Array.ofDim[T](sz)
      var i = 0
      while(i < sz){
        arr(i) = f(i)
        i += 1
      }
      arr
    }

    def init2D[T: ClassTag](rows: Int, cols: Int, f: () => T): Array[Array[T]] = {
      init(rows, () => init(cols, f))
    }

    def init2D_ij[T: ClassTag](rows: Int, cols: Int, f: (Int, Int) => T): Array[Array[T]] = {
      init_i(rows, i => init_i(cols, j => f(i,j)))
    }
  }
}

object RandomExt{
  import ImplicitExtensions.RichArray

  def shuffleArray[T: ClassTag](rnd: Random, data: Array[T]): Array[T] = {
    val randomNumbers = Array.init(data.length, () => rnd.nextDouble())
    data.zip(randomNumbers).sortBy(_._2).map(_._1).toArray
  }

}
