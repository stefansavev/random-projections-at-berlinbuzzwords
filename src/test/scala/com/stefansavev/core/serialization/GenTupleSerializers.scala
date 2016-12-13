package com.stefansavev.core.serialization

object GenTupleSerializers {
  //use this code to generate all tuple serializers
  def genTupleSerializers(n: Int): Unit = {
    val ids = Vector.range(1, n + 1)
    val commaSepParams = ids.map(i => s"A${i}").mkString(", ")
    val toBinaryStr = ids.map(i => s"        serA${i}.toBinary(outputStream, input._${i})").mkString("\n")
    val fromBinaryStr = ids.map(i => s"        val a${i} = serA${i}.fromBinary(inputStream)").mkString("\n")
    val outputParams = "(" + ids.map(i => s"a${i}").mkString(", ") + ")"
    val typedSerParams = ids.map(i => s"serA${i}: TypedSerializer[A${i}]").mkString(", ")
    val serParams = ids.map(i => s"serA${i}").mkString(", ")

    var code =
      """
        |    class Tuple%K%Serializer[%commaSepParams%](%typedSerParams%) extends TypedSerializer[(%commaSepParams%)] {
        |
        |      def toBinary(outputStream: OutputStream, input: (%commaSepParams%)): Unit = {
        |%toBinaryStr%
        |      }
        |
        |      def fromBinary(inputStream: InputStream): (%commaSepParams%) = {
        |%fromBinaryStr%
        |        %outputParams%
        |      }
        |    }
        |
        |    implicit def tuple%K%Serializer[%commaSepParams%](implicit %typedSerParams%): Tuple%K%Serializer[%commaSepParams%] = {
        |      new Tuple%K%Serializer[%commaSepParams%](%serParams%)
        |    }
        |
      """.stripMargin

    val replacements = List(
      ("%K%", n.toString),
      ("%commaSepParams%", commaSepParams),
      ("%toBinaryStr%", toBinaryStr),
      ("%fromBinaryStr%", fromBinaryStr),
      ("%outputParams%", outputParams),
      ("%typedSerParams%", typedSerParams),
      ("%serParams%", serParams)
    )

    for ((pattern, value) <- replacements) {
      code = code.replaceAll(pattern, value)
    }
    println(code)
  }

  def main(args: Array[String]): Unit = {
    println("package com.stefansavev.core.serialization.core\n")

    println("import java.io._\n")

    println("object TupleSerializers {")

    for (i <- 2 until 15) {
      genTupleSerializers(i)
      println("    //---------------------------------------------------------")
    }
    println("}\n")

  }

}