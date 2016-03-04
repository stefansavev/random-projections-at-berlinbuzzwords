package com.stefansavev.core.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.core.serialization.core.{StringSerializer, IntSerializer, MemoryTracker, TypedSerializer}
import com.stefansavev.randomprojections.utils.{StringIdHasherSettings, String2IdHasher}

object String2IdHasherSerialization {

  implicit object String2IdHasherSerializer extends TypedSerializer[String2IdHasher] {
    def toBinary(outputStream: OutputStream, string2Id: String2IdHasher): Unit = {
      if (string2Id == null){
        throw new IllegalStateException("string2IdHasher cannot be null")
      }
      val settings = string2Id.getSettings()
      IntSerializer.write(outputStream, settings.maxValues)
      IntSerializer.write(outputStream, settings.avgStringLen)
      IntSerializer.write(outputStream, settings.toleratedNumberOfCollisions)
      var id = 0
      val numStrings = string2Id.numberOfUniqueStrings()
      IntSerializer.write(outputStream, numStrings)
      while(id < numStrings){
        val str = string2Id.getStringAtInternalIndex(id).get
        StringSerializer.write(outputStream, str)
        id += 1
      }
    }

    def fromBinary(inputStream: InputStream): String2IdHasher = {
      /*just print memory usage
      val maxValues = IntSerializer.read(inputStream)
      val avgStringLen = IntSerializer.read(inputStream)
      val numCollisions = IntSerializer.read(inputStream)
      val numStrings = IntSerializer.read(inputStream)
      var size = 0L
      var i = 0
      while(i < numStrings){
        val str = StringSerializer.read(inputStream)
        size += (1 + str.length*2)
        i += 1
      }
      println("Strings size in MB: " + (size/(1024*1024)))
      null
      */
      val maxValues = IntSerializer.read(inputStream)
      val avgStringLen = IntSerializer.read(inputStream)
      val numCollisions = IntSerializer.read(inputStream)
      val settings = new StringIdHasherSettings(maxValues, avgStringLen, numCollisions)
      val string2IdHasher = new String2IdHasher(settings)
      val numStrings = IntSerializer.read(inputStream)
      var i = 0
      while(i < numStrings){
        val str = StringSerializer.read(inputStream)
        val handle = string2IdHasher.add(str)
        val index = string2IdHasher.getInternalId(handle)
        if (index != i){
          throw new IllegalStateException("Internal error while reading hashed strings")
        }
        i += 1
      }
      string2IdHasher
    }

    def sizeInBytes(memoryTracker: MemoryTracker, string2Id: String2IdHasher): Long = {
      //probably its stored more efficiently on disk than in memory
      if (string2Id == null){
        throw new IllegalStateException("string2IdHasher cannot be null")
      }
      val settings = string2Id.getSettings()
      var sumSizes = 3*IntSerializer.sizeInBytes

      var id = 0
      val numStrings = string2Id.numberOfUniqueStrings()
      sumSizes += IntSerializer.sizeInBytes
      while(id < numStrings){
        val str = string2Id.getStringAtInternalIndex(id).get
        sumSizes += StringSerializer.sizeInBytes(str)
        id += 1
      }
      sumSizes
    }
  }


}
