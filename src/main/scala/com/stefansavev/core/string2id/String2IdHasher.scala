package com.stefansavev.core.string2id

import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.implementation.HadamardUtils
import com.stefansavev.randomprojections.utils.HashUtils

object String2IdHasherHelper{
  def emptyIntArray(size: Int): Array[Int] = {
    val arr = Array.ofDim[Int](size)
    var i = 0
    while(i < size){
      arr(i) = -1
      i += 1
    }
    arr
  }
}

class DynamicDoubleArray(initSize: Int, defaultValue: Double){

  def arrayBuilder(size: Int): Array[Double] = {
    val arr = Array.ofDim[Double](size)
    var i = 0
    while(i < size){
      arr(i) = defaultValue
      i += 1
    }
    arr
  }

  var buffer = arrayBuilder(initSize)

  def set(index: Int, value: Double): Unit = {
    if (index >= buffer.length){
      val oldBuffer = buffer
      buffer = arrayBuilder(2*buffer.length)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
    buffer(index) = value
  }

  def get(index: Int): Double = {
    if (index < buffer.length){
      buffer(index)
    }
    else{
      defaultValue
    }
  }

  def inc(index: Int, value: Double): Unit = {
    set(index, value + get(index))
  }
}

object DynamicIntArray{
  type TupleType = (Int, Int, Array[Int])

  def fromTuple(t: TupleType): DynamicIntArray = {
    val arr = new DynamicIntArray(t._1, t._2, t._3)
    arr
  }
}

object DynamicShortArray{
  type TupleType = (Int, Short, Array[Short])

  def fromTuple(t: TupleType): DynamicShortArray = {
    val arr = new DynamicShortArray(t._1, t._2, t._3)
    arr
  }
}

class DynamicIntArray(initSize: Int, defaultValue: Int, overwriteBuffer: Array[Int] = null){

  def arrayBuilder(size: Int): Array[Int] = {
    val arr = Array.ofDim[Int](size)
    var i = 0
    while(i < size){
      arr(i) = defaultValue
      i += 1
    }
    arr
  }

  var buffer: Array[Int] = if (overwriteBuffer == null) arrayBuilder(initSize) else overwriteBuffer

  def toTuple(): DynamicIntArray.TupleType = {
    (initSize, defaultValue, buffer)
  }

  def set(index: Int, value: Int): Unit = {
    if (index >= buffer.length){ //TODO: make efficient see resize
    val oldBuffer = buffer
      buffer = arrayBuilder(2*buffer.length)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
    buffer(index) = value
  }

  def get(index: Int): Int = {
    if (index < buffer.length){
      buffer(index)
    }
    else{
      defaultValue
    }
  }

  def allocatedSize(): Int = buffer.length

  def truncate(index: Int): Unit = {
    val copy = Array.ofDim[Int](index)
    System.arraycopy(buffer, 0, copy, 0, index)
    buffer = copy
  }

  def resize(newSize: Int): Unit = {
    while (newSize >= buffer.length){ //TODO: make efficient
    val oldBuffer = buffer
      buffer = arrayBuilder(2*buffer.length)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
  }

  def inc(index: Int, value: Int): Unit = {
    set(index, value + get(index))
  }
}

class DynamicShortArray(initSize: Int, defaultValue: Short, overwriteBuffer: Array[Short] = null){

  def arrayBuilder(size: Int): Array[Short] = {
    val arr = Array.ofDim[Short](size)
    var i = 0
    while(i < size){
      arr(i) = defaultValue
      i += 1
    }
    arr
  }

  var buffer: Array[Short] = if (overwriteBuffer == null) arrayBuilder(initSize) else overwriteBuffer

  def toTuple(): DynamicShortArray.TupleType = {
    (initSize, defaultValue, buffer)
  }

  def set(index: Int, value: Short): Unit = {
    if (index >= buffer.length){ //TODO: make efficient see resize
    val oldBuffer = buffer
      buffer = arrayBuilder(2*buffer.length)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
    buffer(index) = value
  }

  def get(index: Int): Short = {
    if (index < buffer.length){
      buffer(index)
    }
    else{
      defaultValue
    }
  }

  def allocatedSize(): Int = buffer.length

  def truncate(index: Int): Unit = {
    val copy = Array.ofDim[Short](index)
    System.arraycopy(buffer, 0, copy, 0, index)
    buffer = copy
  }

  def resize(newSize: Int): Unit = {
    while (newSize >= buffer.length){ //TODO: make efficient
    val oldBuffer = buffer
      buffer = arrayBuilder(2*buffer.length)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
  }


}

class DynamicCharArray(initSize: Int){
  var buffer = Array.ofDim[Char](initSize)

  def ensureSize(size: Int): Unit = {
    if (size > buffer.length){
      var newLen = 2*buffer.length
      while(size > newLen){
        newLen += buffer.length
      }
      val oldBuffer = buffer
      buffer = Array.ofDim[Char](newLen)
      System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length)
    }
  }

  def set(index: Int, ch: Char): Unit = {
    buffer(index) = ch
  }

  def apply(index: Int): Int = {
    buffer(index)
  }
}

class StringStore(val numStrings: Int, val avgStringLen: Int){
  var initStringCache = numStrings*avgStringLen

  //TODO: store stringLengths as short
  var stringLengths = new DynamicIntArray(numStrings, 0)
  var stringPointers = new DynamicIntArray(numStrings, -1)

  val store = new DynamicCharArray(initStringCache)
  var nextPointerToStrings = 0

  //returns > 1 offset at which the string is stored
  def addNewString(index: Int, arr: Array[Char], start: Int, end: Int): Unit = {
    if (index < stringPointers.buffer.length && stringPointers.get(index) != -1){
      throw new IllegalStateException("Cannot overwrite an existing string")
    }
    store.ensureSize(nextPointerToStrings + (end - start))
    stringPointers.set(index, nextPointerToStrings)

    var i = start
    var j = nextPointerToStrings
    while(i < end){
      store.set(j, arr(i))
      j += 1
      i += 1
    }
    nextPointerToStrings = j
    stringLengths.set(index, end - start)
  }

  def equalsValue(index: Int, arr: Array[Char], fromPos: Int, toPos: Int): Boolean = {
    val stringPointer = stringPointers.get(index)
    if (stringPointer == -1){ //similar to null
      false
    }
    else {
      val len = stringLengths.get(index)
      if (len != (toPos - fromPos)){
        false
      }
      else{
        var i = fromPos
        var j = stringPointer
        var areEqual = true
        while (i < toPos && areEqual) {
          if (arr(i) != store(j)) {
            areEqual = false
          }
          else {
            i += 1
            j += 1
          }
        }
        areEqual
      }
    }
  }

  def getStringAtIndex(index: Int): Option[String] = {
    if (stringPointers.get(index) != -1){
      val start = stringPointers.get(index)
      val end = start + stringLengths.get(index)
      val s = store.buffer.subSequence(start, end)
      Some(s.toString)
    }
    else{
      None
    }
  }
}

case class StringIdHasherSettings(maxValues: Int, avgStringLen: Int, toleratedNumberOfCollisions: Int = 4){
  if (!HadamardUtils.isPowerOf2(maxValues)){
    throw new IllegalStateException("The initial size of the table must be a power of 2")
  }
  if (!HadamardUtils.isPowerOf2(toleratedNumberOfCollisions)){
    throw new IllegalStateException("The initial size of the table must be a power of 2")
  }
  if (toleratedNumberOfCollisions < 4){
    throw new IllegalArgumentException("The tolerated number of collisions should be >= 4")
  }
}

class String2IdHasher(settings: StringIdHasherSettings){
  def getSettings(): StringIdHasherSettings = settings

  val maxAllowedValues = settings.maxValues
  val _maxValues = settings.toleratedNumberOfCollisions*settings.maxValues //has to be a power of 2, verified in settings

  //TODO:
  //it's non-resizable now
  //if we want resizable, we need to scan through all strings and rehash
  //or we need to keep the hash code for each string and just truncate fewer bits
  val pointersToValues = String2IdHasherHelper.emptyIntArray(_maxValues)

  val stringStore = new StringStore(16384, settings.avgStringLen)

  var nextAvailablePointer = 0

  val numberOfCollisions = settings.toleratedNumberOfCollisions //should be a power of 2
  val logNumberOfCollisions = java.lang.Integer.bitCount(numberOfCollisions - 1)

  var onesV2 = (pointersToValues.length/numberOfCollisions) - 1
  if (onesV2 < 1){
    throw new IllegalStateException("Invalid arguments to String2IdHasher")
  }
  var bitCountOnesV2: Int = java.lang.Integer.bitCount(onesV2)


  def numberOfUniqueStrings(): Int = {
    nextAvailablePointer
  }

  protected final def nextIndex(hcode: Int, offset: Int) = {
    //no need to improve murmur hash
    val improved = hcode //improve(hcode + offset, seed)
    val shifted = ((improved) >> (32 - (bitCountOnesV2))) & onesV2
    //shifted << logNumberOfCollisions + offset
    (numberOfCollisions*shifted + offset) % pointersToValues.length //because we may overrun
    //we need to chop off the least significant bits
  }

  def findOrGetNewEntryIndex(str: String): Int = {
    val arr = str.toCharArray
    findOrGetNewEntryIndex(arr, 0, arr.length)
  }

  /*
    word string => hash_1, hash_2,...., hash_i
    we store the word at the first available hash_i

    at hash_i => we store a pointer to the next free data store
   */


  def findOrGetNewEntryIndex(arr: Array[Char], fromPos: Int, toPos: Int): Int = {
    val hashCode = HashUtils.charArrayHash(arr, fromPos, toPos)
    var index: Int = 0
    var offset = 0
    do{
      index = nextIndex(hashCode, offset)
      val currentPointer = pointersToValues(index)
      if (currentPointer == -1){
        offset = -1 //we found a free position
      }
      else{
        val isEqual = stringStore.equalsValue(currentPointer, arr, fromPos, toPos)
        if (isEqual){
          offset = -1 //we found a position equal to the input string
        }
        else{
          offset += 1 //we continue to search
        }
      }
      //if offset >= 16 this is definitely an error
    }
    while(offset >= 0 && offset < 64) //we should not see a sequence of 64 non-empty slots in a row

    if (offset >= 64){
      -1 //table must be completely full
    }
    else{
      index
    }
  }

  def fillFactor(): Double = {
    100.0*(nextAvailablePointer.toDouble/_maxValues.toDouble)
  }

  def addAtIndex(index: Int, arr: Array[Char], fromPos: Int, toPos: Int): Unit = {
    val currentPointer = pointersToValues(index)
    if (currentPointer != -1){
      throw new IllegalStateException("occupied pointer")
    }
    pointersToValues(index) = nextAvailablePointer
    stringStore.addNewString(nextAvailablePointer, arr, fromPos, toPos)
    //document store
    nextAvailablePointer += 1
  }

  def getOrAddId(str: String, add: Boolean): Int = {
    val arr = str.toCharArray
    getOrAddId(arr, 0, arr.length, add)
  }

  def add(str: String): Int = {
    getOrAddId(str, true)
  }

  def getInternalId(index: Int): Int = {
    pointersToValues(index)
  }

  def getOrAddId(arr: Array[Char], fromPos: Int, toPos: Int, addMode: Boolean): Int = {
    val index = findOrGetNewEntryIndex(arr, fromPos, toPos)
    if (index == -1){
      -2 //table size is exceeded, with a max collisions >= 4 this is unlikely to happen
    }
    else {
      val currentPointer = pointersToValues(index)
      if (currentPointer >= 0) {
        index
      }
      else {
        if (addMode) {
          if (nextAvailablePointer < maxAllowedValues) {
            addAtIndex(index, arr, fromPos, toPos)
            index
          }
          else {
            -2 //max size of table exceeded, cannot add
          }
        }
        else {
          -1 //did not find existing entry
        }
      }
    }
  }

  def hasMoreSpace(): Boolean = {nextAvailablePointer < maxAllowedValues}

  def getAllIds(): Array[Int] = {
    val output = new IntArrayBuffer()
    var i = 0
    while(i < pointersToValues.length){
      if (pointersToValues(i) != -1){
        output += i
      }
      i += 1
    }
    output.toArray()
  }

  /*
  def dumpContents(): Unit = {
    var i = 0
    while(i < pointersToValues.length){
      getStringAtIndex(i) match {
        case None => {} //println("At " + i + ": no word")
        case Some(v) => {
          println("At " + i + ": " + v + " docData: ")
        }
      }
      i += 1
    }
  }
  */

  def getStringAtIndex(index: Int): Option[String] = {
    val internalId = getInternalId(index)
    if (internalId != -1) {
      stringStore.getStringAtIndex(internalId)
    }
    else{
      None
    }
  }

  def getStringAtInternalIndex(internalId: Int): Option[String] = {
    if (internalId != -1) {
      stringStore.getStringAtIndex(internalId)
    }
    else{
      None
    }
  }

}

class AddStringResult{
  var stringId: Int = -1
  var existedPreviously: Boolean = false
}

class String2UniqueIdTable{
  val avgStringLen = 10
  val toleratedNumCollisions = 8
  var settings = StringIdHasherSettings(1024, avgStringLen, toleratedNumCollisions)
  var h = new String2IdHasher(settings)

  def rebuildTable(): Unit = {
    val newSettings = StringIdHasherSettings(2*settings.maxValues, avgStringLen, toleratedNumCollisions)
    val newH = new String2IdHasher(newSettings)

    val numStrings = h.numberOfUniqueStrings()
    var id = 0
    while(id < numStrings){
      val str = h.getStringAtInternalIndex(id).get
      newH.add(str)
      id += 1
    }
    settings = newSettings
    h = newH
  }

  def addString(input: String, output: AddStringResult = null): AddStringResult = {
    if (!h.hasMoreSpace()){
      rebuildTable()
    }
    val existedPreviously = (h.getOrAddId(input, false) >= 0) //work around
    val handle = h.add(input)
    val nonNullOutput = if (output == null) new AddStringResult() else output
    nonNullOutput.stringId = h.getInternalId(handle)
    nonNullOutput.existedPreviously = existedPreviously
    nonNullOutput
  }

  def getStringId(input: String): Int = {
    val id = h.getOrAddId(input, false)
    if (id >= 0){
      h.getInternalId(id)
    }
    else{
      id
    }
  }

  def getStringById(id: Int): String = {
    val optStr = h.getStringAtInternalIndex(id)
    if (optStr.isEmpty){
      throw new IllegalStateException("The string does not exist")
    }
    else{
      optStr.get
    }
  }
}



