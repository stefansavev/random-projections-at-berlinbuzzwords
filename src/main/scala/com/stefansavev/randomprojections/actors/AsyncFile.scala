package com.stefansavev.randomprojections.actors

import java.nio.ByteBuffer
import java.nio.channels.{CompletionHandler, AsynchronousFileChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileAttribute
import java.nio.file.{Files, OpenOption, StandardOpenOption, Paths}
import java.util.Random
import java.util.concurrent.{Executors, ExecutorService, ThreadPoolExecutor}

import akka.actor.ActorDSL._
import akka.actor._
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask

class AsyncReadFile(val channel: AsynchronousFileChannel, val size: Long){
  def close(): Unit = {
    channel.close()
  }
}

class AsyncWriteFile(val channel: AsynchronousFileChannel){
  def close(): Unit = {
    channel.close()
  }
}

object AsyncFile{

  def openRead(fileName: String, pool: ExecutorService): AsyncReadFile = {
    import scala.collection.JavaConverters._
    val options: java.util.Set[OpenOption] = Set[OpenOption](StandardOpenOption.READ).asJava
    val fileAttributes: Array[FileAttribute[_]] = Array()
    val channel = AsynchronousFileChannel.open(Paths.get(fileName), options, pool, fileAttributes : _*)
    val size = channel.size()
    new AsyncReadFile(channel, size)
  }

  def openWrite(fileName: String, pool: ExecutorService): AsyncWriteFile = {
    import scala.collection.JavaConverters._
    val options: java.util.Set[OpenOption] = Set[OpenOption](StandardOpenOption.CREATE, StandardOpenOption.WRITE).asJava
    val fileAttributes: Array[FileAttribute[_]] = Array()
    val channel = AsynchronousFileChannel.open(Paths.get(fileName), options, pool, fileAttributes : _*)
    new AsyncWriteFile(channel)
  }
}

trait AsyncFileCommand

//case class Write(bytes: ByteString, position: Long) extends Command
case class Read(readerId: Int, size: Int, position: Long) extends AsyncFileCommand
case class Write(writerId: Int, buffer: Array[Byte], position: Long) extends AsyncFileCommand

trait AsyncFileEvent

//case class Written(bytesWritten: Int) extends AsyncFileEvent
case class ReadResult(readerId: Int, bytes: Array[Byte], bytesRead: Int, position: Long) extends AsyncFileEvent
case class WriteResult(writerId: Int, bytesWritten: Int, position: Long) extends AsyncFileEvent
case class CommandFailed(cmd: AsyncFileCommand, cause: Throwable) extends AsyncFileEvent

trait BasicCompletionHandler[A, B] extends CompletionHandler[A, B] {
  def receiver: ActorRef
  def cmd: AsyncFileCommand

  override def failed(exc: Throwable, attachment: B): Unit = receiver ! CommandFailed(cmd, exc)
}

class ReadCompletionHandler(val receiver: ActorRef, dst: ByteBuffer, val cmd: Read) extends BasicCompletionHandler[Integer, AnyRef] {
  override def completed(result: Integer, attachment: AnyRef): Unit = {
    val bytes = Array.ofDim[Byte](result.intValue())
    dst.rewind()
    dst.get(bytes)
    receiver ! ReadResult(cmd.readerId, bytes, result.intValue(), cmd.position)
  }
}

class WriteCompletionHandler(val receiver: ActorRef, val cmd: Write) extends BasicCompletionHandler[Integer, AnyRef] {
  override def completed(result: Integer, attachment: AnyRef): Unit = {
    receiver ! WriteResult(cmd.writerId, result.toInt, cmd.position)
  }
}

class ActorFileReader(file: AsyncReadFile) extends Actor{
  override def receive = {
    case cmd@Read(_, size, position) => {
      val dst = ByteBuffer.allocate(size)
      //attachment is ignored
      file.channel.read[AnyRef](dst, position, null, new ReadCompletionHandler(sender(), dst, cmd))
    }
  }
}

class ActorFileWriter(file: AsyncWriteFile) extends Actor{
  override def receive = {
    case cmd@Write(writerId, buffer, position) => {
      val byteBuffer = ByteBuffer.wrap(buffer)
      //attachment is ignored
      file.channel.write[AnyRef](byteBuffer, position, null, new WriteCompletionHandler(sender(), cmd))
    }
  }
}

class AsyncFileWriter(fileName: String, system: ActorSystem, pool: ExecutorService, actorNameSuffix: String){
  println("creating async file: " + fileName)
  //Files.delete(Paths.get(fileName))
  val fileWriter = AsyncFile.openWrite(fileName, pool)
  val fileWriterActorProps = Props(classOf[ActorFileWriter], fileWriter)
  val fileWriterActor = system.actorOf(fileWriterActorProps, "FileWriter_" + actorNameSuffix)

  var writePosition = 0L

  private def incWritePosition(numBytes: Int): Long = {
    val prevPosition = writePosition
    writePosition += numBytes
    prevPosition
  }

  def write(writerId: Int, bytes: Array[Byte])(implicit sender: ActorRef): Unit = {
    fileWriterActor ! Write(writerId, bytes, incWritePosition(bytes.length))
  }

  def close(context: ActorContext): Unit = {
    fileWriter.close()
    context.stop(fileWriterActor)
  }
}

class AsyncFileReader(fileName: String, system: ActorSystem, pool: ExecutorService, actorNameSuffix: String, startPos: Long = 0, endPos: Long = -1){
  val fileReader = AsyncFile.openRead(fileName, pool)

  val fileReaderActorProps = Props(classOf[ActorFileReader], fileReader)
  val fileReaderActor = system.actorOf(fileReaderActorProps, "FileReader_" + actorNameSuffix)
  var readPosition = startPos
  var adjustedEndPos = if (endPos < 0) fileReader.size else endPos
  //val adjustedFileSize = if (endPos < 0) fileReader.size else Math.min(endPos, fileReader.size)

  def read(readerId: Int, requestedSize: Int)(implicit sender: ActorRef): Boolean = {
    if (readPosition < adjustedEndPos) {
      val adjustedSize = if (readPosition + requestedSize > adjustedEndPos) {
        (adjustedEndPos - readPosition).toInt
      }
      else {
        requestedSize
      }
      val prevReadPosition = readPosition
      readPosition += adjustedSize
      fileReaderActor ! Read(readerId, adjustedSize, prevReadPosition)
      true
    }
    else{
      false
    }
  }

  def close(context: ActorContext): Unit = {
    fileReader.close()
    context.stop(fileReaderActor)
  }
}


object AsyncReaderUtils{
  case class FileSizes(fileSizes: Array[Long])
  case class AsyncReadMultipleResults(readerIndex: Int, bytes: Array[Byte], bytesRead: Int, position: Long)

  def readMultipleFiles(fileNames: Array[String], onFileSizes: FileSizes => Unit, onRead: AsyncReadMultipleResults => Unit): Unit = {

    implicit val system = akka.actor.ActorSystem("test-reader-writer")

    val pool = Executors.newFixedThreadPool(50) //TODO: replace with something else

    val maxNumBytes: Long = 1024*1024*64L //64 MB's
    val readers = fileNames.zipWithIndex.map{case (fileName, index) => {
        new AsyncFileReader(fileName, system, pool, index.toString)
      }}

    val fileSizes = readers.map(_.fileReader.size)
    onFileSizes(FileSizes(fileSizes))

    case object StartReading
    case class ReadNextBytes(readerId: Int)
    case object DoneReading
    val readBufferSize = 1024*1024*1
    var numRemaining = readers.length
    var initialSender: Option[ActorRef] = None
    val loop = actor(new Act {
      become {
        case StartReading => {
          initialSender = Some(sender())
          for(readerId <- 0 until readers.length){
            self ! ReadNextBytes(readerId)
          }
        }
        case ReadNextBytes(readerId) => {
          if (!readers(readerId).read(readerId, readBufferSize)){
            numRemaining -= 1
          }
          if (numRemaining == 0){
            self ! DoneReading
          }
        }
        case ReadResult(readerId, bytes: Array[Byte], bytesRead: Int, position: Long) => {
          println("Read # bytes " + bytes.length + " at position " + position + " from reader: " + readerId)
          onRead(AsyncReadMultipleResults(readerId, bytes, bytesRead, position))
          self ! ReadNextBytes(readerId)
        }
        case DoneReading => {
          readers.foreach(reader => reader.close(context))
          context.stop(self)
          println("done")
          initialSender.get ! "done readering"
          initialSender = None
        }
      }
    })

    implicit val timeout = Timeout(50 seconds)
    val future = loop ? StartReading
    val result = Await.result(future, timeout.duration).asInstanceOf[String]
    println("Got result after waiting: " + result)
    loop ! PoisonPill
    pool.shutdown()
    system.shutdown()
  }
}

object MinTestReadWrite{
  import akka.actor.ActorDSL._
  import akka.actor.{PoisonPill, ActorSystem, ActorRef}

  def main(args: Array[String]) {
    implicit val system = akka.actor.ActorSystem("test-reader-writer")

    val wikipediaFile = "C:/wikipedia-parsed/title_tokens.txt/title_tokens.txt"
    val pool = Executors.newFixedThreadPool(50) //TODO: replace with something else

    val maxNumBytes: Long = 1024*1024*64L //64 MB's
    val reader = new AsyncFileReader(wikipediaFile, system, pool, "1",0, maxNumBytes)
    val writers = Array.range(0, 10).map(i => new AsyncFileWriter("C:/wikipedia-parsed/trashdir/trash" + i + ".txt", system, pool, i.toString))

    case object ReadNextBytes
    case object DoneReading

    val rnd = new Random(8484)
    val loop = actor(new Act {
      become {
        case ReadNextBytes => {
          if (!reader.read(0, 1024*1024*1)){
            self ! DoneReading
          }
        }
        case ReadResult(_, bytes: Array[Byte], bytesRead: Int, position: Long) => {
          println("Read # bytes " + bytes.length + " at position " + position)
          val randomWriterId = rnd.nextInt(writers.length)
          val writer = writers(randomWriterId)
          writer.write(randomWriterId, bytes)
        }
        case WriteResult(writerId, bytesWritten: Int, position: Long) => {
          println("Wrote # bytes " + bytesWritten + " at pos " + position)
          self ! ReadNextBytes
        }
        case DoneReading => {
          reader.close(context)
          writers.foreach(writer => writer.close(context))
          context.stop(self)
          pool.shutdown()
          system.shutdown()
          println("done")
        }
      }
    })
    loop ! ReadNextBytes
  }
}


