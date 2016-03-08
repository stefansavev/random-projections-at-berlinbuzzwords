package com.stefansavev.randomprojections.actors

import java.nio.ByteBuffer
import java.nio.channels.{CompletionHandler, AsynchronousFileChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileAttribute
import java.nio.file.{Files, OpenOption, StandardOpenOption, Paths}
import java.util.Random
import java.util.concurrent.{Executors, ExecutorService, ThreadPoolExecutor}

import akka.actor._
import com.stefansavev.randomprojections.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import scala.collection.mutable


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
case class Read(readerId: Int, fromPosition: Long, toPosition: Long, hook: Array[Byte] => Unit) extends AsyncFileCommand
case class Write(writerId: Int, buffer: Array[Byte], position: Long) extends AsyncFileCommand

trait AsyncFileEvent

case class ReadResult(readRequest: Read, bytes: Array[Byte]) extends AsyncFileEvent
case class WriteResult(writerId: Int, bytesWritten: Int, position: Long) extends AsyncFileEvent
case class CommandFailed(cmd: AsyncFileCommand, cause: Throwable) extends AsyncFileEvent

trait BasicCompletionHandler[A, B] extends CompletionHandler[A, B] with StrictLogging {
  def receiver: ActorRef
  def cmd: AsyncFileCommand

  override def failed(exc: Throwable, attachment: B): Unit = {
    logger.error("Asyncronous read/write command failed", exc)
    receiver ! CommandFailed(cmd, exc)
  }
}

class ReadCompletionHandler(val receiver: ActorRef, dst: ByteBuffer, val cmd: Read) extends BasicCompletionHandler[Integer, AnyRef] {
  override def completed(result: Integer, attachment: AnyRef): Unit = {
    val numBytesRead = result.intValue()
    if (numBytesRead != (cmd.toPosition - cmd.fromPosition).toInt){
      Utils.internalError()
    }
    val bytes = Array.ofDim[Byte](numBytesRead)
    dst.rewind()
    dst.get(bytes)
    receiver ! ReadResult(cmd, bytes)
  }
}

class WriteCompletionHandler(val receiver: ActorRef, val cmd: Write) extends BasicCompletionHandler[Integer, AnyRef] {
  override def completed(result: Integer, attachment: AnyRef): Unit = {
    receiver ! WriteResult(cmd.writerId, result.toInt, cmd.position)
  }
}

class ActorFileReader(file: AsyncReadFile) extends Actor{
  override def receive = {
    case cmd@Read(readerId, fromPosition, toPosition, hook) => {
      val size = (toPosition - fromPosition).toInt
      val dst = ByteBuffer.allocate(size)
      //attachment is ignored
      val handler = new ReadCompletionHandler(sender(), dst, cmd)
      file.channel.read[AnyRef](dst, fromPosition, null, handler)
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

object ActorFileWriterSupervisorMessages{
  trait ActorFileWriterSupervisorMessage
  object WaitUntilDone extends ActorFileWriterSupervisorMessage
  object WaitUntilSlotAvailable extends ActorFileWriterSupervisorMessage
  case class Done(totalBytesWritten: Long) extends ActorFileWriterSupervisorMessage
  case class WritePosition(pos: Long) extends ActorFileWriterSupervisorMessage
}

class FileWriterSupervisor(supervisorActor: ActorRef){
  import ActorFileWriterSupervisorMessages._
  import akka.actor.ActorDSL._
  import akka.actor._
  import akka.util.Timeout
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import akka.pattern._

  def write(writerId: Int, bytes: Array[Byte], pos: Long): WritePosition = {
    implicit val timeout = Timeout(5000 seconds)
    val future = supervisorActor ? Write(writerId, bytes, pos)
    val result = Await.result(future, timeout.duration).asInstanceOf[WritePosition]
    result
  }

  def waitUntilDone(): Done = {
    implicit val timeout = Timeout(5000 seconds)
    val future = supervisorActor ? WaitUntilDone
    val result = Await.result(future, timeout.duration).asInstanceOf[Done]
    result
  }
}

class FileReaderSupervisor(supervisorActor: ActorRef){
  import ActorFileReaderSupervisorMessages._
  import akka.util.Timeout
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import akka.pattern._

  def read(fromPos: Long, toPos: Long, hook: Array[Byte] => Unit): Unit = {
    implicit val timeout = Timeout(5000 seconds)
    val future = supervisorActor ? Read(-1, fromPos, toPos, hook)
    Await.result(future, timeout.duration).asInstanceOf[ScheduledReadMessage.type] //ignore result
    println("finished waiting for read to get scheduled")
  }

  def waitUntilDone(): ReaderDone = {
    implicit val timeout = Timeout(5000 seconds)
    val future = supervisorActor ? ReaderWaitUntilDone
    val result = Await.result(future, timeout.duration).asInstanceOf[ReaderDone]
    println("reader done")
    result
  }
}

class ActorFileWriterSupervisor(maxNumberOfPendingWrites: Int, writers: Array[AsyncFileWriter]) extends Actor with StrictLogging{
  import ActorFileWriterSupervisorMessages._
  import akka.pattern.ask

  var numWritesPending = 0L
  var totalBytesWritten = 0L

  case class ScheduledWrite(sender: ActorRef, writerId: Int, buffer: Array[Byte], position: Long)

  var waiter: Option[ActorRef] = None
  var scheduledWrite: Option[ScheduledWrite] = None

  def closeAll(): Unit = {
    writers.foreach(writer => writer.close(context))
    context.stop(self)
  }

  def scheduleWrite(scheduledWrite: ScheduledWrite): Unit = {
    numWritesPending += 1
    val writerId = scheduledWrite.writerId
    val currentPos = writers(writerId).write(writerId, scheduledWrite.buffer)
    scheduledWrite.sender ! WritePosition(currentPos)
  }

  override def receive = {
    case Write(writerId: Int, buffer: Array[Byte], position: Long) => {
      logger.info(s"Writing at pos ${position}  with pending writes ${numWritesPending}")
      if (numWritesPending > maxNumberOfPendingWrites){
        if (scheduledWrite.isDefined){
          throw new IllegalStateException("One one pending message is allowed")
        }
        scheduledWrite = Some(ScheduledWrite(sender(), writerId, buffer, position))
      }
      else {
        scheduleWrite(ScheduledWrite(sender(), writerId, buffer, position))
      }
    }

    case cmd@WriteResult(writerId: Int, bytesWritten: Int, position: Long) => {
      totalBytesWritten += bytesWritten
      numWritesPending -= 1
      if (scheduledWrite.isDefined){
        scheduleWrite(scheduledWrite.get)
        scheduledWrite = None
      }
      if (numWritesPending == 0 && waiter.isDefined){
        if (scheduledWrite.isDefined){
          throw new IllegalStateException("One one pending message is allowed")
        }

        waiter.get ! Done(totalBytesWritten)
        waiter = None
        closeAll()
      }
    }
    case WaitUntilDone => {
      if (numWritesPending == 0){
        sender() ! Done(totalBytesWritten)
        closeAll()
      }
      else{
        waiter = Some(sender())
      }
    }
  }
}

object ActorFileReaderSupervisorMessages{
  trait ActorFileReaderSupervisorMessage
  object ReaderWaitUntilDone extends ActorFileReaderSupervisorMessage
  object ScheduledReadMessage extends ActorFileReaderSupervisorMessage
  object ReaderWaitUntilSlotAvailable extends ActorFileReaderSupervisorMessage
  case class ReaderDone(totalBytesRead: Long) extends ActorFileReaderSupervisorMessage
}


class ActorFileReaderSupervisor(maxNumberOfPendingReads: Int, readers: Array[AsyncFileReader]) extends Actor{
  import ActorFileReaderSupervisorMessages._

  var numReadsPending = 0L
  var totalBytesRead = 0L

  case class ScheduledRead(sender: ActorRef, readMessage: Read)

  var waiter: Option[ActorRef] = None
  var scheduledRead: Option[ScheduledRead] = None
  val availableReaders = {
    val queue = new mutable.Queue[Int]()
    (0 until maxNumberOfPendingReads).foreach(i => queue.enqueue(i))
    queue
  }

  def closeAll(): Unit = {
    readers.foreach(reader => reader.close(context))
    context.stop(self)
  }

  def scheduleRead(scheduledRead: ScheduledRead): Unit = {
    println("scheduling read with # pending " + numReadsPending)
    numReadsPending += 1
    val readerId = availableReaders.dequeue()
    println("scheduling at reader: " + readerId)
    readers(readerId).read(scheduledRead.readMessage.copy(readerId = readerId))
    scheduledRead.sender ! ScheduledReadMessage
  }

  override def receive = {
    case readMessage@Read(_, fromPosition, toPosition, internalMessage) => {
      println("read message with #reads pending " + numReadsPending)
      if (numReadsPending >= maxNumberOfPendingReads){
        println("queuing read message with #pending " + numReadsPending)
        if (scheduledRead.isDefined){
          throw new IllegalStateException("One one pending message is allowed")
        }
        scheduledRead = Some(ScheduledRead(sender(), readMessage))
      }
      else {
        scheduleRead(ScheduledRead(sender(), readMessage))
      }
    }

    case cmd@ReadResult(readRequest: Read, bytes: Array[Byte]) => {
      if (numReadsPending <= 0){
        Utils.internalError()
      }
      readRequest.hook(bytes) //run the hook (it may block the actor for a while)
      availableReaders.enqueue(readRequest.readerId)
      totalBytesRead += bytes.length
      numReadsPending -= 1

      if (scheduledRead.isDefined){
        println("releasing pending # " + numReadsPending)
        scheduleRead(scheduledRead.get)
        scheduledRead = None
      }

      if (numReadsPending == 0 && waiter.isDefined){
        if (scheduledRead.isDefined){
          throw new IllegalStateException("One one pending message is allowed")
        }
        if (readers(0).size != totalBytesRead){
          Utils.internalError()
        }
        waiter.get ! ReaderDone(totalBytesRead)
        waiter = None
        closeAll()
      }
    }

    case ReaderWaitUntilDone => {
      println("reader wait until done")
      if (waiter.isDefined){
        Utils.internalError()
      }
      if (numReadsPending == 0){
        if (scheduledRead.isDefined){
          Utils.internalError()
        }
        if (readers(0).size != totalBytesRead){
          Utils.internalError()
        }
        sender() ! ReaderDone(totalBytesRead)
        closeAll()
      }
      else{
        waiter = Some(sender())
      }
    }
  }
}

class AsyncFileWriter(fileName: String, system: ActorSystem, pool: ExecutorService, actorNameSuffix: String) extends StrictLogging{
  logger.info(s"Creating file ${fileName} for asyncronous writing")

  val fileWriter = AsyncFile.openWrite(fileName, pool)
  val fileWriterActorProps = Props(classOf[ActorFileWriter], fileWriter)
  val fileWriterActor = system.actorOf(fileWriterActorProps, "FileWriter_" + actorNameSuffix)

  var writePosition = 0L

  private def incWritePosition(numBytes: Int): Long = {
    val prevPosition = writePosition
    writePosition += numBytes
    prevPosition
  }

  def write(writerId: Int, bytes: Array[Byte])(implicit sender: ActorRef): Long = {
    val currentWritePos = incWritePosition(bytes.length)
    fileWriterActor ! Write(writerId, bytes, currentWritePos)
    currentWritePos
  }

  def close(context: ActorContext): Unit = {
    fileWriter.close()
    context.stop(fileWriterActor)
  }
}

class AsyncFileReader(fileName: String, system: ActorSystem, pool: ExecutorService, actorNameSuffix: String){
  val fileReader = AsyncFile.openRead(fileName, pool)
  val size = fileReader.size
  val fileReaderActorProps = Props(classOf[ActorFileReader], fileReader)
  val fileReaderActor = system.actorOf(fileReaderActorProps, "FileReader_" + actorNameSuffix)

  def read(readMessage: Read)(implicit sender: ActorRef): Unit = {
    if (readMessage.fromPosition >= readMessage.toPosition){
      Utils.internalError()
    }
    if (readMessage.toPosition > size){
      Utils.internalError()
    }
    fileReaderActor ! readMessage
  }

  def close(context: ActorContext): Unit = {
    fileReader.close()
    context.stop(fileReaderActor)
  }
}

object Application{
  implicit val system = akka.actor.ActorSystem("High-Dimensional-Search")

  val pool = Executors.newFixedThreadPool(50) //TODO: replace with something else

  def getActorSystem: ActorSystem = {
    system
  }

  def getExecutorService: ExecutorService = {
    pool
  }

  def createAsyncFileWriter(fileName: String, actorSuffix: String): AsyncFileWriter = {
    new AsyncFileWriter(fileName, getActorSystem, getExecutorService, actorSuffix)
  }

  def createAsyncFileReader(fileName: String, actorSuffix: String): AsyncFileReader = {
    new AsyncFileReader(fileName, getActorSystem, getExecutorService, actorSuffix)
  }

  def createWriterSupervisor(suffix: String, maxNumberOfPendingWrites: Int, writers: Array[AsyncFileWriter]): FileWriterSupervisor = {
    val props = Props(classOf[ActorFileWriterSupervisor], maxNumberOfPendingWrites, writers)
    val actorRef = system.actorOf(props, "WriterSupervisor_" + suffix)
    new FileWriterSupervisor(actorRef)
  }

  def createReaderSupervisor(suffix: String, maxNumberOfPendingReads: Int, fileName: String): FileReaderSupervisor = {
    if (maxNumberOfPendingReads < 1){
      Utils.internalError()
    }
    val readers = Array.range(0, maxNumberOfPendingReads).map(i => Application.createAsyncFileReader(fileName, suffix + "Reader_" + i))
    val props = Props(classOf[ActorFileReaderSupervisor], maxNumberOfPendingReads, readers)
    val actorRef = system.actorOf(props, "ReaderSupervisor_" + suffix)
    new FileReaderSupervisor(actorRef)
  }

  def shutdown(): Unit ={
    pool.shutdown()
    system.shutdown()
  }

}



