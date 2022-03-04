
package booking

import akka.Done
import akka.actor.typed.ActorSystem
import akka.kafka.scaladsl.SendProducer
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import com.google.protobuf.any.{Any => ScalaPBAny}
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class PublishEventsProjectionHandler(
    system: ActorSystem[_],
    topic: String,
    sendProducer: SendProducer[String, Array[Byte]]) 
    extends Handler[EventEnvelope[ClassList.Event]] {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext =
    system.executionContext

  override def process(
      envelope: EventEnvelope[ClassList.Event]): Future[Done] = {
    val event = envelope.event

    // using the classId as the key and `DefaultPartitioner` will select partition based on the key
    // so that events for same classList always ends up in same partition
    val key = event.classId
    val producerRecord = new ProducerRecord(topic, key, serialize(event))
    // 	SendProducer comes from the Kafka connector in Alpakka
    val result = sendProducer.send(producerRecord).map { recordMetadata =>
      log.info(
        "Published event [{}] to topic/partition {}/{}",
        event,
        topic,
        recordMetadata.partition)
      Done
    }
    result
  }

  private def serialize(event: ClassList.Event): Array[Byte] = {
    val protoMessage = event match {
      case ClassList.ParticipantAdded(classId, name) =>
        proto.ParticipantAdded(classId, name)
      case ClassList.ParticipantRemoved(classId, name) =>
        proto.ParticipantRemoved(classId, name)
      
      case ClassList.ClosedClass(classId) =>
        proto.ClosedClass(classId)
    }
    // pack in Any so that type information is included for deserialization
    ScalaPBAny.pack(protoMessage, "class-booking-service").toByteArray
  }
}

