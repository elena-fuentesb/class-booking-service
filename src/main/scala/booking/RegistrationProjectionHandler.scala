package booking

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.util.Timeout
import org.slf4j.LoggerFactory
import register.proto.{ClassBooking, ClassRegisterService}

import scala.concurrent.{ExecutionContext, Future}

class RegistrationProjectionHandler (
                                      system: ActorSystem[_],
                                      classRegisterService: ClassRegisterService)
  extends Handler[EventEnvelope[ClassList.Event]] {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext =
    system.executionContext

  private val sharding = ClusterSharding(system)
  implicit private val timeout: Timeout =
    Timeout.create(
      system.settings.config.getDuration("class-register-service.ask-timeout"))

  override def process(
                        envelope: EventEnvelope[ClassList.Event]): Future[Done] = {
    envelope.event match {
      case closeClass: ClassList.ClosedClass =>
        close(closeClass)

      case _ =>
        // this projection is only interested in CheckedOut events
        Future.successful(Done)
    }

  }

  private def close(checkout: ClassList.ClosedClass): Future[Done] = {
    val entityRef =
      sharding.entityRefFor(ClassList.EntityKey, checkout.classId)
    entityRef.ask(ClassList.Get).flatMap { classInfo =>
      log.info(
        "Closing class {} with the following participants: {}.",
        checkout.classId,
        classInfo.participants.mkString(", "))
      val orderReq = ClassBooking(checkout.classId, classInfo.participants.mkString(", "))
      classRegisterService.completeClass(orderReq).map(_ => Done)
    }
  }

}
