package booking

import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.util.Timeout
import booking.proto.RegisterRequest
import booking.repository.{ClassOccupancyRepository, ScalikeJdbcSession}
import io.grpc.Status
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeoutException
import scala.concurrent.{ExecutionContext, Future}

class ClassBookingServiceImpl(system: ActorSystem[_],
                              classOccupancyRepository: ClassOccupancyRepository) extends proto.ClassBookingService {

  import system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)
  private val sharding = ClusterSharding(system)

  implicit private val timeout: Timeout =
    Timeout.create(
      system.settings.config.getDuration("class-register-service.ask-timeout"))

  private val blockingJdbcExecutor: ExecutionContext =
    system.dispatchers.lookup(
      DispatcherSelector
        .fromConfig("akka.projection.jdbc.blocking-jdbc-dispatcher")
    )
  override def addParticipant(in: RegisterRequest): Future[proto.ClassList] = {
    logger.info("addParticipant {} to class {}", in.participant, in.classId)
    val entityRef = sharding.entityRefFor(ClassList.EntityKey, in.classId)
    val reply: Future[ClassList.Summary] =
      entityRef.askWithStatus(ClassList.AddParticipant(in.classId, in.participant.head.name, _))
    val response = reply.map(classList => toProtoClassList(classList))
    convertError(response)
  }

  override def closeClass(in: booking.proto.CloseRequest): scala.concurrent.Future[booking.proto.ClassList] = {
    logger.info("close class {}", in.classId)
    val entityRef = sharding.entityRefFor(ClassList.EntityKey, in.classId)
    val reply: Future[ClassList.Summary] =
      entityRef.askWithStatus(ClassList.CloseClass(_))
    val response = reply.map(classList => toProtoClassList(classList))
    convertError(response)
  }

  override def get(in: booking.proto.GetClassListRequest): scala.concurrent.Future[booking.proto.ClassList] = {
    logger.info("getClassList {}", in.classId)
    val entityRef = sharding.entityRefFor(ClassList.EntityKey, in.classId)
    val response =
      entityRef.ask(ClassList.Get).map { classList => toProtoClassList(classList) }
    convertError(response)
  }

  override def removeRequest(in: booking.proto.RegisterRequest): scala.concurrent.Future[booking.proto.ClassList] = {
    logger.info("remove participant {} from class {}", in.participant, in.classId)
    val entityRef = sharding.entityRefFor(ClassList.EntityKey, in.classId)
    val response = entityRef.askWithStatus(ClassList.RemoveParticipant(in.classId, in.participant.head.name, _))
      .map(classList => toProtoClassList(classList))
    convertError(response)
  }


  private def toProtoClassList(classList: ClassList.Summary): proto.ClassList = {
    val participants: Seq[proto.Participant] = classList.participants.map(proto.Participant(_))
    proto.ClassList(participants, classList.closed)
  }

  private def convertError[T](response: Future[T]): Future[T] = {
    response.recoverWith {
      case _: TimeoutException =>
        Future.failed(
          new GrpcServiceException(
            Status.UNAVAILABLE.withDescription("Operation timed out")))
      case exc =>
        Future.failed(
          new GrpcServiceException(
            Status.INVALID_ARGUMENT.withDescription(exc.getMessage)))
    }
  }

  override def getClassOccupancyPercentage(in: proto.GetClassOccupancyRequest) : Future[proto.ClassOccupancyResponse] = {
    val spotsAvailable = 20
    Future {
      ScalikeJdbcSession.withSession { session =>
        classOccupancyRepository.getItem(session, in.classId)
      }
    }(blockingJdbcExecutor).map {
      case Some(count) =>
        val percentage = (count / spotsAvailable)* 100
        println(s"percentage $percentage")
        proto.ClassOccupancyResponse(in.classId, percentage)
      case None =>
        proto.ClassOccupancyResponse(in.classId, 0)
    }
  }
}
