package booking

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import booking.repository.{ClassOccupancyRepositoryImpl, ScalikeJdbcSetup}
import org.slf4j.LoggerFactory
import register.proto.{ClassRegisterService, ClassRegisterServiceClient}

import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("booking.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "ClassBookingService")
    try {
      val registerService = registerServiceClient(system)

      init(system, registerService)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }

  def init(system: ActorSystem[_], registerService: ClassRegisterService): Unit = {
    ScalikeJdbcSetup.init(system)

    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    ClassList.init(system)

    val classOccupancyRepository = new ClassOccupancyRepositoryImpl()
    ClassOccupancyProjection.init(system, classOccupancyRepository)

PublishEventsProjection.init(system)
    RegistrationProjection.init(system, registerService)

    val grpcInterface =
      system.settings.config.getString("class-booking-service.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("class-booking-service.grpc.port")
    val grpcService = new ClassBookingServiceImpl(system, classOccupancyRepository)
    ClassBookingServer.start(
      grpcInterface,
      grpcPort,
      system,
      grpcService
    )
  }

  protected def registerServiceClient(
                                    system: ActorSystem[_]): ClassRegisterService = {
    val orderServiceClientSettings =
      GrpcClientSettings
        .connectToServiceAt(
          system.settings.config.getString("class-register-service.host"),
          system.settings.config.getInt("class-register-service.port"))(system)
        .withTls(false)
    ClassRegisterServiceClient(orderServiceClientSettings)(system)
  }
}