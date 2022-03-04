package booking

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.persistence.query.Offset
import akka.projection.{ProjectionBehavior, ProjectionId}
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.scaladsl.{AtLeastOnceProjection, SourceProvider}
import booking.repository.ScalikeJdbcSession
import register.proto.ClassRegisterService

object RegistrationProjection {

  def init(system: ActorSystem[_], classRegisterService: ClassRegisterService): Unit = {
    ShardedDaemonProcess(system).init(
      name = "RegistrationProjection",
      ClassList.tags.size,
      index =>
        ProjectionBehavior(createProjectionFor(system, classRegisterService, index)),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop))
  }

  private def createProjectionFor(
                                   system: ActorSystem[_],
                                   classRegisterService: ClassRegisterService,
                                   index: Int)
  : AtLeastOnceProjection[Offset, EventEnvelope[ClassList.Event]] = {
    val tag = ClassList.tags(index)
    val sourceProvider
    : SourceProvider[Offset, EventEnvelope[ClassList.Event]] =
      EventSourcedProvider.eventsByTag[ClassList.Event](
        system = system,
        readJournalPluginId = JdbcReadJournal.Identifier,
        tag = tag)

    JdbcProjection.atLeastOnceAsync(
      projectionId = ProjectionId("RegistrationProjection", tag),
      sourceProvider,
      handler = () => new RegistrationProjectionHandler(system, classRegisterService),
      sessionFactory = () => new ScalikeJdbcSession())(system)
  }
}
