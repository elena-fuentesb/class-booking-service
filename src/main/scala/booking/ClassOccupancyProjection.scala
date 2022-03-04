package booking

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.scaladsl.{ExactlyOnceProjection, SourceProvider}
import akka.projection.{ProjectionBehavior, ProjectionId}
import booking.repository.{ClassOccupancyRepository, ScalikeJdbcSession}

object ClassOccupancyProjection {
  def init(
            system: ActorSystem[_],
            repository: ClassOccupancyRepository): Unit = {
    ShardedDaemonProcess(system).init(
      name = "ClassOccupancyProjection",
      ClassList.tags.size,
      index =>
        ProjectionBehavior(createProjectionFor(system, repository, index)),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop))
  }


  private def createProjectionFor(
                                   system: ActorSystem[_],
                                   repository: ClassOccupancyRepository,
                                   index: Int)
  : ExactlyOnceProjection[Offset, EventEnvelope[ClassList.Event]] = {
    val tag = ClassList.tags(index)

    val sourceProvider
    : SourceProvider[Offset, EventEnvelope[ClassList.Event]] =
      EventSourcedProvider.eventsByTag[ClassList.Event](
        system = system,
        readJournalPluginId = JdbcReadJournal.Identifier,
        tag = tag)

    JdbcProjection.exactlyOnce(
      projectionId = ProjectionId("ClassOccupancyProjection", tag),
      sourceProvider,
      handler = () =>
        new ClassOccupancyProjectionHandler(tag, system, repository),
      sessionFactory = () => new ScalikeJdbcSession())(system)
  }
}
