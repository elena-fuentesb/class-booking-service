package booking

import akka.actor.typed.ActorSystem
import akka.projection.eventsourced.EventEnvelope
import akka.projection.jdbc.scaladsl.JdbcHandler
import booking.repository.{ClassOccupancyRepository, ScalikeJdbcSession}
import org.slf4j.LoggerFactory

class ClassOccupancyProjectionHandler(
                                       tag: String,
                                       system: ActorSystem[_],
                                       repo: ClassOccupancyRepository)
  extends JdbcHandler[
    EventEnvelope[ClassList.Event],
    ScalikeJdbcSession]() {

  private val log = LoggerFactory.getLogger(getClass)

  override def process(
                        session: ScalikeJdbcSession,
                        envelope: EventEnvelope[ClassList.Event]): Unit = {
    envelope.event match {
      case ClassList.ParticipantAdded(classId, _) =>
        repo.update(session, classId)
        logItemCount(session, classId)


      case ClassList.ParticipantRemoved(classId, _) =>
        repo.remove(session, classId)
        logItemCount(session, classId)


      case _: ClassList.ClosedClass => ???
    }
  }

  private def logItemCount(
                            session: ScalikeJdbcSession,
                            classId: String): Unit = {
    log.info(
      "ClassOccupancyProjectionHandler({}) class occupancy for '{}': [{}]",
      tag,
      classId,
      repo.getItem(session, classId).getOrElse(0))
  }

}
