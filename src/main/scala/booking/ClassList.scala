package booking

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}

import java.time.Instant
import scala.concurrent.duration._

object ClassList {

  final case class State(participants: Seq[String], closeDate: Option[Instant])
    extends BookingCborSerializable {
    def isClosed: Boolean = closeDate.exists(_.isBefore(Instant.now()))

    def isEmpty = participants.isEmpty

    def hasParticipant(name: String) = participants.contains(name)

    def addParticipant(name: String): State =
      copy(participants = name +: participants)

    def removeParticipant(name: String): State =
      copy(participants = participants diff Seq(name))

    def closeClass: State =
      copy(closeDate = Some(Instant.now()))

    def toSummary: Summary =
      Summary(participants, isClosed)
  }

  object State {
    val empty =
      State(participants = Nil, closeDate = None)
  }

  final case class Summary(participants: Seq[String], closed: Boolean)
    extends BookingCborSerializable


  sealed trait Event extends BookingCborSerializable {
    def classId: String
  }

  final case class ParticipantAdded(classId: String, name: String)
    extends Event

  final case class ParticipantRemoved(classId: String, name: String)
    extends Event

  final case class ClosedClass(classId: String) extends Event

  sealed trait Command extends BookingCborSerializable

  final case class Get(replyTo: ActorRef[Summary]) extends Command

  final case class AddParticipant(classId: String, name: String, replyTo: ActorRef[StatusReply[Summary]])
    extends Command

  final case class RemoveParticipant(classId: String, name: String, replyTo: ActorRef[StatusReply[Summary]])
    extends Command

  final case class CloseClass(replyTo: ActorRef[StatusReply[Summary]])
    extends Command

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("ClassList")

  val tags = Vector.tabulate(5)(i => s"lists-$i")

  def init(system: ActorSystem[_]): Unit = {
    val behaviorFactory: EntityContext[Command] => Behavior[Command] = {
      entityContext =>
        val i = math.abs(entityContext.entityId.hashCode % tags.size)
        val selectedTag = tags(i)
        ClassList(entityContext.entityId, selectedTag)
    }
    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
  }

  def apply(cartId: String, projectionTag: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty,
        commandHandler =
          (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withTagger(_ => Set(projectionTag))
      .withRetention(RetentionCriteria
        .snapshotEvery(numberOfEvents = 100, keepNSnapshots = 3))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }

  private def handleCommand(
                             classId: String,
                             state: State,
                             command: Command): ReplyEffect[Event, State] = {
    if (state.isClosed)
      closedClassList(classId, state, command)
    else
      openClassList(classId, state, command)
  }

  private def openClassList(
                             classId: String,
                             state: State,
                             command: Command): ReplyEffect[Event, State] = {
    command match {

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)

      case AddParticipant(classId, name, replyTo) =>
        if (state.hasParticipant(name))
          Effect.reply(replyTo)(
            StatusReply.Error(
              s"Participant '$name' was already added to this class"))
        else
          Effect
            .persist(ParticipantAdded(classId, name))
            .thenReply(replyTo) { updatedList =>
              StatusReply.Success(updatedList.toSummary)
            }

      case RemoveParticipant(classId, name, replyTo) =>
        Effect
          .persist(ParticipantRemoved(classId, name))
          .thenReply(replyTo) { updatedList =>
            StatusReply.Success(updatedList.toSummary)
          }

      case CloseClass(replyTo) =>
        Effect
          .persist(ClosedClass(classId))
          .thenReply(replyTo) { updatedList =>
            StatusReply.Success(updatedList.toSummary)
          }
    }
  }

  private def closedClassList(
                               classId: String,
                                      state: State,
                                      command: Command): ReplyEffect[Event, State] = {
    command match {

      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)

      case cmd: AddParticipant =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't add participant to closed class"))

      case cmd: RemoveParticipant =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't remove participant from closed class"))

      case cmd: CloseClass =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't close a closed class."))
    }
  }

  private def handleEvent(state: State, event: Event): State = {
    event match {
      case ParticipantAdded(_, name) =>
        state.addParticipant(name)

      case ParticipantRemoved(_, name) =>
        state.removeParticipant(name)

      case ClosedClass(_) =>
        state.closeClass
    }
  }
}
