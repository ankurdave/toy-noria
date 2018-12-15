package com.ankurdave.toynoria.examples

import java.time.Duration
import java.time.ZonedDateTime
import java.time.temporal.TemporalUnit

import com.ankurdave.toynoria._

// Base tables

case class EventInfo(
  override val id: Id,
  title: String) extends Record

case class OneOffEvent(
  override val id: Id,
  start: ZonedDateTime,
  duration: Duration) extends Record

case class RecurringEvent(
  override val id: Id,
  start: ZonedDateTime,
  duration: Duration,
  end: ZonedDateTime,
  frequency: TemporalUnit) extends Record {

  def numRepetitions: Long = start.until(end, frequency)
  def repetition(i: Long): OneOffEvent =
    OneOffEvent(id, start.plus(i, frequency), duration)
}

case class EventCancellation(
  override val id: Id,
  start: ZonedDateTime) extends Record

// Operator results

case class EventOccurrenceWithInfo(
  override val id: Id,
  title: String,
  start: ZonedDateTime,
  duration: Duration) extends Record

/**
 * A calendar that stores one-off and recurring events and supports retrieving the events occurring
 * within a given interval.
 */
class Calendar() {
  val eventInfo = Table[EventInfo]()
  val oneOffEvents = Table[OneOffEvent]()
  val recurringEvents = Table[RecurringEvent]()
  val cancellations = Table[EventCancellation]()

  def clear(): Unit = {
    for (t <- Seq(eventInfo, oneOffEvents, recurringEvents, cancellations)) {
      eventInfo.clearParents()
      oneOffEvents.clearParents()
      recurringEvents.clearParents()
      cancellations.clearParents()
    }
  }

  def agenda(start: ZonedDateTime, end: ZonedDateTime): Node[EventOccurrenceWithInfo] = {
    val currentRecurringEventsExploded = Explode[RecurringEvent, OneOffEvent](
      e => {
        val repetitionsStart = clamp(e.start.until(start, e.frequency), 0, e.numRepetitions)
        val repetitionsEnd = clamp(e.start.until(end, e.frequency), 0, e.numRepetitions)
        for (i <- repetitionsStart to repetitionsEnd) yield e.repetition(i)
      },
      recurringEvents)

    val currentRecurringEventsMinusCancellations =
      Antijoin[OneOffEvent, EventCancellation](
        (e, cancellations) => !cancellations.exists(c => e.start == c.start),
        currentRecurringEventsExploded,
        cancellations)

    val candidateEvents = Union[OneOffEvent](
      oneOffEvents, currentRecurringEventsMinusCancellations)

    val currentEvents = Filter[OneOffEvent](
      e => isBetween(e.start, start, end) || isBetween(e.start.plus(e.duration), start, end)
        || contains(e.start, e.start.plus(e.duration), start, end),
      candidateEvents)

    val currentEventsWithInfo = Join[OneOffEvent, EventInfo, EventOccurrenceWithInfo](
      (e, ei) => EventOccurrenceWithInfo(e.id, ei.title, e.start, e.duration),
      currentEvents,
      eventInfo)

    recurringEvents.addParent(currentRecurringEventsExploded)
    currentRecurringEventsExploded.addLeftParent(currentRecurringEventsMinusCancellations)
    cancellations.addRightParent(currentRecurringEventsMinusCancellations)
    oneOffEvents.addLeftParent(candidateEvents)
    currentRecurringEventsMinusCancellations.addRightParent(candidateEvents)
    candidateEvents.addParent(currentEvents)
    currentEvents.addLeftParent(currentEventsWithInfo)
    eventInfo.addRightParent(currentEventsWithInfo)

    currentEventsWithInfo
  }

  /** Returns true if query is between start and end, inclusive. */
  private def isBetween(query: ZonedDateTime, start: ZonedDateTime, end: ZonedDateTime): Boolean =
    !(start.isAfter(query) || query.isAfter(end))

  /** Returns true if (a, b) completely contains (c, d) */
  private def contains(a: ZonedDateTime, b: ZonedDateTime, c: ZonedDateTime, d: ZonedDateTime): Boolean =
    a.isBefore(c) && a.isBefore(d) && b.isAfter(c) && b.isAfter(d)

  private def clamp(x: Long, a: Long, b: Long): Long = {
    if (x < a) a
    else if (x > b) b
    else x
  }

}
