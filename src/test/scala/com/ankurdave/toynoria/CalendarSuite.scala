package com.ankurdave.toynoria

import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit

import org.scalatest.FunSuite

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
  frequency: TemporalUnit) extends Record

case class EventCancellation(
  override val id: Id,
  start: ZonedDateTime) extends Record

// Operator results

case class EventOccurrenceWithInfo(
  override val id: Id,
  title: String,
  start: ZonedDateTime,
  duration: Duration) extends Record

class CalendarSuite extends FunSuite {
  test("calendar") {
    val eventInfo = Table[EventInfo]()
    val oneOffEvents = Table[OneOffEvent]()
    val recurringEvents = Table[RecurringEvent]()
    val cancellations = Table[EventCancellation]()

    /** Returns true if query is between start and end, inclusive. */
    def isBetween(query: ZonedDateTime, start: ZonedDateTime, end: ZonedDateTime): Boolean =
      !(start.isAfter(query) || query.isAfter(end))

    /** Returns true if (a, b) completely contains (c, d) */
    def contains(a: ZonedDateTime, b: ZonedDateTime, c: ZonedDateTime, d: ZonedDateTime): Boolean =
      a.isBefore(c) && a.isBefore(d) && b.isAfter(c) && b.isAfter(d)

    def clamp(x: Long, a: Long, b: Long): Long = {
      if (x < a) a
      else if (x > b) b
      else x
    }

    def agenda(start: ZonedDateTime, end: ZonedDateTime): Node[EventOccurrenceWithInfo] = {
      val currentRecurringEventsExploded = Explode[RecurringEvent, OneOffEvent](
        e => {
          val nonEmpty = (isBetween(e.start, start, end)
            || isBetween(e.end, start, end)
            || contains(e.start, e.end, start, end))

          if (nonEmpty) {
            val maxRepetitions = e.start.until(e.end, e.frequency)
            val repetitionsStart = clamp(e.start.until(start, e.frequency), 0, maxRepetitions)
            val repetitionsEnd = clamp(e.start.until(end, e.frequency), 0, maxRepetitions)

            for (i <- repetitionsStart to repetitionsEnd)
            yield OneOffEvent(e.id, e.start.plus(i, e.frequency), e.duration)
          } else {
            Seq.empty
          }
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

    logTrace("Inserting event info")
    eventInfo.handle(Insert(EventInfo(1,
      "Event A - one-off")))
    eventInfo.handle(Insert(EventInfo(2,
      "Event B - recurring every Wednesday at noon in 2017-2019")))
    eventInfo.handle(Insert(EventInfo(3,
      "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03")))

    logTrace("Constructing week agenda")
    val weekAgenda = agenda(
      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ZonedDateTime.of(2018, 1, 8, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")))

    logTrace("Inserting a one-off event at 2018-01-01 9:00 AM")
    oneOffEvents.handle(Insert(OneOffEvent(
      1,
      ZonedDateTime.of(2018, 1, 1, 9, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1))))

    logTrace("Inserting a recurring event every Wednesday at noon in 2017-2019")
    recurringEvents.handle(Insert(RecurringEvent(
      2,
      ZonedDateTime.of(2017, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1),
      ZonedDateTime.of(2019, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ChronoUnit.WEEKS)))

    logTrace("Inserting a recurring event every day at 5 PM from 2018-01-02 to 2018-02-02")
    recurringEvents.handle(Insert(RecurringEvent(
      3,
      ZonedDateTime.of(2018, 1, 2, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1),
      ZonedDateTime.of(2018, 2, 2, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ChronoUnit.DAYS)))

    logTrace("Inserting a cancellation for that event on 2018-01-03")
    cancellations.handle(Insert(EventCancellation(
      3,
      ZonedDateTime.of(2018, 1, 3, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")))))

    logTrace("Querying week agenda")
    assert(weekAgenda.query().toSet ===
      Set(
        EventOccurrenceWithInfo(
          1,
          "Event A - one-off",
          ZonedDateTime.of(2018, 1, 1, 9, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          2,
          "Event B - recurring every Wednesday at noon in 2017-2019",
          ZonedDateTime.of(2018, 1, 3, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          3,
          "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03",
          ZonedDateTime.of(2018, 1, 2, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          3,
          "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03",
          ZonedDateTime.of(2018, 1, 4, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          3,
          "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03",
          ZonedDateTime.of(2018, 1, 5, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          3,
          "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03",
          ZonedDateTime.of(2018, 1, 6, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          3,
          "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03",
          ZonedDateTime.of(2018, 1, 7, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
      ))
  }
}
