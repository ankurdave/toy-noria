package com.ankurdave.toynoria

import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import org.scalatest.FunSuite

import com.ankurdave.toynoria.examples._

class CalendarSuite extends FunSuite {
  test("calendar") {
    val c = new Calendar()

    logTrace("Inserting event info")
    c.eventInfo.handle(Insert(EventInfo(1,
      "Event A - one-off")))
    c.eventInfo.handle(Insert(EventInfo(2,
      "Event B - recurring every Wednesday at noon in 2017-2019")))
    c.eventInfo.handle(Insert(EventInfo(3,
      "Event C - recurring every day at 5 PM from 2018-01-02 to 2018-02-02 except 2018-01-03")))
    c.eventInfo.handle(Insert(EventInfo(4,
      "Event D - recurring every Thursday at noon from 2017-01-05 to 2018-01-04")))
    c.eventInfo.handle(Insert(EventInfo(5,
      "Event E - covers the entire week")))

    logTrace("Constructing week agenda")
    val weekAgenda = c.agenda(
      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ZonedDateTime.of(2018, 1, 8, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")))

    logTrace("Inserting a one-off event at 2018-01-01 9:00 AM")
    c.oneOffEvents.handle(Insert(OneOffEvent(
      1,
      ZonedDateTime.of(2018, 1, 1, 9, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1))))

    logTrace("Inserting a recurring event every Wednesday at noon in 2017-2019")
    c.recurringEvents.handle(Insert(RecurringEvent(
      2,
      ZonedDateTime.of(2017, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1),
      ZonedDateTime.of(2019, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ChronoUnit.WEEKS)))

    logTrace("Inserting a recurring event every day at 5 PM from 2018-01-02 to 2018-02-02")
    c.recurringEvents.handle(Insert(RecurringEvent(
      3,
      ZonedDateTime.of(2018, 1, 2, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1),
      ZonedDateTime.of(2018, 2, 2, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ChronoUnit.DAYS)))

    logTrace("Inserting a cancellation for that event on 2018-01-03")
    c.cancellations.handle(Insert(EventCancellation(
      3,
      ZonedDateTime.of(2018, 1, 3, 17, 0, 0, 0, ZoneId.of("America/Los_Angeles")))))

    logTrace("Inserting a recurring event every Thursday at noon from 2017-01-05 to 2018-01-04")
    c.recurringEvents.handle(Insert(RecurringEvent(
      4,
      ZonedDateTime.of(2017, 1, 5, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofHours(1),
      ZonedDateTime.of(2018, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      ChronoUnit.WEEKS)))

    logTrace("Inserting a one-off event that covers the entire week")
    c.oneOffEvents.handle(Insert(OneOffEvent(
      5,
      ZonedDateTime.of(2017, 12, 31, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
      Duration.ofDays(10))))

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
        EventOccurrenceWithInfo(
          4,
          "Event D - recurring every Thursday at noon from 2017-01-05 to 2018-01-04",
          ZonedDateTime.of(2018, 1, 4, 12, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofHours(1)),
        EventOccurrenceWithInfo(
          5,
          "Event E - covers the entire week",
          ZonedDateTime.of(2017, 12, 31, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles")),
          Duration.ofDays(10))))
  }
}
