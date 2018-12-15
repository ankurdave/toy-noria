package com.ankurdave.toynoria

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import com.ankurdave.toynoria.examples._

@State(Scope.Thread)
class CalendarBenchmark {
  var nextEventId: Int = 0

  val r = new scala.util.Random()
  val rs = r.alphanumeric

  val zone = ZoneId.of("America/Los_Angeles")
  def randomDate(): ZonedDateTime =
    ZonedDateTime.ofInstant(Instant.ofEpochSecond(r.nextInt(Int.MaxValue)), zone)

  def randomDatePair(): (ZonedDateTime, ZonedDateTime) = {
    val a = randomDate()
    val b = randomDate()
    if (a.isBefore(b)) (a, b) else (b, a)
  }

  def randomDuration(): Duration =
    Duration.ofHours(r.nextInt(10000))

  val temporalUnits = Seq(
    ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.MONTHS, ChronoUnit.WEEKS, ChronoUnit.YEARS)
  def randomTemporalUnit(): TemporalUnit =
    temporalUnits(r.nextInt(temporalUnits.size))

  val c = new Calendar()

  @Setup
  def prepare(): Unit = {
    // Insert some data
    for (i <- 0 until 1000) {
      c.eventInfo.handle(Insert(EventInfo(nextEventId, "one-off " + rs.take(10).mkString)))
      nextEventId += 1
    }
    val maxOneOffEventId = nextEventId
    def randomOneOffEventId(): Int =
      r.nextInt(maxOneOffEventId)

    for (i <- 0 until 1000) {
      c.eventInfo.handle(Insert(EventInfo(nextEventId, "recurring " + rs.take(10).mkString)))
      nextEventId += 1
    }
    val maxRecurringEventId = nextEventId
    def randomRecurringEventId(): Int =
      r.nextInt(maxRecurringEventId - maxOneOffEventId) + maxOneOffEventId

    for (i <- 0 until 1000) {
      c.oneOffEvents.handle(Insert(OneOffEvent(
        randomOneOffEventId(),
        randomDate(),
        randomDuration())))
    }
    for (i <- 0 until 1000) {
      val (start, end) = randomDatePair()
      c.recurringEvents.handle(Insert(RecurringEvent(
        randomRecurringEventId(),
        start,
        randomDuration(),
        end,
        randomTemporalUnit())))
    }
    for (i <- 0 until 1000) {
      // Pick a random recurrence of a random event and cancel it
      for {
        event <- c.recurringEvents.query(randomRecurringEventId())
        if event.numRepetitions > 0
        recurrence = event.repetition(r.nextInt(event.numRepetitions.toInt))
      } {
        c.cancellations.handle(Insert(EventCancellation(
          recurrence.id,
          recurrence.start)))
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def agenda(bh: Blackhole): Unit = {
    c.clear()
    val randomAgendaView = Function.tupled(c.agenda _)(randomDatePair())
    bh.consume(randomAgendaView.query())
  }
}
