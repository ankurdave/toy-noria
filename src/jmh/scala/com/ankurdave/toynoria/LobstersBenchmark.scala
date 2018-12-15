package com.ankurdave.toynoria

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import com.ankurdave.toynoria.examples._

@State(Scope.Thread)
class LobstersBenchmark {
  var nextStoryId: Int = 0

  val r = new scala.util.Random()
  val rs = r.alphanumeric

  val l = new Lobsters()

  @Setup
  def prepare(): Unit = {
    // Insert some data
    for (i <- 0 until 1000) {
      l.stories.handle(Insert(Story(nextStoryId, rs.take(10).mkString)))
      nextStoryId += 1
    }
    for (i <- 0 until 1000) {
      l.votes.handle(Insert(Vote(r.nextInt(nextStoryId), if (r.nextBoolean) +1 else -1)))
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertStory(bh: Blackhole): Unit = {
    l.stories.handle(Insert(Story(nextStoryId, "story title")))
    nextStoryId += 1
    bh.consume(l.topStories.query())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertUpvote(bh: Blackhole): Unit = {
    l.votes.handle(Insert(Vote(r.nextInt(nextStoryId), +1)))
    bh.consume(l.topStories.query())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertDownvote(bh: Blackhole): Unit = {
    l.votes.handle(Insert(Vote(r.nextInt(nextStoryId), -1)))
    bh.consume(l.topStories.query())
  }
}
