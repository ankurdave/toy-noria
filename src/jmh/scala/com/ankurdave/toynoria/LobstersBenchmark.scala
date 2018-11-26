package com.ankurdave.toynoria

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

case class Vote(override val id: Id, vote: Int) extends Record

case class Story(override val id: Id, title: String) extends Record

case class StoryWithVoteCount(override val id: Id, title: String, voteCount: Int) extends Record

@State(Scope.Thread)
class LobstersBenchmark {
  val votes = Table[Vote]()
  val stories = Table[Story]()
  var topStories: TopK[StoryWithVoteCount] = null

  var nextStoryId: Int = 0

  val r = new scala.util.Random()
  val rs = r.alphanumeric

  @Setup
  def prepare(): Unit = {
    val totalVotesByStoryId = Aggregate[Vote](
      id => Vote(id, 0),
      (a, b) => {
        assert(a.id == b.id)
        Vote(a.id, a.vote + b.vote)
      },
      (a, b) => {
        assert(a.id == b.id)
        Vote(a.id, a.vote - b.vote)
      },
      votes)
    val storiesWithVoteCount = Join[Story, Vote, StoryWithVoteCount](
      (story: Story, voteCount: Vote) => {
        assert(story.id == voteCount.id)
        StoryWithVoteCount(story.id, story.title, voteCount.vote)
      },
      stories,
      totalVotesByStoryId)
    topStories =
      TopK(2, storiesWithVoteCount)(Ordering.by(_.voteCount))

    votes.addParent(totalVotesByStoryId)
    stories.addLeftParent(storiesWithVoteCount)
    totalVotesByStoryId.addRightParent(storiesWithVoteCount)
    storiesWithVoteCount.addParent(topStories)
    // Sec 4.5: Noria disables partial state for operators upstream of full-state descendants. TopK
    // is a full-state descendant of Aggregate.
    totalVotesByStoryId.disablePartialState()

    // Insert some data
    for (i <- 0 until 1000) {
      stories.handle(Insert(Story(nextStoryId, rs.take(10).mkString)))
      nextStoryId += 1
    }
    for (i <- 0 until 1000) {
      votes.handle(Insert(Vote(r.nextInt(nextStoryId), if (r.nextBoolean) +1 else -1)))
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertStory(bh: Blackhole): Unit = {
    stories.handle(Insert(Story(nextStoryId, "story title")))
    nextStoryId += 1
    bh.consume(topStories.query())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertUpvote(bh: Blackhole): Unit = {
    votes.handle(Insert(Vote(r.nextInt(nextStoryId), +1)))
    bh.consume(topStories.query())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def insertDownvote(bh: Blackhole): Unit = {
    votes.handle(Insert(Vote(r.nextInt(nextStoryId), -1)))
    bh.consume(topStories.query())
  }
}
