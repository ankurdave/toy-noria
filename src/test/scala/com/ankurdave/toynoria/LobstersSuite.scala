package com.ankurdave.toynoria

import org.scalatest.FunSuite

case class Vote(storyId: Id, vote: Int)

case class Story(id: Id, title: String) {
  override def equals(other: Any): Boolean = other match {
    case Story(otherId, _) => id == otherId
    case _ => false
  }
  override def hashCode: Int = id.hashCode
}

case class StoryWithVoteCount(id: Id, title: String, voteCount: Int) {
  override def equals(that: Any): Boolean = that match {
    case StoryWithVoteCount(otherId, _, _) => id == otherId
    case _ => false
  }
  override def hashCode: Int = id.hashCode
}

class LobstersSuite extends FunSuite {
  test("top stories") {
    val votes = Table[Vote]()
    val stories = Table[Story]()
    val totalVotesByStoryId = Aggregate[Vote, Int](
      (vote: Vote) => (vote.storyId, vote.vote),
      _ + _,
      _ - _,
      votes)
    val storiesWithVoteCount = Join[Story, Story, AggResult[Int], Int, StoryWithVoteCount](
      (story: Story) => (story.id, story),
      (aggResult: AggResult[Int]) => (aggResult.id, aggResult.a),
      (storyId, story: Story, voteCount: Int) => StoryWithVoteCount(
        story.id, story.title, voteCount),
      (storyId, story: Story) => story.id == storyId,
      (storyId, aggResult: AggResult[Int]) => storyId == aggResult.id,
      stories,
      totalVotesByStoryId)
    val topStories =
      TopK(2, storiesWithVoteCount)(Ordering.by(_.voteCount))

    votes.addParent(totalVotesByStoryId)
    stories.addLeftParent(storiesWithVoteCount)
    totalVotesByStoryId.addRightParent(storiesWithVoteCount)
    storiesWithVoteCount.addParent(topStories)

    stories.handle(Insert(Story(1, "Story A")))
    stories.handle(Insert(Story(2, "Story B")))
    stories.handle(Insert(Story(3, "Story C")))

    votes.handle(Insert(Vote(1, +1)))
    votes.handle(Insert(Vote(1, +1)))
    votes.handle(Insert(Vote(1, +1)))
    votes.handle(Insert(Vote(2, -1)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(2, "Story B", -1),
        StoryWithVoteCount(1, "Story A", 3)).map(_.toString))

    votes.handle(Insert(Vote(2, +1)))
    votes.handle(Insert(Vote(2, +1)))
    votes.handle(Insert(Vote(3, +100)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 100)).map(_.toString))
  }
}
