package com.ankurdave.toynoria.examples

import com.ankurdave.toynoria._

case class Vote(override val id: Id, vote: Int) extends Record

case class Story(override val id: Id, title: String) extends Record

case class StoryWithVoteCount(override val id: Id, title: String, voteCount: Int) extends Record

/**
 * A link aggregator that stores stories that can be upvoted or downvoted and supports retrieving
 * the top stories.
 */
class Lobsters() {
  val votes = Table[Vote]()
  val stories = Table[Story]()
  private val totalVotesByStoryId = Aggregate[Vote](
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
  private val storiesWithVoteCount = Join[Story, Vote, StoryWithVoteCount](
    (story: Story, voteCount: Vote) => {
      assert(story.id == voteCount.id)
      StoryWithVoteCount(story.id, story.title, voteCount.vote)
    },
    stories,
    totalVotesByStoryId)
  val topStories =
    TopK(2, storiesWithVoteCount)(Ordering.by(_.voteCount))

  votes.addParent(totalVotesByStoryId)
  stories.addLeftParent(storiesWithVoteCount)
  totalVotesByStoryId.addRightParent(storiesWithVoteCount)
  storiesWithVoteCount.addParent(topStories)
  // Sec 4.5: Noria disables partial state for operators upstream of full-state descendants. TopK
  // is a full-state descendant of Aggregate.
  totalVotesByStoryId.disablePartialState()
}
