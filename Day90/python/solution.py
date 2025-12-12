class Twitter:

    def __init__(self):
        self.followmap=defaultdict(set)
        self.tweetmap=defaultdict(list)
        self.time = 0

    def postTweet(self, userId: int, tweetId: int) -> None:
        self.time+=1
        if userId in self.tweetmap:
            self.tweetmap[userId].append([self.time,tweetId])
        else:
            self.tweetmap[userId]=[[self.time,tweetId]]

    def getNewsFeed(self, userId: int) -> List[int]:

        lt = self.tweetmap[userId][:]
        print(lt)
        for each in self.followmap[userId]:
            lt.extend(self.tweetmap[each])
        lt.sort(key=lambda x: -x[0])
        return [tweetId for _,tweetId in lt[:10]]
        

    def follow(self, followerId: int, followeeId: int) -> None:
        print(f"insied follow, {followerId}")
        if followerId != followeeId:
            self.followmap[followerId].add(followeeId)


    def unfollow(self, followerId: int, followeeId: int) -> None:
        self.followmap[followerId].discard(followeeId)
