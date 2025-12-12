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
        for each in self.followmap[userId]:
            lt.extend(self.tweetmap[each])
        lt.sort(key=lambda x: -x[0])
        return [tweetId for _,tweetId in lt[:10]]
        

    def follow(self, followerId: int, followeeId: int) -> None:
        if followerId != followeeId:
            self.followmap[followerId].add(followeeId)


    def unfollow(self, followerId: int, followeeId: int) -> None:
        self.followmap[followerId].discard(followeeId)

# optimal solution

class Twitter:
    def __init__(self):
        self.count = 0
        self.tweetMap = defaultdict(list)  # userId -> list of [count, tweetIds]
        self.followMap = defaultdict(set)  # userId -> set of followeeId

    def postTweet(self, userId: int, tweetId: int) -> None:
        self.tweetMap[userId].append([self.count, tweetId])
        self.count -= 1

    def getNewsFeed(self, userId: int) -> List[int]:
        res = []
        minHeap = []

        self.followMap[userId].add(userId)
        for followeeId in self.followMap[userId]:
            if followeeId in self.tweetMap:
                index = len(self.tweetMap[followeeId]) - 1
                count, tweetId = self.tweetMap[followeeId][index]
                heapq.heappush(minHeap, [count, tweetId, followeeId, index - 1])

        while minHeap and len(res) < 10:
            count, tweetId, followeeId, index = heapq.heappop(minHeap)
            res.append(tweetId)
            if index >= 0:
                count, tweetId = self.tweetMap[followeeId][index]
                heapq.heappush(minHeap, [count, tweetId, followeeId, index - 1])
        return res

    def follow(self, followerId: int, followeeId: int) -> None:
        self.followMap[followerId].add(followeeId)

    def unfollow(self, followerId: int, followeeId: int) -> None:
        if followeeId in self.followMap[followerId]:
            self.followMap[followerId].remove(followeeId)