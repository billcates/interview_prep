class TrieNode:
    def __init__(self):
        self.words={}
        self.isend=False

class WordDictionary:

    def __init__(self):
        self.head=TrieNode()

    def addWord(self, word: str) -> None:
        tmp=self.head
        for each in word:
            if each not in tmp.words:
                tmp.words[each]=TrieNode()
            tmp=tmp.words[each]
        tmp.isend=True        

    def search(self, word: str) -> bool:
        def dfs(idx, node):
            cur=node
            for i in range(idx,len(word)):
                if word[i] =='.':
                    for child in cur.words.values():
                        if dfs(i+1,child):
                            return True
                    return False
                if word[i] not in cur.words:
                    return False
                cur=cur.words[word[i]]
            return cur.isend
        return dfs(0, self.head)
        


# Your WordDictionary object will be instantiated and called as such:
# obj = WordDictionary()
# obj.addWord(word)
# param_2 = obj.search(word)