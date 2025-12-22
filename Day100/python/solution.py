class trienode:
    def __init__(self):
        self.isend=False
        self.words={}

class WordDictionary:

    def __init__(self):
        self.root=trienode()
        
    def addWord(self, word: str) -> None:
        cur=self.root
        for each in word:
            if each not in cur.words:
                cur.words[each]=trienode()
            cur=cur.words[each]
        cur.isend=True

    def search(self, word: str) -> bool:
        def dfs(i,root):
            cur=root
            for j in range(i,len(word)):
                if word[j]=='.':
                    for child in cur.words.values():
                        if dfs(j+1,child):
                            return True
                    return False
                else:
                    if word[j] not in cur.words:
                        return False
                    cur=cur.words[word[j]]
            return cur.isend

        return dfs(0,self.root)


# Your WordDictionary object will be instantiated and called as such:
# obj = WordDictionary()
# obj.addWord(word)
# param_2 = obj.search(word)