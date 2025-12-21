class trienode:
    def __init__(self):
        self.isend=False
        self.words={}

class Trie:

    def __init__(self):
        self.trie=trienode()
        
    def insert(self, word: str) -> None:
        cur=self.trie
        for each in word:
            if each not in cur.words:
                cur.words[each]=trienode()
            cur=cur.words[each]
        cur.isend=True

    def search(self, word: str) -> bool:
        cur=self.trie
        for each in word:
            if each not in cur.words:
                return False
            cur=cur.words[each]
        return cur.isend 
        
    def startsWith(self, prefix: str) -> bool:
        cur=self.trie
        for each in prefix:
            if each not in cur.words:
                return False
            cur=cur.words[each]
        return True
        


# Your Trie object will be instantiated and called as such:
# obj = Trie()
# obj.insert(word)
# param_2 = obj.search(word)
# param_3 = obj.startsWith(prefix)