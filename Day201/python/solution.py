class TrieNode:
    def __init__(self):
        self.words={}
        self.isend=False

class Trie:

    def __init__(self):
        self.head=TrieNode()
        

    def insert(self, word: str) -> None:
        tmp=self.head
        for each in word:
            if each not in tmp.words:
                tmp.words[each]=TrieNode()
            tmp=tmp.words[each]
        tmp.isend=True
        

    def search(self, word: str) -> bool:
        tmp=self.head
        for each in word:
            if each not in tmp.words:
                return False
            tmp=tmp.words[each]
        return tmp.isend
        
    def startsWith(self, prefix: str) -> bool:
        tmp=self.head
        for each in prefix:
            if each not in tmp.words:
                return False
            tmp=tmp.words[each]
        return True
        


# Your Trie object will be instantiated and called as such:
# obj = Trie()
# obj.insert(word)
# param_2 = obj.search(word)
# param_3 = obj.startsWith(prefix)