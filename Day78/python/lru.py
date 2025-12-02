class Node:
    def __init__(self,key,val):
        self.key, self.val = key, val
        self.prev = self.next =None

class LRUCache:

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = {}
        self.left = Node(0,0)
        self.right = Node(0,0)
        self.left.next, self.right.prev = self.right, self.left
        
    def insert(self,Node):
        prev=self.right.prev
        nxt=self.right
        prev.next=Node
        nxt.prev=Node
        Node.next=nxt
        Node.prev=prev

    def remove(self,Node):
        prev, nxt = Node.prev, Node.next

        prev.next=nxt
        nxt.prev=prev


    def get(self, key: int) -> int:
        if key in self.cache:
            self.remove(self.cache[key])
            self.insert(self.cache[key])
            return self.cache[key].val

        return -1
        

    def put(self, key: int, value: int) -> None:
        if key in self.cache:
            self.remove(self.cache[key])
        node=Node(key,value)
        self.insert(node)
        self.cache[key]=node

        if self.capacity <len(self.cache):
            lru=self.left.next
            self.remove(lru)
            del self.cache[lru.key]


        


# Your LRUCache object will be instantiated and called as such:
# obj = LRUCache(capacity)
# param_1 = obj.get(key)
# obj.put(key,value)