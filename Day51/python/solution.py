class Solution:
    def findXSum(self, nums: List[int], k: int, x: int) -> List[int]:
        res=[]
        for i in range(0,len(nums)-k+1):
            st={}
            s=0
            for j in range(i,i+k):
                if nums[j] in st:
                    st[nums[j]]+=1
                else:
                    st[nums[j]]=1
            sorted_st=sorted(st.items(), key=lambda x:(x[1],x[0]), reverse=True)
            lt = [key * v for key, v in sorted_st[:x]]
            res.append(sum(lt))
        return res
