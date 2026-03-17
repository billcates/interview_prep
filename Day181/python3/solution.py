class Solution:
    def summaryRanges(self, nums: List[int]) -> List[str]:
        res = []
        i = 0
        j = 1

        while j < len(nums):
            if nums[j] != nums[j-1] + 1:
                if i == j-1:
                    res.append(str(nums[i]))
                else:
                    res.append(f"{nums[i]}->{nums[j-1]}")
                i = j
            j += 1

        if nums:
            if i == len(nums)-1:
                res.append(str(nums[i]))
            else:
                res.append(f"{nums[i]}->{nums[-1]}")

        return res