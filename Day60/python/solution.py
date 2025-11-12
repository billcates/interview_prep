import math

class Solution:
    def minWindow(self, s: str, t: str) -> str:
        if len(s) < len(t):
            return ""

        # Step 1: Build frequency map for t
        need = {}
        for ch in t:
            need[ch] = need.get(ch, 0) + 1

        # Step 2: Initialize window + counters
        window = {}
        have = 0
        need_count = len(need)
        res = ""
        res_len = math.inf

        l = 0

        # Step 3: Expand the window
        for r in range(len(s)):
            ch = s[r]
            window[ch] = window.get(ch, 0) + 1

            if ch in need and window[ch] == need[ch]:
                have += 1

            # Step 4: Shrink window while valid
            while have == need_count:
                # Update result if smaller
                if (r - l + 1) < res_len:
                    res = s[l:r+1]
                    res_len = r - l + 1

                # Shrink from left
                window[s[l]] -= 1
                if s[l] in need and window[s[l]] < need[s[l]]:
                    have -= 1
                l += 1

        return res
