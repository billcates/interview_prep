class Solution:
    def evalRPN(self, tokens: List[str]) -> int:
        operators=set(['+','-','*','/'])

        st=[]

        for each in tokens:
            # print(each,st)
            if each not in operators:
                st.append(int(each))
            else:
                val2=int(st.pop(-1))
                val1=int(st.pop(-1))
                if each == '+':
                    st.append(val1+val2)
                elif each=='-':
                    st.append(val1-val2)
                elif each=='*':
                    st.append(val1*val2)
                else:
                    st.append(int(val1/val2))
        
        return st.pop(-1)
        