# Databricks notebook source
# MAGIC %md
# MAGIC #####  1. Swap cases in a string

# COMMAND ----------

str1 = "Python is the Best Language in the Whole World."

# COMMAND ----------

#Approach 1
output =""
for each_char in str1:
    if each_char.islower():
        output += each_char.upper()
    else:
        output += each_char.lower()
print(output)


# COMMAND ----------

#Approach2
print(str1.swapcase())
#print(str1.swapcase() == output)
#print(str1.swapcase() is output)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.  Count of characters in string

# COMMAND ----------

from collections import Counter
print("Total Length of the string:",len(str1))
c= Counter(str1)
print(dict(c))

# After removing space
replaced_space_string = str1.replace(" ","")
print(replaced_space_string)

# COMMAND ----------

print("3 Most Common element is :",c.most_common(3))
print("Most Common element is :",c.most_common(1))

for ele,count in c.most_common(5):
    print(f"{ele} count is {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Identify common elements of both list wiithout using loop

# COMMAND ----------

a = [1,2,3,4]
b = [3,4,5,6]

# COMMAND ----------

set(a).intersection(b)

# COMMAND ----------

help(set.difference)

# COMMAND ----------

set(a).difference(b)

# COMMAND ----------

help(set.add)

# COMMAND ----------

set_a = set(a)
print(set_a)
set_a.add(5)
print(set_a)

# COMMAND ----------

help(set.union)

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Vowel count in a string
# MAGIC

# COMMAND ----------

vowels = set("aeiouAEIOU")
vowels_count = 0
for ch in str1:
    if ch in vowels:
        vowels_count += 1
print(vowels_count)

# COMMAND ----------

vowels_count = sum(1 for ch in str1 if ch in vowels)
print(vowels_count)

# COMMAND ----------

(1 for ch in str1 if ch in vowels)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. extract number and alphabets in seperate list

# COMMAND ----------

str2 = "Arin1234Maity@#$%^&*"

# COMMAND ----------

alpha_list = [ch for ch in str2 if ch.isalpha()]
num_list = [ch for ch in str2 if ch.isdecimal()]
print(alpha_list,num_list)

# COMMAND ----------

"Arin123".isalnum()

# COMMAND ----------

"  ".isspace()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Square root a number without math package

# COMMAND ----------

num = 625

# COMMAND ----------

num**0.5

# COMMAND ----------

# Newton-Raphson

def square_root(number):
    if number < 0:
        raise ValueError("Square root undefined for negative numbers")
    if number == 0 or number == 1:
        return number

    # Initial guess for the square root
    guess = number / 2.0

    # Update the guess using successive calculations
    while True:
        new_guess = 0.5 * (guess + number / guess)
        if abs(new_guess - guess) < 0.0001:  # Define your desired precision here
            break
        guess = new_guess

    return guess

# Example usage
num = 25  # The number to find the square root of
result = square_root(num)
print(f"The square root of {num} is approximately {result:.1f}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Write program to count the number of occurrences of chars in a string. Example : 
# MAGIC str ='aabbccc' o/p 2a2b3c

# COMMAND ----------

str3 ='aabbcccd'

# COMMAND ----------

count_dict ={}
count=1
current =str3[0]

for ch in str3[1:]:
    if ch == current:
        count += 1
    else:
        count_dict[current] = count
        current = ch
        count = 1

count_dict[current] = count

output = ""
for k,v in count_dict.items():
    output += str(v)+k
print(output)


# COMMAND ----------

output_list =[]
count=1
current =str3[0]

for ch in str3[1:]:
    if ch == current:
        count += 1
    else:
        count_dict[current] = count
        output_list.append(str(count)+current)
        current = ch
        count = 1

output_list.append(str(count)+current)

print(" ".join(output_list))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. “Find 24 sum of 6 the numbers 56 in the sentence.”
# MAGIC -( sum of numbers in the 
# MAGIC strings)

# COMMAND ----------

# Not the efficient approach
num_str = "Find 24 sum of 6 the numbers 56 in the sentence."
last = num_str[0]
num_list =[]
for ch in num_str[1:]:
    if last.isdecimal():
        if ch.isdecimal():
            last = last+ch
        else:
            num_list.append(int(last))
            last = ch
    else:
        last = ch

print(num_list)
print(sum(num_list))


# COMMAND ----------

num_str = "Find 24 sum of 6 the numbers 56 in the sentence."

num_list = []
current_num = ""

for char in num_str:
    if char.isdigit():
        current_num += char
    elif current_num:
        num_list.append(int(current_num))
        current_num = ""

# Append the last number if there's any left
if current_num:
    num_list.append(int(current_num))

print(num_list)
print(sum(num_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9.  Lambda operations [a,b,c,d] => [a1,b1,c1,d1] without loop

# COMMAND ----------

in_list = ['a','b','c','d']

# COMMAND ----------

list(map(lambda x: x+'1',in_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10. How do you string reversal using 'for' loop

# COMMAND ----------

a_string = "ArinMaity"
reversed_string = ""
for i in range(len(a_string)-1,-1,-1):
    reversed_string += a_string[i]
print(reversed_string)

# COMMAND ----------

a_string[::-1]

# COMMAND ----------

"".join(list(reversed(a_string)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 11.Sort list based on second character in each element in string

# COMMAND ----------

a_list =['a9e','c42','b8o']

# COMMAND ----------

a_list = sorted(a_list,key=lambda a: a[1])
print(a_list)

# COMMAND ----------



# COMMAND ----------

a= "Hello orld"
" W".join(a.split(" "))
