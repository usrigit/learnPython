dict1 = {'key1': [1, 2, 3], 'key2': [11, 21, 31]}
dict2 = {'key1': [4, 5], 'key2': [41, 51]}

print({k: dict1.get(k, []) + dict2.get(k, []) for k in set().union(dict1, dict2)})

dict1 = {'key1': {'a': 1, 'b': 2}}
dict2 = {'key1': {'x': 3, 'y': 4}, 'key2': {'p': 10, 'q': 11, 'r': 12}}


final_dict = {}
for k in set().union(dict1, dict2):
    val = {}
    sub_d1 = dict1.get(k)
    sub_d2 = dict2.get(k)

    if sub_d1:
        val.update(sub_d1)
    if sub_d2:
        val.update(sub_d2)
    final_dict.update({k: val})
print("Final dict = ", final_dict)

print("Valu = ", ''.join(filter(lambda x: x.isdigit(), "VOLUME  6,438")))