# demonstrates some dodginess in the dataset library
# https://github.com/pudo/dataset/issues/89

import dataset

db = dataset.connect("sqlite://")

tbl_name = None
tbl_name = u"butts"
#db.update_table(tbl_name) #<-- this line does not realize that None is a ridiculous table name
tbl = db[tbl_name]
print(tbl_name, "has these columns:")
print(tbl.columns)

print("Inserting data")
tbl.insert({"fred": 4, "barney": "alexia"}) #<-- so the crash happens here, which is not where the bug is
print(tbl.columns)
