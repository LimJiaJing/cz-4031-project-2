import preprocessing
import annotation
import os

Username = input("please input the username")
Password = input("please input the Password")
Database = input("please input the Database")


os.system('python Interface.py')
preprocessing.run_preprocessing(query)
print(annotation.generate_annotation(query))
