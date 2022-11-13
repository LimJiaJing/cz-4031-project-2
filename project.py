import preprocessing
import annotation
import os

os.system('python Interface.py')
preprocessing.run_preprocessing(query)
print(annotation.generate_annotation(query))
