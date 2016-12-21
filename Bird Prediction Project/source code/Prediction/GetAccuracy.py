# Author : Sushil Thasale
import os
import glob   

script_dir = os.path.dirname(__file__)
path1 = "output/orderedPredictions/part-*"
path2 = "labelled_data/part-*"
abs_path1 = os.path.join(script_dir, path1)
abs_path2 = os.path.join(script_dir, path2)

predictions = {}
labelled = {}

true_true = 0
true_false = 0
false_true = 0
false_false = 0

files=glob.glob(abs_path1)   
for file in files:     
    f=open(file, 'r')  
    for line in f:
	if "SAW_AGELAIUS" in line:
		print("header found")
	else:
		words = line.split(",")
		predictions[words[0]] = int(words[1].strip())
    f.close()

#print(predictions)

files=glob.glob(abs_path2)   
for file in files:     
    f=open(file, 'r')  
    for line in f:
	words = line.split(",")
	isPresent = 0
	label = words[26]
	if(label == "X" or int(label) > 0):
		#print("reading file 2")
		isPresent = 1 	
	labelled[words[0]] = isPresent
    f.close()

#print(labelled)

for key in predictions:
	predicted_value = predictions.get(key)
	actual_value = labelled.get(key)	
	if predicted_value == 1 and actual_value == 1:
		true_true += 1
	elif predicted_value == 1 and actual_value == 0:
		true_false += 1
	elif predicted_value == 0 and actual_value == 1:
		false_true += 1
	elif predicted_value == 0 and actual_value == 0:
		false_false += 1

accuracy = (true_true + false_false)  * 100 / (true_false + false_true + true_true + false_false)
print ("True_True : " + str(true_true))
print ("True_False : " + str(true_false))
print ("False_True : " + str(false_true))
print ("False_False : " + str(false_false))
print ("Acuracy : " + str(accuracy))
