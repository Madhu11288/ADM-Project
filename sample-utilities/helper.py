file_ptr = open("data/datafile3hours.dat")
entries = file_ptr.readlines()

max_car_id = 0

for entry in entries:
    if entry.startswith("0"):
        values = entry.split(",")
        if int(values[2]) > max_car_id:
            max_car_id = int(values[2])

print(max_car_id)



