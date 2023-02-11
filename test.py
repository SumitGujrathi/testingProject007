#print("running")
import datetime

cur_time = datetime.now().strftime('%H:%M:%S')
cur_time = datetime.strptime(cur_time, '%H:%M:%S').time()
print(cur_time)



