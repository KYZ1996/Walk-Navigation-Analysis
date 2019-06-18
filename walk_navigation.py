import json
import codecs
import time
import trans      # lib to transform gps coordinates and Mercator coordinates
import glob
import pickle

# transform the coordinates string from Mercator to GPS format
def mkt_to_gps(mkt_str):
    coor_dt = mkt_str.split(',')
    if len(coor_dt)%2 != 0:
        coor_dt = coor_dt[:-1]
    
    pointer = 0
    gps_str = ""
    last_coor = (float(coor_dt[pointer]),float(coor_dt[pointer+1]))
    while pointer <= (len(coor_dt) - 2):
        if pointer == 0:
            gps_str += str(trans.coordinate_mkt_to_china(last_coor,is_float = True))[1:-1]
        else:
            last_coor = (last_coor[0] + float(coor_dt[pointer])/100, last_coor[1] + float(coor_dt[pointer+1])/100)
            gps_str += (", " + str(trans.coordinate_mkt_to_china(last_coor,is_float = True))[1:-1])
        pointer += 2
        
    return gps_str

# format the output json format string
def json_object(time,x,y,imei_str,route,trace):
    string = "{\"case_info\":[[\"" + time + "\",\"\"],[\"weight problem:\"],[\" imei: " + imei_str + "\",\"\"], \
                [\"\",\"\",\"\"]],\"desc\":[[\"NAVI--USER\",\"\"]],\"start\":{\"x\":\"" + x + "\",\"y\":\"" + y + "\"}, \
                \"end\":{\"x\":\"\",\"y\":\"\"},\"detail1\":{\"ip\":\"\",\"plans\":[{\"coords\":\"" + route + "\",\"cost\":\"\", \
                \"dist\":\"\",\"name\":\"USER\",\"time\":\"\"}],\"extra_link\":\"\"},\"detail2\":{\"ip\":\"\", \
                \"plans\":[{\"coords\":\"" + trace + "\",\"cost\":\"\",\"dist\":\"\",\"name\":\"USER\",\"time\":\"\"}], \
                \"extra_link\":\"\"}}"
    return string

# read the log file and put information into dictionary
def read_log_file(filename,route):
    print("Reading " + files)
    with codecs.open(filename) as fin:            
        for line in fin:
            try:
                # data that has valid imei and have a guide route
                if line.find('&imei=') != -1 and (line.find('&pf=') - line.find('&imei=')) > 15 and line.find("\"coors\":\"") != -1:
                    # get the coordinate string
                    coors = mkt_to_gps(line[line.find("\"coors\":\"") + 9:line.find("\",\"distance\"")])

                    strs = line.split('|')
                    t = strs[1]   # time
                    imei = strs[2] # imei
                    
                    # request coordinate
                    start = line[line.find('&start=1$$$$')+12:line.find('$$$$$$')]
                    start_x,start_y = start.split(',')
                    start = tuple(trans.coordinate_mkt_to_china((float(start_x),float(start_y)),is_float= True))
                    
                    coors_array = coors.strip().split(',')
                    dest = (float(coors_array[-2].strip()),float(coors_array[-1].strip()))
                    
                    # only store one record for each user
                    if imei in route.keys():
                        continue
                    
                    element = []
                    element.append([int(time.mktime(time.strptime(t,"%Y-%m-%d %H:%M:%S"))),start,coors,{},dest])
                    route[imei] = element 
            except:
                continue
    return route

# read the user_trace file and put information into dictionary
def read_trace_file(filename,route):
    print(filename)
    with codecs.open(filename) as fin:
        for line in fin:       
            # if imei doesn't match, continue
            imei = json.loads(line[:-1])[u'user'][u'imei']
            point_array = json.loads(line[:-1])[u'points']
                
            if imei not in route.keys():
                continue  
                    
            elements = route[imei]
                
            for i in range(len(point_array)):
                t = int(point_array[i][u'loc_time']/1000)
                    
                for ele in elements:      # try to find the correct route of user to match 
                    if -100 <= (t - ele[0]) <= 300:
                        gps = (float(json.loads(line[:-1])[u'points'][i][u'longitude'])/float(1000000),
                               float(json.loads(line[:-1])[u'points'][i][u'latitude']/float(1000000)))
                        dic = ele[3]
                        dic[t] = gps       
    return route

def print_total_samples(route):
    count_total = 0
    for i in routes.values():
        if len(i[0][3]) > 0:
            count_total += 1
    print("Total number of sample is: " + str(count_total))

    return 

def output(filename,route):
    # output formatted files    
    count = 0
    with codecs.open(filename,'w','gbk') as fout:    
        for i in route.items():
            imei = i[0]
            user_route = i[1]
            for r in user_routes:
                    req_time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(r[0]))
                    start = r[1]
                    dest = r[4]
                    user_route = r[2]

                    user_trace = r[3]
                    if len(user_trace) > 0:
                        trace_str = ""
                        points = user_trace.items()
                        points.sort()
                        trace_time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(points[0][0]))
                        for p in points:
                            if len(trace_str) > 0:
                                trace_str += ","
                            trace_str += str(p[1])[1:-1]

                        if len(user_route.split(',')) > 40 and len(trace_str.split(',')) > 400:
                        # get all the items needed for json files output
                            output_string = json_object(req_time_str,trace_time_str,start,dest,imei,user_route,trace_str)
                            fout.write(output_string + "\n")
                            count += 1
                            
    # print the total number of valid data                                    
    print("Total number of valid event in first 5 files is: " + str(count))
    
    return      

def save_data(filename,route):
    # save data using pickle
    pickle.dump(route,open(filename,'wt'))
    return

def read_data(filename):
    return pickle.load(open(filename,'r')) 


if __name__ == "__main__":  
    
    routes = {}     #store the routes data tuples(request_time,request_starting_point,route,user_trace,destination)

    log_files = glob.glob('log_day\*.log')  # read all log_file names

    # read log files
    for files in log_files:
        routes = read_log_file(files,routes)

    # read coor files
    coor_files = glob.glob('coor_day\*')
    for files in coor_files:
        routes = read_log_file(files,routes)
    
    # print the total number of samples                         
    print_total_samples(routes)                            
    
    # output to file
    output('output.txt',routes)
         
    # save the data
    save_data('data.txt',routes)     
    
    # load the data 
    # routes = read_data('data.txt','r')
