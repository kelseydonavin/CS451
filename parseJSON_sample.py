# Title

import json
import psycopg2

def cleanStr4SQL(s):
    return s.replace("'","`").replace("\n"," ")

def convertIntToBool(integer):
    return bool(integer)

def parseBusinessData():
    # read the JSON file
    with open('.\yelp_business.JSON','r') as f: 
        outfile =  open('business.txt', 'w')
        line = f.readline()
        count_line = 0

        # connect to yelpdb database on postgres server using psycopg2
        try:
            conn = psycopg2.connect("dbname='Project51' user='postgres' host='localhost' password='Luckyme324'")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()
        
        while line:
            data = json.loads(line)
            # parse data
            outfile.write(cleanStr4SQL(data['business_id'])+'\t') #business id
            outfile.write(cleanStr4SQL(data['name'])+'\t') #name
            outfile.write(cleanStr4SQL(data['address'])+'\t') #full_address
            outfile.write(cleanStr4SQL(data['state'])+'\t') #state
            outfile.write(cleanStr4SQL(data['city'])+'\t') #city
            outfile.write(cleanStr4SQL(data['postal_code']) + '\t')  #zipcode
            outfile.write(str(data['latitude'])+'\t') #latitude
            outfile.write(str(data['longitude'])+'\t') #longitude
            outfile.write(str(data['stars'])+'\t') #stars
            outfile.write(str(data['review_count'])+'\t') #reviewcount
            outfile.write(str(data['is_open'])+'\t') #openstatus

            categories = data["categories"].split(', ')
            outfile.write(str(categories)+'\t')  #category list

            attributes = data["attributes"]
            attributeList = []
            for key, value in attributes.items():
                if isinstance(value, dict):
                    for key, value in value.items():
                        attributeTuple = (key, value)
                        attributeList.append(attributeTuple)
                else:
                    attributeTuple = (key, value)
                    attributeList.append(attributeTuple)
            outfile.write(str(attributeList)+'\t') #attribute list

            hours = data["hours"]
            hourList = []
            for key, value in hours.items():
                timeList = value.split('-')
                hourTuple = (key, timeList)
                hourList.append(hourTuple)
            outfile.write(str(hourList)+'\t') #hours
            outfile.write('\n');

            for item in hourList:
                day = item[0]
                openTime = item[1][0]
                closeTime = item[1][1]

                # Generate the INSERT statement for the current business
                sql_str = "INSERT INTO hours (business_id, day, open, close) " \
                          "VALUES ('" + data['business_id'] + "','" + day + "','" + openTime + "','" + closeTime + "');"
                try:
                    cur.execute(sql_str)
                except:
                    print("Insert to business table failed.")
                conn.commit()

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()
        
    print(count_line)
    outfile.close()
    f.close()

def parseUserData():
    # read the JSON file
    with open('.\yelp_user.JSON','r') as f:
        outfile = open('user.txt', 'w')
        line = f.readline()
        count_line = 0

        # connect to yelpdb database on postgres server using psycopg2
        try:
            conn = psycopg2.connect("dbname='Project51' user='postgres' host='localhost' password='Luckyme324'")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()
        
        # read each JSON abject and extract data
        while line:
            data = json.loads(line)
            # parse data
            outfile.write(str(data['average_stars']) + '\t')
            outfile.write(str(data['cool']) + '\t')
            outfile.write(str(data['fans']) + '\t')
            outfile.write(str(data['friends']) + '\t')
            outfile.write(str(data['funny']) + '\t')
            outfile.write(cleanStr4SQL(data['name']) + '\t')
            outfile.write(str(data['tipcount']) + '\t')
            outfile.write(str(data['useful']) + '\t')
            outfile.write(str(data['user_id']) + '\t')
            outfile.write(str(data['yelping_since'].split(' ')[0]) + '\t')
            outfile.write(str(data['yelping_since'].split(' ')[1]) + '\t')
            outfile.write('\n');

            yelping_since_date = str(data['yelping_since'].split(' ')[0])
            yelping_since_time = str(data['yelping_since'].split(' ')[1])

            # Generate the INSERT statement for the current business
            sql_str = "INSERT INTO users (user_id, cool, useful, funny, yelping_since_time, yelping_since_date, latitude, longitude, name, average_stars, fans, tip_count, total_likes) " \
                      "VALUES ('" + data['user_id'] + "'," + str(data["cool"]) + "," + str(data["useful"]) + "," + \
                      str(data["funny"]) + ",'" + yelping_since_date + "','" + yelping_since_time + "', 0, 0, '" + \
                      cleanStr4SQL(data["name"]) + "'," + str(data["average_stars"]) + "," + str(data['fans']) + ", 0, 0);"
            try:
                cur.execute(sql_str)
            except:
                print("Insert to user table failed.")
            conn.commit()

            line = f.readline()
            count_line += 1

        cur.close()
        conn.close()
        
    print(count_line)
    outfile.close()
    f.close()

def parseCheckinData():
    with open('.\yelp_checkin.JSON','r') as f:  # Assumes that the data files are available in the current directory. If not, you should set the path for the yelp data files.
        outfile = open('checkin.txt', 'w')
        line = f.readline()
        count_line = 0
        # read each JSON abject and extract data
        while line:
            data = json.loads(line)
            outfile.write(str(data['business_id']) + '\t') # business_id

            # Splitting date into year, month, day, and time tuples
            date = data['date']
            pairs = date.split(',')
            dateTimeList = []
            finalList = []
            for item in pairs:
                dateTime = item.split(' ')
                dateTimeList.append(dateTime)
            for item in dateTimeList:
                yearMonthDay = item[0].split('-')
                year = yearMonthDay[0]
                month = yearMonthDay[1]
                day = yearMonthDay[2]
                dateTuple = (year, month, day, item[1])
                finalList.append(dateTuple)

            outfile.write(str(finalList) + '\t') # date
            outfile.write('\n');

            line = f.readline()
            count_line += 1
    print(count_line)
    outfile.close()
    f.close()

def parseTipData():
    #read the JSON file
    with open('.\yelp_tip.JSON','r') as f:
        outfile =  open('tip.txt', 'w')
        line = f.readline()
        count_line = 0
        #read each JSON abject and extract data
        while line:
            data = json.loads(line)
            outfile.write(cleanStr4SQL(data['business_id'])+'\t') #business id
            
            #split date into day and time
            outfile.write(cleanStr4SQL(data['date'].split(' ')[0])+'\t') #date
            outfile.write(cleanStr4SQL(data['date'].split(' ')[1])+'\t') #timestamp
            
            outfile.write(str(data['likes'])+'\t') #likes
            outfile.write(cleanStr4SQL(data['text'])+'\t') #text
            outfile.write(cleanStr4SQL(data['user_id'])+'\t') #user id
            outfile.write('\n');

            line = f.readline()
            count_line +=1
    print(count_line)
    outfile.close()
    f.close()

def insert2BusinessTable():
    #reading the JSON file
    with open('.\yelp_business.JSON','r') as f:
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        try:
            conn = psycopg2.connect("dbname='Project51' user='postgres' host='localhost' password='Luckyme324'")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
            data = json.loads(line)
            # Generate the INSERT statement for the current business
            sql_str = "INSERT INTO business (business_id, is_open, latitude, longitude, stars, name, state, city, postal_code, tip_count, check_in_count) " \
                      "VALUES ('" + data['business_id'] + "'," + str(convertIntToBool(data["is_open"])) + "," + str(data["latitude"]) + "," + str(data["longitude"]) + "," + \
                      str(data["stars"]) + ",'" + cleanStr4SQL(data["name"]) + "','" + cleanStr4SQL(data["state"]) + "','" + cleanStr4SQL(data["city"]) + "'," + \
                      data["postal_code"] + ", 0 , 0);"
            try:
                cur.execute(sql_str)
            except:
                print("Insert to business table failed.")
            conn.commit()

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    f.close()

def insert2TipTable():
    #reading the JSON file
    with open('.\yelp_tip.JSON','r') as f:
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        try:
            conn = psycopg2.connect("dbname='Project51' user='postgres' host='localhost' password='Luckyme324'")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
            data = json.loads(line)

            date = cleanStr4SQL(data['date'].split(' ')[0])
            time = cleanStr4SQL(data['date'].split(' ')[1])
            
            # Generate the INSERT statement for the current business
            sql_str = "INSERT INTO tip (business_id, text, date, time, likes, user_id) " \
                      "VALUES ('" + data['business_id'] + "','" + cleanStr4SQL(data["text"]) + "','" + date + "','" + time + "','" + \
                      str(data["likes"]) + "','" + data["user_id"] + "');"
            try:
                cur.execute(sql_str)
            except:
                print("Insert to tip table failed.")
            conn.commit()

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    f.close()

def insert2CheckInTable():
    #reading the JSON file
    with open('.\yelp_checkin.JSON','r') as f:
        line = f.readline()
        count_line = 0

        #connect to yelpdb database on postgres server using psycopg2
        try:
            conn = psycopg2.connect("dbname='Project51' user='postgres' host='localhost' password='Luckyme324'")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
            data = json.loads(line)

            # Splitting date into year, month, day, and time tuples
            date = data['date']
            pairs = date.split(',')
            dateTimeList = []
            finalList = []
            for item in pairs:
                dateTime = item.split(' ')
                dateTimeList.append(dateTime)
            for item in dateTimeList:
                yearMonthDay = item[0].split('-')
                year = yearMonthDay[0]
                month = yearMonthDay[1]
                day = yearMonthDay[2]
                dateTuple = (year, month, day, item[1])
                finalList.append(dateTuple)
            
                # Generate the INSERT statement for the current business
                sql_str = "INSERT INTO check_in (business_id, year, month, day, time) " \
                          "VALUES ('" + data['business_id'] + "','" + year + "','" + month + "','" + day + "','" + item[1] + "');"
                try:
                    cur.execute(sql_str)
                except:
                    print("Insert to check_in table failed.")
                conn.commit()

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    f.close()

#parseBusinessData()
#parseUserData()
#parseCheckinData()
#parseTipData()
    
#insert2BusinessTable()
#parseUserData()
#insert2TipTable()
insert2CheckInTable()




