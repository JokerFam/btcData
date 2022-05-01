# install libraries
%pip install yfinance

# Import libraries
import yfinance as yf
import pandas as pd

from datetime import date, datetime

# Get BTC data from yahoo finance
btc = yf.Ticker("BTC-USD")

# Get max historical market data in 3 month intervals
btc_price_data = btc.history(period="max", interval="3mo")

# Get the closing data from history
btc_price_closing = btc_price_data.Close
#display(btc_price_closing)

# Creating a list of quarterly dates
# Data is collect quarterly (MM-dd) 

# variables
quarterly = ['-03-01', '-06-01', '-09-01', '-12-01']
dates = []
starting_year = 2014
year_count = starting_year
ending_date = date.today() # - 90
index = 2
run_loop = True

# Loop that creates quartely dates
while run_loop:
    # Set a quarterly date
    newDate = str(year_count)+quarterly[index]
    datetimeFormat = datetime.strptime(newDate, "%Y-%m-%d").date()
    
    # Compare the todays date to quarterly date to stop loop
    date1 = datetime(ending_date.year, ending_date.month, ending_date.day)
    date2 = datetime(datetimeFormat.year, datetimeFormat.month, datetimeFormat.day)
    
    if (date1 < date2) == True:
        run_loop = False
    else:
        dates.append(newDate)
        
    index+=1  
    if index == 4:
        index = 0
        year_count+=1
    

#print(dates)    
#print(str(date.today()))

# Putting price to list
price = []

for i in btc_price_closing:
    price.append(i)
    
#print(price)    

# Creating PD dataframe
price2 = price[:]

try:
    # try statement to keep length of dates equal to the length of prices
    if len(dates) != len(price): # The price list collected a date that is no a quarterly date price
        price2.pop()
        prices = {"Date": dates, "BTC Price": price2}
        btc_price = pd.DataFrame(prices)
    else:
        prices = {"Date": dates, "BTC Price": price2}
        btc_price = pd.DataFrame(prices)
except ValueError:
    print("Price list not the same length as dates list.")

#len(price2) # Should be equal to the length of dates list

# Convert pandas DF to spark DF
btc_rdd = spark.createDataFrame(btc_price)
btc_rdd.show()

# Show Schema
#btc_rdd.printSchema()

# Show data
#display(btc_rdd)

# Show top 5 records
#btc_rdd.take(5)