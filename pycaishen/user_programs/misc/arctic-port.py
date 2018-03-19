from arctic import Arctic
import quandl
import pymongo

host = 'localhost:57017'
c = pymongo.MongoClient(host, connect=False)

# Connect to Local MONGODB
# store = Arctic(c)
store = Arctic(host)

# Create the library - defaults to VersionStore
store.initialize_library('NASDAQq')

# Access the library
library = store['NASDAQq']

# Load some data - maybe from Quandl
aapl = quandl.get("WIKI/AAPL", authtoken="tETRuReNpNs82YcXHCWR")

# Store the data in the library
library.write('AAPL', aapl, metadata={'source': 'Quandl'})

# Reading the data

item = library.read('AAPL')
aapl = item.data
metadata = item.metadata

print(item)
print(aapl)
print(metadata)
