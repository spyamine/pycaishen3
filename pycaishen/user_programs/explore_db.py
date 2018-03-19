from pycaishen.pycaishenstorage import PycaishenStorage

storage = PycaishenStorage("arctic")

libs = storage.list_libraries()

print(libs)

for lib in libs:

    print(("library name: %s" % lib))
    # print storage.list_symbols(lib)[0:10]

lib = "Bloomberg.EOD"
lib = "Bloomberg.Tickers.Metadata"


symbols = storage.list_symbols(lib)
import pandas as pd

l = []
i = 0
# data = pd.DataFrame()
for symbol in symbols:
    i = i+ 1
    dataa = storage.read(symbol, lib)
    if len(dataa.columns) == 14:
        l.append(storage.read(symbol,lib))
    # if i == 0:
    #     data = storage.read(symbol,lib)
    # else:
    #     data = pd.concat(data,storage.read(symbol,lib))


    # if i== 10:
    #     break

data = pd.concat(l)
data.to_csv("metadata.csv")


print(data)
print((len(symbols)))


#

#
# symbols = ["MOSENEW Index","ADH MC Equity","WAA MC Equity","EURUSD Curncy"]
# # symbols = ["ADH MC Equity","WAA MC Equity"]
# # import matplotlib.pyplot as plt
# for symbol in symbols:
#     data =  storage.read(symbol,lib)
#     print data.tail()
#     print "max date:"
#     print data.index.max()
#     print "min date:"
#     print data.index.min()