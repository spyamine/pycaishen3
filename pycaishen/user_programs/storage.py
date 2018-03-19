from pycaishen.pycaishenstorage import PycaishenStorage

storage = PycaishenStorage("arctic")
print((storage.list_libraries()))
lib = "Bloomberg.Index.Composition"
tickers = storage.list_symbols(lib)
tot = 0
all_sym = []
for _ in tickers:
    print(_)
    symbols = list(set(storage.read(_,lib)["Symbol"]))
    all_sym = all_sym + symbols
    print(symbols)
    print((len(symbols)))
    tot = tot + len(symbols)
    print(tot)

all_sym = list(set(all_sym))
print((len(all_sym)))

all_sym1 = all_sym
print((3*"---"))

lib = "Bloomberg.OpenFigi.symbols"
tickers = storage.list_symbols(lib)
tot = 0
all_sym =[]
for _ in tickers:
    print(_)
    symbols = list(set(storage.read(_,lib)["Symbol"]))
    print(symbols)
    print((len(symbols)))
    tot = tot + len(symbols)
    print(tot)
    all_sym = all_sym + symbols

all_sym = list(set(all_sym))
print((len(all_sym)))

print((3*"---"))


print('open figi')
print((len(all_sym)))
print(all_sym)
print('composition')
print((len(all_sym1)))
print(all_sym1)

all  = all_sym + all_sym1

print("difference: ")
diff =  list(set(all_sym)- set(all_sym1))
print((len(diff)))
print(diff)

diff =  list(set(all_sym1)- set(all_sym))
print((len(diff)))
print(diff)

print("combinaison : ")
all = list(set(all))

print((len(all)))

# storage.delete_library("Bloomberg.Fundametals")
#




#
# print storage.read("0602996D MP Equity","Bloomberg.tickers.metadata")

#
# lib ="Bloomberg.Index.Composition"
#
# data = storage.read("BCOM Index","Bloomberg.Index.Composition")
#
# # data["Symbol"] = data["Index Member"] + " Comdty"
#
# print data
#
# # storage.write("BCOM Index",data,"Bloomberg.Index.Composition")