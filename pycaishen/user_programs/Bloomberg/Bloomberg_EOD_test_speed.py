import time

from pycaishen.user_programs.nice_print import print_n
from pycaishen.pycaishenstorage import PycaishenStorage


engine = PycaishenStorage("arctic")


libraries = engine.list_libraries()

print_n(libraries,5)

library = 'Bloomberg.EOD'
symbols =  engine.list_symbols(library)

print_n(symbols,10)


start = time.time()
rows_read = 0
for symbol in symbols:
    # print engine.read(symbol,library).head()
    rows_read += len(engine.read(symbol,library))
    # print len(engine.read(symbol,library))

print(("Symbols: %s Rows: %s  Time: %s  Rows/s: %s" % (len(symbols),
                                                          rows_read,
                                                          (time.time() - start),
                                                          rows_read / (time.time() - start))))
