from pycaishen.pycaishenstorage import PycaishenStorage


if __name__ == '__main__':

    storage = PycaishenStorage("Arctic")
    print(storage)

    # print Storage.io_engine
    libraries = storage.list_libraries()
    print(libraries)
    library = libraries[2]
    symbols = storage.list_symbols(library)
    print(symbols)

    library = "Amine.test"
    symbol = "test_symbol"
    import pandas as pd
    dt = pd.DataFrame([1,2,3],[3,2,1])
    print(dt)
    storage.write(symbol, dt, library, False)
    print((storage.read(symbol, library)))

    print((storage.has_symbol(symbol, library)))

    storage.delete_library(library)
    print((storage.list_libraries()))






