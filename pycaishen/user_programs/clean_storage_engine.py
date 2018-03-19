from pycaishen.ioengines.ioenginefactory import IOEngineFactory

def clean_DB(engine="Arctic"):
    # clean the Arctic Engine
    # deleting all the old libraries
    engine = IOEngineFactory().load_engine(engine)
    libraries = engine.list_libraries()
    print("libraries to be deleted:")
    print(libraries)
    for library in libraries:
        engine.delete_library(library)

    libraries = engine.list_libraries()

    try:
        assert libraries == []
        print("deleting libraries successful")
    except :
        print("deleting libraries not done")

if __name__ == '__main__':
    clean_DB()

    # engine = 'arctic'
    # engine = IOEngineFactory().load_engine(engine)
    # libraries = engine.list_libraries()
    # print "libraries to be deleted:"
    # print libraries
    # for library in libraries:
    #
    #     if "Quandl" in library:
    #         print library
    #         engine.delete_library(library)
    #
    # pass