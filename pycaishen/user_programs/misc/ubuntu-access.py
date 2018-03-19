from pycaishen.pycaishenstorage import PycaishenStorage

from pycaishen.user_programs.user_programs_settings import Configurer


storage = PycaishenStorage("arctic")

print((storage.list_libraries()))
lib = 'Quandl.WIKI'
print((storage.list_symbols(lib)))


print((storage.read('WIKI/A',lib)))