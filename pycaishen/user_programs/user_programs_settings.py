
class Configurer(object):

    BLOOMBERG_EOD_FIELDS =["close", "open", "high", "low", "turnover", "volume", "mkt_cap"]
    BLOOMBERG_EOD_DATASOURCE_FIELDS = ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "TURNOVER", "PX_VOLUME", "CUR_MKT_CAP"]

     # Original list
    # AFRICAN_INDEXES =["SPAFRUP Index","DJAFK Index","MOSENEW Index","BGSMDC Index","DARSDSEI INDEX","EGX30 Index",
    #               "FTN098 Index","GGSECI Index","GGSEGSE INDEX","ICX10 Index","ICXCOMP Index","LUSEIDX Index",
    #               "JALSH Index","KNSMIDX Index","NGSEINDX Index","NSEASI INDEX","SBBCMOL Index","SEMDEX Index","SEMGPCPD Index",
    #               "SZSISSM Index","TUSISE Index","UGSINDX INDEX","ZHINDUSD INDEX","ZHMINUSD INDEX"]

    AFRICAN_INDEXES = ["SPAFRUP Index", "DJAFK Index", "MOSENEW Index", "BGSMDC Index", "DARSDSEI INDEX", "EGX30 Index",
                       "FTN098 Index",  "ICX10 Index",  "LUSEIDX Index",
                       "JALSH Index",  "NGSEINDX Index", "NSEASI INDEX", "SBBCMOL Index",
                       "SEMDEX Index",
                        "TUSISE Index",  "ZHINDUSD INDEX","JA00 Index"]
    COMMODITIES_INDEX = ["BCOM Index"]




    BLOOMBERG_METADATA_FIELDS = ['NAME', 'CIE_DES', 'GICS_SECTOR_NAME', 'GICS_INDUSTRY_NAME', 'ICB_SECTOR_NAME',
                       'ICB_SUPERSECTOR_NAME', 'ISSUER_INDUSTRY', 'COUNTRY_FULL_NAME', 'CURRENCY', 'TICKER',
                       'ACCOUNTING_STANDARD', 'ID_ISIN', 'EXCH_CODE','MARKET_STATUS','SECURITY_TYP']


    LIB_BLOOMBERG_METADATA = "Bloomberg.Tickers.Metadata"
    LIB_INDEX_COMPOSITION = "Bloomberg.Index.Composition"
    LIB_BLOOMBERG_OPEN_FIGI = "Bloomberg.OpenFigi.symbols"
    LIB_BLOOMBERG_FUNDAMENTAL_FIELDS ="Bloomberg.Fundamental.fields"
    LIB_BLOOMBERG_FUNDAMENTAL_DATA = "Bloomberg.Fundamentals"
    LIB_BLOOMBERG_EOD_ADJUSTED = "Bloomberg.EOD"
    LIB_BLOOMBERG_EOD_NOT_ADJUSTED = "Bloomberg.EOD.unadjusted"
    LIB_BLOOMBERG_DVD = "Bloomberg.Fundamentals.dividends"
    LIB_BLOOMBERG_MINUTE_ADJUSTED ="Bloomberg.Minute"
    LIB_BLOOMBERG_MINUTE_NOT_ADJUSTED = "Bloomberg.Minute.unadjusted"

    QUANDL_SYMBOL_LIBRARY ="Quandl.Database.symbols"
    QUANDL_TICKERS_FOLDER ="Quandl_Reference_Folder"

    QUANDL_API_KEY = "tETRuReNpNs82YcXHCWR"
    # QUANDL_API_KEY = "JscGzKnKzXP9ro2Vux7V"

    # QUANDL_LIBRARIES_LIST = ["UNDATA", "WORLDBANK", "ODA", "PERTH",  "JOHNMATT", "WGC", "BKRHUGHES",
    #                          "USDAERS", "FAO", "WIKI", "CHRIS", "NBSC", "CME", "ICE", "CBOE", "CFTC", "BLSI", "COM",
    #                          "BP", "EIA", "JODI", "ISM", "WHNP", "WGFD", "WWGI", "UCOM"]

    QUANDL_LIBRARIES_LIST_ECONOMY = ["WADI","FRED","ODA","UGID","PBCHINA" ,"FED","USTREASURY"]
    QUANDL_LIBRARIES_LIST_MARKETS = ["WIKI"]
    QUANDL_LIBRARIES_LIST_MARKET_DATA = ["KFRENCH","WFE"]
    QUANDL_LIBRARIES_LIST_COMMODITIES = ["COM","CHRIS","WGC","LSE"]

    QUANDL_LIBRARIES_LIST = QUANDL_LIBRARIES_LIST_ECONOMY + QUANDL_LIBRARIES_LIST_MARKETS + QUANDL_LIBRARIES_LIST_MARKET_DATA \
                            + QUANDL_LIBRARIES_LIST_COMMODITIES
    # QUANDL_LIBRARIES_LIST_DESC = \
    """
    WIKI : End of day stock prices, dividends and splits for 3,000 US companies, curated by the Quandl community and
        released into the public domain.
    CHRIS : Individual futures contracts trade for very short periods of time, and are hence unsuitable for long-horizon
        analysis. Continuous futures contracts solve this problem by chaining together a series of individual futures
        contracts, to provide a long-term price history that is suitable for trading, behavioral and strategy analysis.
    NBSC : Statistics of China relating to finance, industry, trade, agriculture, real estate, and transportation.
    NSE : Stock and index data from the National Stock Exchange of India.
    CME : Futures data for a wide range of commodities (metals, grains, energy) and financial instruments
        (equities, currencies, rates)
    ICE : Futures data for softs (coffee, sugar, cotton, cocoa), grains, energy products and financial instruments,
        with historical contracts going back decades.
    CBOE : The Chicago Board Options Exchange is the largest options exchange in the US, offering many different options
        contracts for companies, indices, and ETFs.
    CFTC : Weekly Commitment of Traders and Concentration Ratios. Reports for futures positions, as well as futures
        plus options positions. New and legacy formats.
    BLSI : US national and state-level inflation data, published by the Bureau of Labor Statistics.
    COM : Price data for commodities including agricultural products, metals, and energy, in different regions of the
        world from a variety of sources. Sources include: USDA, COMEX, MARKIT, FRBNY and more.
    BP : BP is a large energy producer and distributor. It provides data on energy production and consumption in
        individual countries and larger subregions.
    EIA : US national and state data on production, consumption and other indicators on all major energy products,
        such as electricity, coal, natural gas and petroleum.
    JODI : JODI oil and gas data comes from over 100 countries consisting of multiple energy products and flows in
        various methods of measurement.
    ISM : ISM promotes supply-chain management practices and publishes data on production and supply chains, new orders,
        inventories, and capital expenditures.
    OPEC : International organization and economic cartel overseeing policies of oil-producers, such as Iraq, Iran,
        Saudi Arabia, and Venezuela. Data on oil prices.
    WHNP : Key health, nutrition and population statistics.
    WGFD : Data on financial system characteristics, including measures of size, use, access to, efficiency, and
        stability of financial institutions and markets.
    WWGI : Data on aggregate and individual governance indicators for six dimensions of governance.
    UCOM : This database offers comprehensive global data on imports and exports of commodities such as food, live
        animals, pharmaceuticals, metals, fuels and machinery.
    FAO : Comprehensive global data on food and agriculture. Data covers production, consumption, price inices, export
        and import of hundreds of agricultural products.
    USDAERS : The primary statistical agency of the USDA provides data on the consumption and production of food and
        products related to the agriculture industry.
    BKRHUGHES : Baker Hughes is an oilfield service company. It publishes census data on the number of oil rigs in U.S.
        states, Canadian provinces, and per basin.
    WGC : The World Gold Council is a market development organization for the gold industry. It publishes data on gold
        prices in different currencies.
    JOHNMATT : A leading provider of current and historical data on platinum group metals such as prices, supply, and
        demand.
    WORLDAL : World Aluminium capacity and production in thousand metric tonnes.
    PERTH : The Perth Mint's highs, lows and averages of interest rates and commodities prices, updated on a monthly
        basis.

    ODA : IMF primary commodity prices and world economic outlook data, published by Open Data for Africa.
    Excellent cross-country macroeconomic data.
    WORLDBANK : Annual data on economic development, health, demography and society, for every country in the world.
        Monthly commodity price data.
    UNDATA : Cross-country agriculture, commodity, energy, environment, growth, health, industry, labour, population
        and trade data from the UN Statistics Division.

    """

    REUTERS_FIELDS_LIB= "Reuters.Fields"
    REUTERS_TICKERS_LIB = "Reuters.Tickers"
    REUTERS_METADATA_FIELDS =  ["TR.CommonName()", "TR.HeadquartersCountry", "TR.ExchangeName", "TR.TRBCIndustryGroup", "TR.GICSSector", "TR.CompanyMarketCap.Currency"]

    REUTERS_EOD_LIB = "Reuters.EOD"
    REUTERS_EOD_UNADJUSTED_LIB = "Reuters.EOD.Unadjusted"
    REUTERS_FUNDAMENTAL_DATA = "Reuters.Fundamentals"
